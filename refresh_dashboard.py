#!/usr/bin/env python3
"""
refresh_dashboard.py — Pull Salesforce reports via the Reports API,
transform into dashboard JSON, and rebuild index.html with fresh data.

Required environment variables:
  SF_CLIENT_ID, SF_CLIENT_SECRET, SF_LOGIN_URL (e.g. https://login.salesforce.com)

Usage:
  python refresh_dashboard.py
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.parse import urlencode

REPORT_IDS = {
    "pub_pipe": "00OTS000004g6oK2AQ",
    "priv_pipe": "00OTS000004gDkv2AE",
    "pub_leads": "00OTS000004npNF2AY",
    "priv_leads": "00OTS000004wIof2AE",
}

INDEX_PATH = os.path.join(os.path.dirname(__file__), "index.html")


# ---------------------------------------------------------------------------
# Salesforce auth (client credentials / OAuth 2.0 client_credentials flow)
# ---------------------------------------------------------------------------

def sf_authenticate():
    login_url = os.environ["SF_LOGIN_URL"].rstrip("/")
    payload = urlencode({
        "grant_type": "client_credentials",
        "client_id": os.environ["SF_CLIENT_ID"],
        "client_secret": os.environ["SF_CLIENT_SECRET"],
    }).encode()
    req = Request(f"{login_url}/services/oauth2/token", data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urlopen(req) as resp:
            body = json.loads(resp.read())
    except Exception as e:
        if hasattr(e, "read"):
            print(f"  Auth error response: {e.read().decode()}")
        raise
    return body["instance_url"], body["access_token"]


# ---------------------------------------------------------------------------
# Salesforce Reports API helper
# ---------------------------------------------------------------------------

def fetch_report(instance_url, token, report_id):
    """Fetch a report via the Analytics Reports API. Returns the full JSON."""
    url = f"{instance_url}/services/data/v62.0/analytics/reports/{report_id}?includeDetails=true"
    req = Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    try:
        with urlopen(req) as resp:
            return json.loads(resp.read())
    except Exception as e:
        if hasattr(e, "read"):
            print(f"  Report API error: {e.read().decode()}")
        raise


def extract_rows(report_json):
    """
    Flatten a Salesforce tabular/summary report into a list of dicts.
    For summary reports, grouping field labels are injected into each row.
    """
    columns = report_json["reportMetadata"]["detailColumns"]
    col_info = report_json["reportExtendedMetadata"]["detailColumnInfo"]
    col_labels = [col_info[c]["label"] for c in columns]

    fact_map = report_json.get("factMap", {})
    report_format = report_json["reportMetadata"].get("reportFormat", "TABULAR")
    groupings_down = report_json["reportMetadata"].get("groupingsDown", [])

    grp_col_info = report_json["reportExtendedMetadata"].get("groupingColumnInfo", {})
    grp_labels = [grp_col_info[g["name"]]["label"] for g in groupings_down]

    print(f"    Report format: {report_format}")
    print(f"    Detail columns: {col_labels}")
    print(f"    Grouping columns: {grp_labels}")
    print(f"    factMap keys ({len(fact_map)}): {sorted(fact_map.keys())[:10]}...")

    rows = []

    def walk_groupings(groupings, depth, inherited):
        for g in groupings:
            current = dict(inherited)
            current[grp_labels[depth]] = g.get("label", "")
            if g.get("groupings"):
                walk_groupings(g["groupings"], depth + 1, current)
            else:
                fm_key = f"{g['key']}!T"
                fact = fact_map.get(fm_key)
                if not fact or "rows" not in fact:
                    continue
                for row_data in fact["rows"]:
                    row = dict(current)
                    for i, cell in enumerate(row_data["dataCells"]):
                        row[col_labels[i]] = cell.get("label", cell.get("value", ""))
                    rows.append(row)

    gd = report_json.get("groupingsDown", {})
    if gd and gd.get("groupings"):
        walk_groupings(gd["groupings"], 0, {})
    else:
        fact = fact_map.get("T!T")
        if fact and "rows" in fact:
            for row_data in fact["rows"]:
                row = {}
                for i, cell in enumerate(row_data["dataCells"]):
                    row[col_labels[i]] = cell.get("label", cell.get("value", ""))
                rows.append(row)

    all_labels = grp_labels + col_labels
    print(f"    Extracted {len(rows)} detail rows")
    if rows:
        print(f"    Sample row keys: {list(rows[0].keys())}")
    return rows, all_labels


# ---------------------------------------------------------------------------
# Column-name mapping (report column labels -> dashboard field names)
# Adjust these if your report column labels differ.
# ---------------------------------------------------------------------------

PUB_PIPE_COLS = {
    # Grouping fields (from groupingsDown)
    "Team": "team",
    "Opportunity Owner": "owner",
    "Type": "type",
    # Detail columns (from API response)
    "Priority (JH)": "priority",
    "Opportunity Name": "opp",
    "Amount (ARR)": "arr",
    "Pgo Stage": "pgoStage",
    "Pwin Stage": "pwinStage",
    "Factored ARR": "factoredArr",
    "Next Steps / Latest Updates (new)": "notes",
    "Close Date": "closeDate",
    "Contract Start": "contractStart",
    "Contract End": "contractEnd",
    "Forecast Category": "forecastCat",
    "Forecast Category (Admin)": "forecastAdmin",
}

PRIV_PIPE_COLS = {
    # Grouping fields (from groupingsDown)
    "Opportunity Owner": "owner",
    "Type": "type",
    # Detail columns (from API response)
    "Account Name": "account",
    "Opportunity Name": "opp",
    "Amount (ARR)": "arr",
    "Next Steps / Latest Updates (new)": "notes",
    "Stage": "stage",
    "Close Date": "closeDate",
    "Forecast Category": "forecastCat",
    "Forecast Category (Admin)": "forecastAdmin",
}

PUB_LEADS_COLS = {
    # Grouping fields (from groupingsDown)
    "Lead Owner": "owner",
    "Lead Status": "status",
    # Detail columns (from API response)
    "Company / Account": "company",
    "First Name": "firstName",
    "Last Name": "lastName",
    "Lead Source": "source",
    "Title": "title",
    "Rating (PubSec)": "rating",
    "Next Steps / Latest Updates": "notes",
    "Branch": "team",
}

PRIV_LEADS_COLS = {
    # Grouping fields (from groupingsDown)
    "Lead Owner": "owner",
    "Lead Status": "status",
    # Detail columns (from API response)
    "Company / Account": "company",
    "First Name": "firstName",
    "Last Name": "lastName",
    "Lead Source": "source",
    "Title": "title",
    "Next Steps / Latest Updates": "notes",
}


# ---------------------------------------------------------------------------
# Transform helpers
# ---------------------------------------------------------------------------

def parse_currency(val):
    if not val:
        return 0
    cleaned = re.sub(r"[^0-9.\-]", "", str(val))
    return float(cleaned) if cleaned else 0


def map_row(row, col_map):
    """Map a report row dict to a dashboard deal/lead dict using col_map."""
    result = {}
    for report_label, dash_key in col_map.items():
        result[dash_key] = row.get(report_label, "")
    return result


def build_pp_data(rows, col_map):
    """Build PP_DATA or PVP_DATA from pipeline report rows."""
    deals = []
    total_arr = 0
    total_factored = 0
    for row in rows:
        deal = map_row(row, col_map)
        arr_val = parse_currency(deal.get("arr", 0))
        deal["arr"] = round(arr_val)
        total_arr += arr_val

        if "factoredArr" in deal:
            fact_val = parse_currency(deal.get("factoredArr", 0))
            deal["factoredArr"] = round(fact_val)
            total_factored += fact_val

        deals.append(deal)

    totals = {"records": len(deals), "totalArr": round(total_arr)}
    if "factoredArr" in col_map.values():
        totals["totalFactored"] = round(total_factored)

    return {"totals": totals, "deals": deals}


def build_leads_data(rows, col_map):
    """Build PL_DATA or PRL_DATA from leads report rows."""
    leads = []
    for row in rows:
        lead = map_row(row, col_map)
        leads.append(lead)
    return {"totals": {"records": len(leads)}, "leads": leads}


def build_dash(pp_data, pvp_data):
    """
    Build the DASH summary object from pipeline data.
    DASH has: arrToday, arrTarget, pubPipeChart, privPipeChart.
    arrTarget is a fixed goal — we preserve the existing values.
    """
    pub_pipe_chart = {}
    for deal in pp_data["deals"]:
        team = deal.get("team", "Unknown") or "Unknown"
        dtype = deal.get("type", "Unknown") or "Unknown"
        pub_pipe_chart.setdefault(team, {})
        pub_pipe_chart[team][dtype] = pub_pipe_chart[team].get(dtype, 0) + deal["arr"]

    priv_pipe_chart = {}
    for deal in pvp_data["deals"]:
        owner = deal.get("owner", "Unknown") or "Unknown"
        dtype = deal.get("type", "Unknown") or "Unknown"
        priv_pipe_chart.setdefault(owner, {})
        priv_pipe_chart[owner][dtype] = priv_pipe_chart[owner].get(dtype, 0) + deal["arr"]

    return {
        "pubPipeChart": pub_pipe_chart,
        "privPipeChart": priv_pipe_chart,
    }


# ---------------------------------------------------------------------------
# HTML rebuild
# ---------------------------------------------------------------------------

def read_existing_dash_targets(html):
    """Extract current arrToday and arrTarget from existing DASH const."""
    m = re.search(r"^const DASH = ({.+});", html, re.MULTILINE)
    if m:
        try:
            existing = json.loads(m.group(1))
            return existing.get("arrToday", {}), existing.get("arrTarget", {})
        except json.JSONDecodeError:
            pass
    return {}, {}


def replace_const(html, var_name, new_value_json):
    """Replace `const VAR = {...};` on a single line with new JSON value."""
    pattern = re.compile(
        r"^(const " + re.escape(var_name) + r" = )(\{.+\})(;)\s*$",
        re.MULTILINE,
    )
    m = pattern.search(html)
    if not m:
        print(f"  WARNING: Could not find 'const {var_name}' in HTML")
        return html
    return html[:m.start(2)] + new_value_json + html[m.end(2):]


def update_timestamp(html, ts_str):
    """Update the last-refresh line."""
    return re.sub(
        r"\$\('#updated'\)\.textContent = 'Last refresh: .*?'",
        f"$('#updated').textContent = 'Last refresh: {ts_str}'",
        html,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    for var in ("SF_CLIENT_ID", "SF_CLIENT_SECRET", "SF_LOGIN_URL"):
        if var not in os.environ:
            print(f"ERROR: Missing env var {var}")
            sys.exit(1)

    missing_ids = [k for k, v in REPORT_IDS.items() if not v]
    if missing_ids:
        print(f"WARNING: Missing report IDs for: {', '.join(missing_ids)}")
        print("  Will skip those reports and preserve existing HTML data.\n")

    print("Authenticating to Salesforce...")
    instance_url, token = sf_authenticate()
    print(f"  Connected to {instance_url}")

    pp_data = pvp_data = pl_data = prl_data = None

    if REPORT_IDS["pub_pipe"]:
        print("\nFetching Public Sector Pipeline report...")
        report = fetch_report(instance_url, token, REPORT_IDS["pub_pipe"])
        rows, cols = extract_rows(report)
        print(f"  Got {len(rows)} rows, columns: {cols}")
        pp_data = build_pp_data(rows, PUB_PIPE_COLS)
        print(f"  Pipeline total: ${pp_data['totals']['totalArr']:,.0f}")

    if REPORT_IDS["priv_pipe"]:
        print("\nFetching Private Sector Pipeline report...")
        report = fetch_report(instance_url, token, REPORT_IDS["priv_pipe"])
        rows, cols = extract_rows(report)
        print(f"  Got {len(rows)} rows, columns: {cols}")
        pvp_data = build_pp_data(rows, PRIV_PIPE_COLS)
        print(f"  Pipeline total: ${pvp_data['totals']['totalArr']:,.0f}")

    if REPORT_IDS["pub_leads"]:
        print("\nFetching Public Sector Leads report...")
        report = fetch_report(instance_url, token, REPORT_IDS["pub_leads"])
        rows, cols = extract_rows(report)
        print(f"  Got {len(rows)} rows, columns: {cols}")
        pl_data = build_leads_data(rows, PUB_LEADS_COLS)
        print(f"  Leads: {pl_data['totals']['records']}")

    if REPORT_IDS["priv_leads"]:
        print("\nFetching Private Sector Leads report...")
        report = fetch_report(instance_url, token, REPORT_IDS["priv_leads"])
        rows, cols = extract_rows(report)
        print(f"  Got {len(rows)} rows, columns: {cols}")
        prl_data = build_leads_data(rows, PRIV_LEADS_COLS)
        print(f"  Leads: {prl_data['totals']['records']}")

    print("\nRebuilding index.html...")

    with open(INDEX_PATH, "r", encoding="utf-8") as f:
        html = f.read()

    existing_today, existing_target = read_existing_dash_targets(html)

    if pp_data and pvp_data:
        dash = build_dash(pp_data, pvp_data)
    elif pp_data:
        dash = build_dash(pp_data, {"deals": []})
    else:
        dash = {}

    dash["arrToday"] = existing_today
    dash["arrTarget"] = existing_target

    if dash:
        html = replace_const(html, "DASH", json.dumps(dash, separators=(",", ": ")))
    if pp_data:
        html = replace_const(html, "PP_DATA", json.dumps(pp_data, separators=(",", ": ")))
    if pvp_data:
        html = replace_const(html, "PVP_DATA", json.dumps(pvp_data, separators=(",", ": ")))
    if pl_data:
        html = replace_const(html, "PL_DATA", json.dumps(pl_data, separators=(",", ": ")))
    if prl_data:
        html = replace_const(html, "PRL_DATA", json.dumps(prl_data, separators=(",", ": ")))

    now_et = datetime.now(timezone.utc).strftime("%b %-d, %Y")
    html = update_timestamp(html, now_et)

    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  Updated index.html with fresh data")
    print("Done!")


if __name__ == "__main__":
    main()
