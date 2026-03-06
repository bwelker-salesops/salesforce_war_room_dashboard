#!/usr/bin/env python3
"""
transform.py — Converts Salesforce CSV exports into dashboard-ready JSON.

Usage:
  python3 transform.py

Reads CSVs from ./data/ folder, outputs ./data/dashboard.json
Run this after each Salesforce export refresh.
"""

import csv
import json
import os
from collections import defaultdict
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT = os.path.join(DATA_DIR, "dashboard.json")


def read_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, encoding="utf-8", errors="replace") as f:
        return list(csv.DictReader(f))


def parse_num(val):
    if not val:
        return 0
    return float(val.strip().replace(",", "").replace('"', "").replace("\xa0", "") or 0)


def build_arr_data(pub_rows, priv_rows):
    """Aggregate current ARR by sector, parent account, product, and fiscal period."""
    sectors = {"Public": pub_rows, "Private": priv_rows}
    result = {"by_sector": {}, "by_parent": [], "by_product": {}, "by_period": {}, "deals": []}

    all_parents = defaultdict(lambda: {"arr": 0, "sector": "", "opps": set()})
    all_products = defaultdict(lambda: {"Public": 0, "Private": 0})
    all_periods = defaultdict(lambda: {"Public": 0, "Private": 0})

    for sector, rows in sectors.items():
        total_arr = 0
        opp_names = set()
        for r in rows:
            arr = parse_num(r.get("Prod ARR", "0"))
            total_arr += arr
            opp = r["Opportunity Name"]
            opp_names.add(opp)
            parent = r.get("Parent Account", "") or r.get("Account Name", "")
            product = r.get("Product Name", "Unknown")
            period = r.get("Fiscal Period", "Unknown")

            all_parents[parent]["arr"] += arr
            all_parents[parent]["sector"] = sector
            all_parents[parent]["opps"].add(opp)
            all_products[product][sector] += arr
            all_periods[period][sector] += arr

            result["deals"].append({
                "sector": sector,
                "opp": opp,
                "account": r.get("Account Name", ""),
                "parent": parent,
                "product": product,
                "arr": arr,
                "period": period,
                "contract_start": r.get("Contract Start", ""),
                "contract_end": r.get("Contract End", ""),
                "users": r.get("User Count", ""),
            })

        result["by_sector"][sector] = {
            "total_arr": round(total_arr),
            "opp_count": len(opp_names),
        }

    result["by_parent"] = sorted(
        [{"name": k, "arr": round(v["arr"]), "sector": v["sector"], "opp_count": len(v["opps"])}
         for k, v in all_parents.items()],
        key=lambda x: -x["arr"]
    )

    result["by_product"] = {
        k: {"Public": round(v["Public"]), "Private": round(v["Private"])}
        for k, v in sorted(all_products.items(), key=lambda x: -(x[1]["Public"] + x[1]["Private"]))
    }

    result["by_period"] = {
        k: {"Public": round(v["Public"]), "Private": round(v["Private"])}
        for k, v in sorted(all_periods.items())
    }

    return result


def normalize_stage(stage_str):
    """Extract just the stage name, dropping percentages."""
    if not stage_str:
        return "Unknown"
    return stage_str.split("(")[0].strip()


def build_pipeline_data(pub_rows, priv_rows):
    """Aggregate pipeline by stage, forecast category, type, and owner."""
    result = {"by_sector": {}, "by_stage": {}, "by_forecast": {}, "by_type": {}, "by_owner": [], "deals": []}

    all_stages = defaultdict(lambda: {"Public": 0, "Private": 0, "count": 0})
    all_forecast = defaultdict(lambda: {"Public": 0, "Private": 0})
    all_types = defaultdict(lambda: {"Public": 0, "Private": 0})
    all_owners = defaultdict(lambda: {"arr": 0, "deal_count": 0})

    for sector, rows in [("Public", pub_rows), ("Private", priv_rows)]:
        total_arr = 0
        factored = 0
        for r in rows:
            arr = parse_num(r.get("Amount (ARR)", "0"))
            fact = parse_num(r.get("Factored ARR", "0"))
            total_arr += arr
            factored += fact

            stage = normalize_stage(r.get("Pgo Stage", "") or r.get("Stage", ""))
            forecast = r.get("Forecast Category", "Unknown").strip()
            deal_type = r.get("Type", "Unknown").strip()
            owner = r.get("Opportunity Owner", "Unknown").strip()
            close_date = r.get("Close Date", "")

            all_stages[stage][sector] += arr
            all_stages[stage]["count"] += 1
            all_forecast[forecast][sector] += arr
            all_types[deal_type][sector] += arr
            all_owners[owner]["arr"] += arr
            all_owners[owner]["deal_count"] += 1

            result["deals"].append({
                "sector": sector,
                "opp": r.get("Opportunity Name", ""),
                "account": r.get("Account Name", ""),
                "arr": round(arr),
                "factored_arr": round(fact) if fact else None,
                "stage": stage,
                "forecast": forecast,
                "type": deal_type,
                "owner": owner,
                "close_date": close_date,
                "priority": r.get("Priority (JH)", ""),
                "team": r.get("Team", ""),
            })

        result["by_sector"][sector] = {
            "total_arr": round(total_arr),
            "factored_arr": round(factored) if factored else None,
            "deal_count": len(rows),
        }

    result["by_stage"] = {
        k: {"Public": round(v["Public"]), "Private": round(v["Private"]), "count": v["count"]}
        for k, v in sorted(all_stages.items(), key=lambda x: -(x[1]["Public"] + x[1]["Private"]))
    }

    result["by_forecast"] = {
        k: {"Public": round(v["Public"]), "Private": round(v["Private"])}
        for k, v in all_forecast.items()
    }

    result["by_type"] = {
        k: {"Public": round(v["Public"]), "Private": round(v["Private"])}
        for k, v in all_types.items()
    }

    result["by_owner"] = sorted(
        [{"name": k, "arr": round(v["arr"]), "deals": v["deal_count"]} for k, v in all_owners.items()],
        key=lambda x: -x["arr"]
    )

    return result


def build_partner_data(rows):
    """Aggregate partner pipeline by partner, quarter, and probability."""
    result = {"totals": {}, "by_partner": [], "by_quarter": {}, "deals": []}

    total_arr = 0
    total_nrr = 0
    partners = defaultdict(lambda: {"arr": 0, "nrr": 0, "deal_count": 0})
    quarters = defaultdict(lambda: {"arr": 0, "nrr": 0})

    for r in rows:
        arr = parse_num(r.get("Amount (ARR)", "0"))
        nrr = parse_num(r.get("Amount (NRR)", "0"))
        total_arr += arr
        total_nrr += nrr
        partner = r.get("Related Partner", "Unknown").strip()
        quarter = r.get("Fiscal Period", "Unknown").strip()
        prob = parse_num(r.get("Probability (%)", "0"))

        partners[partner]["arr"] += arr
        partners[partner]["nrr"] += nrr
        partners[partner]["deal_count"] += 1
        quarters[quarter]["arr"] += arr
        quarters[quarter]["nrr"] += nrr

        result["deals"].append({
            "opp": r.get("Opportunity Name", ""),
            "arr": round(arr),
            "nrr": round(nrr),
            "amount": round(parse_num(r.get("Amount", "0"))),
            "probability": prob,
            "close_date": r.get("Close Date", ""),
            "quarter": quarter,
            "partner": partner,
        })

    result["totals"] = {
        "arr": round(total_arr),
        "nrr": round(total_nrr),
        "deal_count": len(rows),
        "partner_count": len(partners),
    }

    result["by_partner"] = sorted(
        [{"name": k, "arr": round(v["arr"]), "nrr": round(v["nrr"]), "deals": v["deal_count"]}
         for k, v in partners.items()],
        key=lambda x: -x["arr"]
    )

    result["by_quarter"] = {
        k: {"arr": round(v["arr"]), "nrr": round(v["nrr"])}
        for k, v in sorted(quarters.items())
    }

    return result


def main():
    print("Reading CSVs...")
    pub_arr = read_csv("Public_Sector_ARR_as_of_TODAY.csv")
    priv_arr = read_csv("Private_Sector_ARR_as_of_TODAY.csv")
    pub_pipe = read_csv("Public_Sector_Factored_ARR_War_Room_Pipe.csv")
    priv_pipe = read_csv("Private_Sector_ARR_War_Room_Pipeline.csv")
    partner = read_csv("Partner_Deal_Pipeline_CQ_3Q_s.csv")

    print("Transforming...")
    dashboard = {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "arr": build_arr_data(pub_arr, priv_arr),
        "pipeline": build_pipeline_data(pub_pipe, priv_pipe),
        "partners": build_partner_data(partner),
    }

    with open(OUTPUT, "w") as f:
        json.dump(dashboard, f, indent=2)

    total_arr = sum(s["total_arr"] for s in dashboard["arr"]["by_sector"].values())
    total_pipe = sum(s["total_arr"] for s in dashboard["pipeline"]["by_sector"].values())
    print(f"\nDone! Written to {OUTPUT}")
    print(f"  Current ARR: ${total_arr:,.0f}")
    print(f"  Pipeline ARR: ${total_pipe:,.0f}")
    print(f"  Partner Pipeline: ${dashboard['partners']['totals']['arr']:,.0f}")


if __name__ == "__main__":
    main()
