"""
export_null_alias_commodities.py
=================================
Exports all commodity names from markets_commodities where
commodity_alias_lookup_id is null — for creating new commodity_alias_lookup
documents from crop_master.

Output: null_alias_commodities.csv
"""

import os
import csv
from collections import defaultdict
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI         = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME           = os.getenv("MANDI_DB_NAME", "").strip()
MASTER_COLLECTION = os.getenv("MASTER_COLLECTION", "").strip()
PRICE_COLLECTION  = os.getenv("PRICE_COLLECTION", "").strip()
OUTPUT_FILE       = "null_alias_commodities.csv"


def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    print(f"Connected to MongoDB ({DB_NAME})")

    db         = client[DB_NAME]
    master     = db[MASTER_COLLECTION]
    prices     = db[PRICE_COLLECTION]

    # ── Fetch all master docs with null commodity_alias_lookup_id ────────
    null_docs = list(master.find(
        {"commodity_alias_lookup_id": None},
        {
            "_id": 1,
            "commodity_name": 1,
            "commodity_group": 1,
            "variety": 1,
            "state": 1,
            "source_system": 1,
            "market_name": 1,
        }
    ))
    print(f"Found {len(null_docs)} master docs with null commodity_alias_lookup_id")

    # ── Aggregate: per unique commodity_name, collect all states/sources
    #    and count total price records ─────────────────────────────────────
    commodity_map = defaultdict(lambda: {
        "states": set(),
        "sources": set(),
        "varieties": set(),
        "groups": set(),
        "master_doc_ids": [],
        "price_record_count": 0,
    })

    for doc in null_docs:
        name = (doc.get("commodity_name") or "").strip().lower()
        if not name:
            name = "(no commodity_name)"
        entry = commodity_map[name]
        entry["states"].add((doc.get("state") or "").strip().lower())
        entry["sources"].add((doc.get("source_system") or "").strip())
        entry["varieties"].add((doc.get("variety") or "").strip().lower())
        entry["groups"].add((doc.get("commodity_group") or "").strip().lower())
        entry["master_doc_ids"].append(doc["_id"])

    # ── Count price records per commodity (via master _ids) ────────────────
    all_mc_ids = [mid for entry in commodity_map.values() for mid in entry["master_doc_ids"]]
    pipeline = [
        {"$match": {"market_commodity_id": {"$in": all_mc_ids}}},
        {"$group": {"_id": "$market_commodity_id", "count": {"$sum": 1}}},
    ]
    price_counts = {str(r["_id"]): r["count"] for r in prices.aggregate(pipeline)}

    for name, entry in commodity_map.items():
        for mid in entry["master_doc_ids"]:
            entry["price_record_count"] += price_counts.get(str(mid), 0)

    # ── Sort by price_record_count descending (most impactful first) ──────
    sorted_rows = sorted(
        commodity_map.items(),
        key=lambda x: x[1]["price_record_count"],
        reverse=True
    )

    # ── Write CSV ─────────────────────────────────────────────────────────
    fieldnames = [
        "commodity_name",
        "price_record_count",
        "master_doc_count",
        "states",
        "source_systems",
        "varieties",
        "commodity_group",
        # Columns for the user to fill in when creating alias lookup docs:
        "canonical_name",
        "crop_master_id",
        "additional_aliases",
        "notes",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for name, entry in sorted_rows:
            # Clean up sets — remove empty strings
            states   = " | ".join(sorted(s for s in entry["states"] if s))
            sources  = " | ".join(sorted(s for s in entry["sources"] if s))
            varieties= " | ".join(sorted(v for v in entry["varieties"] if v))
            groups   = " | ".join(sorted(g for g in entry["groups"] if g))

            writer.writerow({
                "commodity_name":     name,
                "price_record_count": entry["price_record_count"],
                "master_doc_count":   len(entry["master_doc_ids"]),
                "states":             states,
                "source_systems":     sources,
                "varieties":          varieties,
                "commodity_group":    groups,
                # Leave these blank for the user to fill:
                "canonical_name":     "",
                "crop_master_id":     "",
                "additional_aliases": "",
                "notes":              "",
            })

    client.close()
    print(f"\nExported {len(sorted_rows)} unique commodity name(s) to: {OUTPUT_FILE}")
    print(f"Sorted by price_record_count (most impactful first).")
    print(f"\nTop 10 commodities by price impact:")
    print(f"  {'Commodity':<40s}  {'Price Records':>13}  {'Master Docs':>11}  States")
    print(f"  {'-'*40}  {'-'*13}  {'-'*11}  ------")
    for name, entry in sorted_rows[:10]:
        states = ", ".join(sorted(s for s in entry["states"] if s))
        print(f"  {name:<40s}  {entry['price_record_count']:>13,}  {len(entry['master_doc_ids']):>11,}  {states}")


if __name__ == "__main__":
    main()
