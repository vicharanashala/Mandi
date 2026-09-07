"""
null_scan.py
============
Scans every field in each collection and reports null/missing counts.
"""

import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI            = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME              = os.getenv("MANDI_DB_NAME", "").strip()
MASTER_COLLECTION    = os.getenv("MASTER_COLLECTION", "").strip()
PRICE_COLLECTION     = os.getenv("PRICE_COLLECTION", "").strip()
ALL_MANDI_COLLECTION = os.getenv("ALL_MANDI_COLLECTION", "").strip()
COMMODITY_ALIAS_COLL = "commodity_alias_lookup"

SAMPLE_LIMIT = 3


def pct(num, den):
    return f"{(num / den * 100):.2f}%" if den else "N/A"


def bar(num, den, width=20):
    filled = int((num / den) * width) if den else 0
    return "[" + "█" * filled + "░" * (width - filled) + "]"


def section(title, total):
    print()
    print("━" * 75)
    print(f"  {title}  ({total:,} total documents)")
    print("━" * 75)


def print_field_row(field, null_count, total, has_samples=False):
    p = pct(null_count, total)
    b = bar(null_count, total)
    status = "✓" if null_count == 0 else ("⚠" if null_count < total * 0.1 else "✗")
    print(f"  {status}  {field:<40s}  {null_count:>7,}  {p:>7s}  {b}")


def scan_collection(coll, total, field_list, label):
    """
    For each known field, count docs where field is null OR field is missing.
    Also discover any extra fields via sampling.
    """
    section(label, total)
    if total == 0:
        print("  (collection is empty)")
        return {}

    print(f"  {'Field':<40s}  {'Null':>7}  {'%':>7}  Bar")
    print(f"  {'-'*40}  {'-'*7}  {'-'*7}  {'-'*22}")

    results = {}
    for field in field_list:
        q = {"$or": [{field: None}, {field: {"$exists": False}}]}
        n = coll.count_documents(q)
        results[field] = n
        print_field_row(field, n, total)

        # Show samples for non-zero nulls
        if n > 0:
            sample_q = {field: {"$in": [None]}}
            samples = list(coll.find(sample_q).limit(SAMPLE_LIMIT))
            for s in samples:
                s.pop("_id", None)
                # Trim the doc to only relevant fields for readability
                shown = {k: v for k, v in s.items() if k in field_list or k == field}
                print(f"       └─ sample: {shown}")

    return results


def main():
    print(f"\n{'='*75}")
    print(f"  NULL VALUE SCAN — Mandi DB (`{DB_NAME}`)")
    print(f"  Scanned at: {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*75}")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    print("  Connected to MongoDB ✓\n")

    db      = client[DB_NAME]
    master  = db[MASTER_COLLECTION]
    prices  = db[PRICE_COLLECTION]
    mandis  = db[ALL_MANDI_COLLECTION]
    aliases = db[COMMODITY_ALIAS_COLL]

    mc_total  = master.count_documents({})
    pr_total  = prices.count_documents({})
    am_total  = mandis.count_documents({})
    cal_total = aliases.count_documents({})

    all_results = {}

    # ── 1. markets_commodities ───────────────────────────────────────────
    MC_FIELDS = [
        "source_system",
        "state",
        "market_name",
        "market_id",
        "commodity_alias_lookup_id",
        "commodity_group",
        "commodity_name",
        "variety",
        "grade",
        "source_url",
        "source_name",
        "method",
    ]
    all_results["markets_commodities"] = scan_collection(
        master, mc_total, MC_FIELDS, f"1. {MASTER_COLLECTION}"
    )

    # ── 2. price_records ─────────────────────────────────────────────────
    PR_FIELDS = [
        "market_commodity_id",
        "date",
        "market_id",
        "commodity_alias_lookup_id",
        "arrival_quantity",
        "min_price",
        "max_price",
        "modal_price",
        "ingested_at",
    ]
    all_results["price_records"] = scan_collection(
        prices, pr_total, PR_FIELDS, f"2. {PRICE_COLLECTION}"
    )

    # ── 3. available_mandi ───────────────────────────────────────────────
    AM_FIELDS = [
        "name",
        "state",
        "location",
        "aliases",
        "district",
        "type",
        "source",
    ]
    all_results["available_mandi"] = scan_collection(
        mandis, am_total, AM_FIELDS, f"3. {ALL_MANDI_COLLECTION}"
    )

    # ── 4. commodity_alias_lookup ────────────────────────────────────────
    CAL_FIELDS = [
        "aliases",
        "crop_master_id",
        "canonical_name",
        "category",
        "unit",
    ]
    all_results["commodity_alias_lookup"] = scan_collection(
        aliases, cal_total, CAL_FIELDS, f"4. {COMMODITY_ALIAS_COLL}"
    )

    # ── CROSS-COLLECTION SUMMARY TABLE ───────────────────────────────────
    print()
    print("━" * 75)
    print("  CROSS-COLLECTION NULL SUMMARY")
    print("━" * 75)
    print(f"  {'Collection':<30s}  {'Field':<35s}  {'Null':>7}  {'%':>7}")
    print(f"  {'-'*30}  {'-'*35}  {'-'*7}  {'-'*7}")

    totals_map = {
        "markets_commodities": mc_total,
        "price_records": pr_total,
        "available_mandi": am_total,
        "commodity_alias_lookup": cal_total,
    }
    coll_name_map = {
        "markets_commodities": MASTER_COLLECTION,
        "price_records": PRICE_COLLECTION,
        "available_mandi": ALL_MANDI_COLLECTION,
        "commodity_alias_lookup": COMMODITY_ALIAS_COLL,
    }

    grand_total_nulls = 0
    for coll_key, field_results in all_results.items():
        total = totals_map[coll_key]
        cname = coll_name_map[coll_key]
        for field, null_count in field_results.items():
            if null_count > 0:
                grand_total_nulls += null_count
                p = pct(null_count, total)
                print(f"  {cname:<30s}  {field:<35s}  {null_count:>7,}  {p:>7s}")

    print()
    print(f"  Total null/missing values across all collections: {grand_total_nulls:,}")
    print()

    client.close()


if __name__ == "__main__":
    main()
