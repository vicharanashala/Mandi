"""
anomaly_check.py
=================
Comprehensive database anomaly detection for the Mandi/VicharanaShala MongoDB.
"""

import os
import sys
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI            = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME              = os.getenv("MANDI_DB_NAME", "").strip()
MASTER_COLLECTION    = os.getenv("MASTER_COLLECTION", "").strip()
PRICE_COLLECTION     = os.getenv("PRICE_COLLECTION", "").strip()
ALL_MANDI_COLLECTION = os.getenv("ALL_MANDI_COLLECTION", "").strip()
COMMODITY_ALIAS_COLL = "commodity_alias_lookup"

SAMPLE_LIMIT = 5


def now_utc():
    return datetime.now(timezone.utc)


def pct(num, den):
    return f"{(num / den * 100):.1f}%" if den else "N/A"


def section(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def ok(msg):   print(f"    [OK]   {msg}")
def warn(msg): print(f"    [WARN] {msg}")
def err(msg):  print(f"    [ERR]  {msg}")
def info(msg): print(f"    [INFO] {msg}")


def main():
    print(f"\nMandi DB Anomaly Report")
    print(f"Generated : {now_utc().isoformat()}")
    print(f"Database  : {DB_NAME}")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    print(f"Status    : Connected\n")

    db      = client[DB_NAME]
    master  = db[MASTER_COLLECTION]
    prices  = db[PRICE_COLLECTION]
    mandis  = db[ALL_MANDI_COLLECTION]
    aliases = db[COMMODITY_ALIAS_COLL]
    today   = now_utc()
    report  = {}

    # 1. COLLECTION SIZES
    section("1. COLLECTION SIZES")
    counts = {}
    for name, coll in [
        (MASTER_COLLECTION, master),
        (PRICE_COLLECTION, prices),
        (ALL_MANDI_COLLECTION, mandis),
        (COMMODITY_ALIAS_COLL, aliases),
    ]:
        n = coll.count_documents({})
        counts[name] = n
        info(f"{name:35s}  ->  {n:>10,} documents")
    report["collection_counts"] = counts

    # 2. NULL FIELDS — markets_commodities
    section("2. NULL / MISSING CRITICAL FIELDS — markets_commodities")
    mc_total = counts[MASTER_COLLECTION]
    null_checks = {
        "market_id":                 {"market_id": None},
        "commodity_alias_lookup_id": {"commodity_alias_lookup_id": None},
        "market_name":               {"market_name": None},
        "state":                     {"state": None},
        "commodity_name":            {"commodity_name": None},
        "source_system":             {"source_system": None},
    }
    mc_nulls = {}
    for field, q in null_checks.items():
        n = master.count_documents(q)
        mc_nulls[field] = n
        if n == 0:
            ok(f"No null `{field}`")
        else:
            fn = warn if n < mc_total * 0.1 else err
            fn(f"`{field}` is null in {n:,} docs ({pct(n, mc_total)})")
            for d in list(master.find(q, {"state": 1, "market_name": 1, "commodity_name": 1}).limit(SAMPLE_LIMIT)):
                d.pop("_id", None)
                print(f"           sample: {d}")
    report["mc_null_fields"] = mc_nulls

    # 3. NULL FIELDS — price_records
    section("3. NULL / MISSING CRITICAL FIELDS — price_records")
    pr_total = counts[PRICE_COLLECTION]
    pr_null_checks = {
        "date":                      {"date": None},
        "market_commodity_id":       {"market_commodity_id": None},
        "market_id":                 {"market_id": None},
        "commodity_alias_lookup_id": {"commodity_alias_lookup_id": None},
    }
    pr_nulls = {}
    for field, q in pr_null_checks.items():
        n = prices.count_documents(q)
        pr_nulls[field] = n
        if n == 0:
            ok(f"No null `{field}`")
        else:
            fn = warn if n < pr_total * 0.1 else err
            fn(f"`{field}` is null in {n:,} docs ({pct(n, pr_total)})")
    report["pr_null_fields"] = pr_nulls

    # 4. FUTURE-DATED RECORDS
    section("4. FUTURE-DATED PRICE RECORDS")
    future_q = {"date": {"$gt": today}}
    future_n = prices.count_documents(future_q)
    if future_n == 0:
        ok("No future-dated records.")
    else:
        err(f"{future_n:,} price records have date > today ({today.date()})")
        for s in list(prices.find(future_q, {"market_commodity_id": 1, "date": 1}).limit(SAMPLE_LIMIT)):
            mc_doc = master.find_one({"_id": s["market_commodity_id"]}, {"state": 1, "market_name": 1, "source_system": 1})
            extra = mc_doc or {}
            print(f"           date={s.get('date')} state={extra.get('state')} market={extra.get('market_name')} source={extra.get('source_system')}")
        # by source
        by_source = list(prices.aggregate([
            {"$match": future_q},
            {"$lookup": {"from": MASTER_COLLECTION, "localField": "market_commodity_id", "foreignField": "_id", "as": "m"}},
            {"$unwind": {"path": "$m", "preserveNullAndEmptyArrays": True}},
            {"$group": {"_id": "$m.source_system", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]))
        info("  Future dates by source_system:")
        for row in by_source:
            print(f"           {str(row['_id']):25s} -> {row['count']:,}")
    report["future_dated_count"] = future_n

    # 5. PRICE LOGIC VIOLATIONS
    section("5. PRICE LOGIC VIOLATIONS")
    checks = [
        ("min_price > max_price",  {"min_price": {"$ne": None}, "max_price": {"$ne": None}, "$expr": {"$gt": ["$min_price", "$max_price"]}}),
        ("modal_price > max_price",{"modal_price":{"$ne": None},"max_price":{"$ne": None},"$expr":{"$gt":["$modal_price","$max_price"]}}),
        ("modal_price < min_price",{"modal_price":{"$ne": None},"min_price":{"$ne": None},"$expr":{"$lt":["$modal_price","$min_price"]}}),
        ("negative prices",        {"$or":[{"min_price":{"$lt":0}},{"max_price":{"$lt":0}},{"modal_price":{"$lt":0}}]}),
        ("zero prices",            {"$or":[{"min_price":0},{"max_price":0},{"modal_price":0}]}),
    ]
    price_violations = {}
    for label, q in checks:
        n = prices.count_documents(q)
        price_violations[label] = n
        if n == 0:
            ok(f"No {label}.")
        elif "zero" in label or "modal" in label:
            warn(f"{n:,} records: {label}")
        else:
            err(f"{n:,} records: {label}")
            for d in list(prices.find(q, {"min_price":1,"max_price":1,"modal_price":1,"date":1}).limit(SAMPLE_LIMIT)):
                d.pop("_id", None)
                print(f"           {d}")
    report["price_violations"] = price_violations

    # 6. DUPLICATE PRICE RECORDS
    section("6. DUPLICATE PRICE RECORDS (same market_commodity_id + date)")
    dup_result = list(prices.aggregate([
        {"$group": {"_id": {"mc_id": "$market_commodity_id", "date": "$date"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$count": "duplicate_groups"},
    ]))
    dup_groups = dup_result[0]["duplicate_groups"] if dup_result else 0
    if dup_groups == 0:
        ok("No duplicate (market_commodity_id, date) pairs.")
    else:
        err(f"{dup_groups:,} duplicate group(s) in price_records")
        for ex in list(prices.aggregate([
            {"$group": {"_id": {"mc_id": "$market_commodity_id", "date": "$date"}, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$limit": 3},
        ])):
            print(f"           mc_id={ex['_id']['mc_id']} date={ex['_id']['date']} count={ex['count']}")
    report["duplicate_price_groups"] = dup_groups

    # 7. ORPHANED master docs
    section("7. ORPHANED markets_commodities (no associated price records)")
    orphan_result = list(master.aggregate([
        {"$lookup": {"from": PRICE_COLLECTION, "localField": "_id", "foreignField": "market_commodity_id", "as": "prices_joined"}},
        {"$match": {"prices_joined": {"$size": 0}}},
        {"$count": "orphan_masters"},
    ], allowDiskUse=True))
    orphan_masters = orphan_result[0]["orphan_masters"] if orphan_result else 0
    if orphan_masters == 0:
        ok("Every master doc has at least one price record.")
    else:
        warn(f"{orphan_masters:,} master doc(s) have no price records.")
    report["orphan_master_docs"] = orphan_masters

    # 8. ORPHANED price records
    section("8. ORPHANED price_records (no matching master doc)")
    orphan_price_result = list(prices.aggregate([
        {"$lookup": {"from": MASTER_COLLECTION, "localField": "market_commodity_id", "foreignField": "_id", "as": "master_joined"}},
        {"$match": {"master_joined": {"$size": 0}}},
        {"$count": "orphan_prices"},
    ], allowDiskUse=True))
    orphan_prices = orphan_price_result[0]["orphan_prices"] if orphan_price_result else 0
    if orphan_prices == 0:
        ok("Every price record has a matching master doc.")
    else:
        err(f"{orphan_prices:,} price record(s) reference non-existent master docs!")
    report["orphan_price_records"] = orphan_prices

    # 9. SOURCE SYSTEM DISTRIBUTION
    section("9. SOURCE SYSTEM DISTRIBUTION")
    src_dist = list(master.aggregate([
        {"$group": {"_id": "$source_system", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]))
    info("Master docs per source_system:")
    for row in src_dist:
        print(f"           {str(row['_id']):30s}  ->  {row['count']:>8,}")
    report["source_system_distribution"] = {str(r["_id"]): r["count"] for r in src_dist}

    # 10. STATE DISTRIBUTION
    section("10. STATE DISTRIBUTION")
    state_dist = list(master.aggregate([
        {"$group": {"_id": "$state", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]))
    info("Master docs per state:")
    for row in state_dist:
        count = row["count"]
        prefix = "[WARN]" if count < 5 else "[INFO]"
        print(f"    {prefix}  {str(row['_id'] or 'NULL'):30s}  ->  {count:>8,}")
    report["state_distribution"] = {str(r["_id"]): r["count"] for r in state_dist}

    # 11. STALE DATA CHECK
    section("11. STALE DATA CHECK (data ingested in last 3 days?)")
    cutoff = today - timedelta(days=3)
    recent_n = prices.count_documents({"ingested_at": {"$gt": cutoff}})
    if recent_n > 0:
        ok(f"{recent_n:,} price records ingested in the last 3 days.")
    else:
        err(f"NO price records ingested since {cutoff.date()} — pipeline may be broken!")
    latest_doc = prices.find_one({}, sort=[("ingested_at", -1)], projection={"ingested_at": 1})
    if latest_doc and latest_doc.get("ingested_at"):
        info(f"Latest ingested_at: {latest_doc['ingested_at']}")
    report["recent_ingestion_count_3d"] = recent_n

    # 12. DATE RANGE
    section("12. PRICE RECORDS DATE RANGE")
    date_range = list(prices.aggregate([
        {"$group": {"_id": None, "min_date": {"$min": "$date"}, "max_date": {"$max": "$date"}}}
    ]))
    if date_range:
        dr = date_range[0]
        info(f"Earliest date : {dr.get('min_date')}")
        info(f"Latest date   : {dr.get('max_date')}")
        if isinstance(dr.get("max_date"), datetime):
            days_ago = (today - dr["max_date"].replace(tzinfo=timezone.utc)).days
            if days_ago > 3:
                warn(f"Latest price date is {days_ago} day(s) ago.")
            else:
                ok(f"Latest price date is {days_ago} day(s) ago — recent.")
    report["date_range"] = {
        "min": str(date_range[0].get("min_date")) if date_range else None,
        "max": str(date_range[0].get("max_date")) if date_range else None,
    }

    # 13. COMMODITY ALIAS LOOKUP HEALTH
    section("13. COMMODITY_ALIAS_LOOKUP HEALTH")
    empty_aliases_n = aliases.count_documents({"$or": [
        {"aliases": None},
        {"aliases": {"$size": 0}},
        {"aliases": {"$exists": False}},
    ]})
    if empty_aliases_n == 0:
        ok("All alias entries have at least one alias.")
    else:
        warn(f"{empty_aliases_n:,} entries with null/empty aliases array")

    null_crop_master = aliases.count_documents({"$or": [
        {"crop_master_id": None},
        {"crop_master_id": {"$exists": False}},
        {"crop_master_id": ""},
    ]})
    if null_crop_master == 0:
        ok("All alias entries have crop_master_id.")
    else:
        warn(f"{null_crop_master:,} entries missing crop_master_id")
    report["alias_lookup_empty_aliases"] = empty_aliases_n
    report["alias_lookup_null_crop_master"] = null_crop_master

    # 14. MANDI GEO HEALTH
    section("14. AVAILABLE_MANDI GEO HEALTH")
    am_total = counts[ALL_MANDI_COLLECTION]
    missing_geo = mandis.count_documents({"$or": [
        {"location": None},
        {"location": {"$exists": False}},
    ]})
    if missing_geo == 0:
        ok("All mandi docs have a geo location field.")
    else:
        warn(f"{missing_geo:,} mandi docs ({pct(missing_geo, am_total)}) missing geo location")
        for d in list(mandis.find({"$or":[{"location": None},{"location":{"$exists": False}}]}, {"name":1,"state":1}).limit(SAMPLE_LIMIT)):
            d.pop("_id", None)
            print(f"           {d}")

    missing_state_m = mandis.count_documents({"$or": [{"state": None}, {"state": {"$exists": False}}, {"state": ""}]})
    if missing_state_m == 0:
        ok("All mandi docs have state field.")
    else:
        warn(f"{missing_state_m:,} mandi docs missing state")
    report["mandi_missing_geo"] = missing_geo
    report["mandi_missing_state"] = missing_state_m

    # 15. ARRIVAL QUANTITY ANOMALIES
    section("15. ARRIVAL QUANTITY ANOMALIES")
    neg_arr = prices.count_documents({"arrival_quantity": {"$lt": 0}})
    if neg_arr == 0:
        ok("No negative arrival_quantity values.")
    else:
        err(f"{neg_arr:,} records with negative arrival_quantity")

    huge_arr = prices.count_documents({"arrival_quantity": {"$gt": 1_000_000}})
    if huge_arr == 0:
        ok("No suspiciously large arrival_quantity (>1M) values.")
    else:
        warn(f"{huge_arr:,} records with arrival_quantity > 1,000,000 quintals")
        for d in list(prices.find({"arrival_quantity": {"$gt": 1_000_000}}, {"arrival_quantity":1,"date":1}).limit(SAMPLE_LIMIT)):
            d.pop("_id", None)
            print(f"           {d}")
    report["negative_arrival"] = neg_arr
    report["huge_arrival"] = huge_arr

    # FINAL SUMMARY
    section("FINAL SUMMARY")
    critical = []
    warnings_list = []

    if report["future_dated_count"] > 0:
        critical.append(f"Future-dated price records: {report['future_dated_count']:,}")
    if report["price_violations"].get("min_price > max_price", 0) > 0:
        critical.append(f"min_price > max_price: {report['price_violations']['min_price > max_price']:,}")
    if report["price_violations"].get("negative prices", 0) > 0:
        critical.append(f"Negative prices: {report['price_violations']['negative prices']:,}")
    if report["orphan_price_records"] > 0:
        critical.append(f"Orphaned price records: {report['orphan_price_records']:,}")
    if report["recent_ingestion_count_3d"] == 0:
        critical.append("No data ingested in last 3 days")

    if report["mc_null_fields"].get("market_id", 0) > 0:
        warnings_list.append(f"market_id null in {report['mc_null_fields']['market_id']:,} master docs")
    if report["mc_null_fields"].get("commodity_alias_lookup_id", 0) > 0:
        warnings_list.append(f"commodity_alias_lookup_id null in {report['mc_null_fields']['commodity_alias_lookup_id']:,} master docs")
    if report["price_violations"].get("modal_price > max_price", 0) > 0:
        warnings_list.append(f"modal_price > max_price: {report['price_violations']['modal_price > max_price']:,}")
    if report["price_violations"].get("modal_price < min_price", 0) > 0:
        warnings_list.append(f"modal_price < min_price: {report['price_violations']['modal_price < min_price']:,}")
    if report["price_violations"].get("zero prices", 0) > 0:
        warnings_list.append(f"Zero price fields: {report['price_violations']['zero prices']:,}")
    if report["duplicate_price_groups"] > 0:
        warnings_list.append(f"Duplicate price groups: {report['duplicate_price_groups']:,}")
    if report["orphan_master_docs"] > 0:
        warnings_list.append(f"Orphaned master docs: {report['orphan_master_docs']:,}")
    if report["mandi_missing_geo"] > 0:
        warnings_list.append(f"Mandis missing geo: {report['mandi_missing_geo']:,}")

    if critical:
        print("\n  CRITICAL ISSUES:")
        for c in critical:
            print(f"     [CRIT] {c}")
    else:
        print("\n  No critical issues found.")

    if warnings_list:
        print("\n  WARNINGS:")
        for w in warnings_list:
            print(f"     [WARN] {w}")
    else:
        print("  No warnings.")

    total_issues = len(critical) + len(warnings_list)
    print(f"\n  Total anomalies: {total_issues}  ({len(critical)} critical, {len(warnings_list)} warnings)")
    print()

    client.close()
    return report


if __name__ == "__main__":
    main()
