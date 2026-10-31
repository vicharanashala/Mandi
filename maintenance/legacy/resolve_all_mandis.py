"""
resolve_all_mandis.py
======================
Comprehensive market resolution and backfill for Mandi DB.

1. Identifies all master docs in `markets_commodities` where `market_id` is null.
2. Resolves each unique (state, market_name) pair using exact-first matching:
   - Step 1: Exact name / alias match with canonical state.
   - Step 2: Suffix-tolerant match (e.g. adding or stripping 'apmc' / 'mandi' only if exact not found).
   - Step 3: Geocoding & inserting genuinely new mandis into `available_mandi`.
3. Backfills the resolved `market_id` into:
   - `markets_commodities`
   - `price_records` (in bulk)
"""

import os
import re
import sys
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

# Ensure root directory and maintenance directory are on sys.path
_ROOT_DIR = Path(__file__).resolve().parent.parent
_MAINT_DIR = Path(__file__).resolve().parent
if str(_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(_ROOT_DIR))
if str(_MAINT_DIR) not in sys.path:
    sys.path.insert(0, str(_MAINT_DIR))

load_dotenv(_ROOT_DIR / ".env")

from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError
from database import get_market_id, STATE_CANONICAL_MAP
from lookup_market.get_geo import get_mandi_geo_doc

MONGO_URI            = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME              = os.getenv("MANDI_DB_NAME", "").strip()
MASTER_COLLECTION    = os.getenv("MASTER_COLLECTION", "").strip()
PRICE_COLLECTION     = os.getenv("PRICE_COLLECTION", "").strip()
ALL_MANDI_COLLECTION = os.getenv("ALL_MANDI_COLLECTION", "").strip()


def main(execute: bool = True):
    print()
    print("=" * 70)
    print("  Comprehensive Market Resolver & Backfill")
    print(f"  Mode     : {'EXECUTE (writing to DB)' if execute else 'DRY-RUN (preview only)'}")
    print(f"  Database : {DB_NAME}")
    print("=" * 70)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    print("  Connected to MongoDB ✓\n")

    db         = client[DB_NAME]
    master_col = db[MASTER_COLLECTION]
    price_col  = db[PRICE_COLLECTION]
    mandi_col  = db[ALL_MANDI_COLLECTION]

    # ── Step 1: Find all docs where market_id is null ─────────────────
    null_docs = list(master_col.find(
        {"market_id": None},
        {"_id": 1, "state": 1, "market_name": 1, "source_system": 1}
    ))

    print(f"  Found {len(null_docs):,} master doc(s) with null market_id.")
    if not null_docs:
        print("  Nothing to do! All master docs have market_id.")
        client.close()
        return

    # Group by (state, market_name)
    unique_mandis: dict[tuple[str, str], list] = defaultdict(list)
    null_name_mc_ids = []

    for d in null_docs:
        st = (d.get("state") or "").strip().lower()
        mkt = (d.get("market_name") or "").strip().lower()
        if not mkt:
            null_name_mc_ids.append(d["_id"])
            continue
        unique_mandis[(st, mkt)].append(d["_id"])

    print(f"  Unique (state, market_name) pairs: {len(unique_mandis):,}")
    print(f"  Records with null market_name   : {len(null_name_mc_ids):,}\n")

    resolved_existing = 0
    created_new = 0
    failed_geo = 0

    # Map (state, market_name) -> mandi_id
    resolved_map: dict[tuple[str, str], object] = {}

    print("─" * 70)
    print("  STEP 1: Matching / Geocoding Unique Mandis")
    print("─" * 70)

    for (raw_st, mkt), mc_ids in unique_mandis.items():
        canonical_st = STATE_CANONICAL_MAP.get(raw_st, raw_st)

        # 1. Try get_market_id (Exact first, then suffix fallback)
        mandi_id = get_market_id(mkt, raw_st)

        if mandi_id:
            resolved_map[(raw_st, mkt)] = mandi_id
            resolved_existing += 1
            continue

        # 2. If not found in available_mandi, geocode & insert
        if not execute:
            print(f"  [DRY-RUN] Would geocode & insert new mandi: '{mkt}' ({canonical_st})")
            created_new += 1
            continue

        print(f"  [GEOCODE] Geocoding new mandi: '{mkt}' ({canonical_st}) ...")
        geo_doc = get_mandi_geo_doc(mkt, canonical_st)
        if geo_doc:
            geo_doc["state"] = canonical_st
            geo_doc["name"] = mkt
            try:
                res = mandi_col.insert_one(geo_doc)
                mandi_id = res.inserted_id
                resolved_map[(raw_st, mkt)] = mandi_id
                created_new += 1
                print(f"    ✓ Inserted into available_mandi -> _id={mandi_id}")
            except DuplicateKeyError:
                existing = mandi_col.find_one({"state": canonical_st, "name": mkt})
                if existing:
                    mandi_id = existing["_id"]
                    resolved_map[(raw_st, mkt)] = mandi_id
                    resolved_existing += 1
        else:
            # Fallback insertion with state centroid or manual placeholder
            print(f"    ⚠️ Geocoding failed for '{mkt}' ({canonical_st}) — creating un-geocoded mandi record")
            fallback_doc = {
                "name": mkt,
                "state": canonical_st,
                "aliases": [mkt],
                "location": None,
                "district": None,
                "source": "auto_fallback",
                "createdAt": datetime.now(timezone.utc),
            }
            try:
                res = mandi_col.insert_one(fallback_doc)
                mandi_id = res.inserted_id
                resolved_map[(raw_st, mkt)] = mandi_id
                created_new += 1
            except DuplicateKeyError:
                existing = mandi_col.find_one({"state": canonical_st, "name": mkt})
                if existing:
                    mandi_id = existing["_id"]
                    resolved_map[(raw_st, mkt)] = mandi_id

    print()
    print("─" * 70)
    print(f"  STEP 2: Backfilling markets_commodities & price_records")
    print("─" * 70)
    print(f"  - Matched existing mandis : {resolved_existing:,}")
    print(f"  - Created new mandis      : {created_new:,}")

    if not execute:
        print("\n  [DRY-RUN Complete] Re-run with --execute to write to MongoDB.")
        client.close()
        return

    # Backfill master & prices
    total_mc_updated = 0
    total_pr_updated = 0

    for (raw_st, mkt), mandi_id in resolved_map.items():
        mc_ids = unique_mandis[(raw_st, mkt)]
        if not mc_ids or not mandi_id:
            continue

        # 1. Update master docs
        mc_res = master_col.update_many(
            {"_id": {"$in": mc_ids}},
            {"$set": {"market_id": mandi_id}},
        )
        total_mc_updated += mc_res.modified_count

        # 2. Update price records
        pr_res = price_col.update_many(
            {"market_commodity_id": {"$in": mc_ids}, "market_id": None},
            {"$set": {"market_id": mandi_id}},
        )
        total_pr_updated += pr_res.modified_count

    print()
    print("=" * 70)
    print("  SUMMARY OF BACKFILL")
    print("=" * 70)
    print(f"  Master docs updated       : {total_mc_updated:,}")
    print(f"  Price records updated     : {total_pr_updated:,}")
    print(f"  Remaining null master docs: {master_col.count_documents({'market_id': None}):,}")
    print(f"  Remaining null prices     : {price_col.count_documents({'market_id': None}):,}")
    print("=" * 70)
    print()

    client.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", default=True, help="Execute writes")
    args = parser.parse_args()
    main(execute=args.execute)
