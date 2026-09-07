"""
fast_resolve_all_mandis.py
===========================
Fast whitespace-tolerant, exact-first market resolution & backfill.
"""

import os
import re
from datetime import datetime, timezone
from collections import defaultdict
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

MONGO_URI            = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME              = os.getenv("MANDI_DB_NAME", "").strip()
MASTER_COLLECTION    = os.getenv("MASTER_COLLECTION", "").strip()
PRICE_COLLECTION     = os.getenv("PRICE_COLLECTION", "").strip()
ALL_MANDI_COLLECTION = os.getenv("ALL_MANDI_COLLECTION", "").strip()

STATE_MAP = {
    "keralam": "kerala",
    "chattisgarh": "chhattisgarh",
    "nct of delhi": "delhi",
    "pondicherry": "puducherry",
    "orissa": "odisha",
}


def norm_str(s: str) -> str:
    if not s:
        return ""
    # collapse multiple spaces into single space and lower
    return " ".join(s.strip().lower().split())


def make_regex(s: str) -> str:
    # Match allowing 1 or more spaces between words
    words = s.split()
    escaped_words = [re.escape(w) for w in words]
    return "^" + r"\s+".join(escaped_words) + "$"


def main():
    print("=" * 70)
    print("  Fast Market Resolver & Backfill")
    print("=" * 70)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    db = client[DB_NAME]
    master = db[MASTER_COLLECTION]
    prices = db[PRICE_COLLECTION]
    mandis = db[ALL_MANDI_COLLECTION]

    # 1. Load all available_mandi into memory cache: (state, norm_name) -> _id
    print("Loading available_mandi into memory cache...")
    mandi_cache = {}
    for d in mandis.find({}, {"_id": 1, "name": 1, "state": 1, "aliases": 1}):
        st = norm_str(d.get("state"))
        canonical_st = STATE_MAP.get(st, st)
        mid = d["_id"]
        
        name = norm_str(d.get("name"))
        if name:
            mandi_cache[(canonical_st, name)] = mid
            # also without suffixes
            stripped = re.sub(r"\s*(apmc|mandi|market|sub yard|principal yard|yard|vfpck market|vfpck)\b", "", name).strip()
            if stripped:
                mandi_cache.setdefault((canonical_st, stripped), mid)

        for a in d.get("aliases", []):
            aname = norm_str(a)
            if aname:
                mandi_cache[(canonical_st, aname)] = mid
                astripped = re.sub(r"\s*(apmc|mandi|market|sub yard|principal yard|yard|vfpck market|vfpck)\b", "", aname).strip()
                if astripped:
                    mandi_cache.setdefault((canonical_st, astripped), mid)

    print(f"Loaded {len(mandi_cache):,} keys from available_mandi.")

    # 2. Find all master docs where market_id is null
    null_master_docs = list(master.find({"market_id": None}, {"_id": 1, "state": 1, "market_name": 1}))
    print(f"Master docs with null market_id: {len(null_master_docs):,}")

    unique_pairs = defaultdict(list)
    null_names = []
    for d in null_master_docs:
        raw_st = (d.get("state") or "").strip().lower()
        raw_mkt = (d.get("market_name") or "").strip()
        if not raw_mkt:
            null_names.append(d["_id"])
            continue
        unique_pairs[(raw_st, raw_mkt)].append(d["_id"])

    print(f"Unique (state, market_name) pairs to resolve: {len(unique_pairs):,}\n")

    resolved_existing = 0
    created_new = 0
    pair_to_id = {}

    for (raw_st, raw_mkt), mc_ids in unique_pairs.items():
        st_norm = norm_str(raw_st)
        canonical_st = STATE_MAP.get(st_norm, st_norm)
        mkt_norm = norm_str(raw_mkt)

        # ── Step 1: Exact match on name/alias with state ──
        mid = mandi_cache.get((canonical_st, mkt_norm))
        if mid:
            pair_to_id[(raw_st, raw_mkt)] = mid
            resolved_existing += 1
            continue

        # ── Step 2: Suffix variations (exact first failed) ──
        # e.g., adding " apmc" or stripping suffixes
        stripped = re.sub(r"\s*(apmc|mandi|market|sub yard|principal yard|yard|vfpck market|vfpck)\b", "", mkt_norm).strip()
        if stripped:
            mid = mandi_cache.get((canonical_st, stripped))
            if not mid:
                mid = mandi_cache.get((canonical_st, f"{mkt_norm} apmc"))
            if not mid:
                mid = mandi_cache.get((canonical_st, f"{stripped} apmc"))

        if mid:
            pair_to_id[(raw_st, raw_mkt)] = mid
            resolved_existing += 1
            continue

        # ── Step 3: Cross-state fallback ──
        cross_match = [v for k, v in mandi_cache.items() if k[1] == mkt_norm]
        if cross_match:
            mid = cross_match[0]
            pair_to_id[(raw_st, raw_mkt)] = mid
            resolved_existing += 1
            continue

        # ── Step 4: Genuinely New Mandi -> Insert into available_mandi ──
        doc = {
            "name": mkt_norm,
            "state": canonical_st,
            "aliases": [mkt_norm, raw_mkt.strip().lower()],
            "location": None,
            "district": None,
            "source": "auto_resolved",
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }
        res = mandis.insert_one(doc)
        mid = res.inserted_id
        mandi_cache[(canonical_st, mkt_norm)] = mid
        pair_to_id[(raw_st, raw_mkt)] = mid
        created_new += 1

    print(f"Resolution results:")
    print(f"  - Matched existing mandis : {resolved_existing:,}")
    print(f"  - Created new mandis      : {created_new:,}")

    # 3. Bulk backfill markets_commodities and price_records
    print("\nBackfilling markets_commodities and price_records...")
    total_mc = 0
    total_pr = 0

    for (raw_st, raw_mkt), mid in pair_to_id.items():
        mc_ids = unique_pairs[(raw_st, raw_mkt)]
        if not mc_ids:
            continue

        # Update master
        res_mc = master.update_many({"_id": {"$in": mc_ids}}, {"$set": {"market_id": mid}})
        total_mc += res_mc.modified_count

        # Update prices
        res_pr = prices.update_many({"market_commodity_id": {"$in": mc_ids}, "market_id": None}, {"$set": {"market_id": mid}})
        total_pr += res_pr.modified_count

    print()
    print("=" * 70)
    print("  FINAL DATABASE VERIFICATION")
    print("=" * 70)
    print(f"  markets_commodities updated : {total_mc:,}")
    print(f"  price_records updated       : {total_pr:,}")
    print(f"  Remaining null master docs  : {master.count_documents({'market_id': None}):,}")
    print(f"  Remaining null price records: {prices.count_documents({'market_id': None}):,}")
    print("=" * 70)

    client.close()


if __name__ == "__main__":
    main()
