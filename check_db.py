"""
check_db.py
===========
Find all `markets_commodities` documents where `market_id` is null,
geocode the missing mandis, insert them into `available_mandi`,
and backfill the new `_id` into both `markets_commodities` and `price_records`.

Usage:
    python check_db.py
"""

import os
import re
from pymongo import MongoClient, UpdateMany
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv

load_dotenv()

# Reuse the geo lookup from the lookup_market package
from lookup_market.get_geo import get_mandi_geo_doc

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
MONGO_URI          = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME            = os.getenv("MANDI_DB_NAME", "").strip()
MASTER_COLLECTION  = os.getenv("MASTER_COLLECTION", "").strip()
PRICE_COLLECTION   = os.getenv("PRICE_COLLECTION", "").strip()
ALL_MANDI_COLLECTION = os.getenv("ALL_MANDI_COLLECTION", "").strip()


def main():
    # ── Connect ───────────────────────────────────────────────────────
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("[INFO] Connected to MongoDB.")

    db         = client[DB_NAME]
    master_col = db[MASTER_COLLECTION]
    price_col  = db[PRICE_COLLECTION]
    mandi_col  = db[ALL_MANDI_COLLECTION]

    # ── Step 1: Find all docs where market_id is null ─────────────────
    null_market_docs = list(master_col.find(
        {"market_id": None},
        {"_id": 1, "state": 1, "market_name": 1},
    ))

    if not null_market_docs:
        print("[INFO] No documents with null market_id found. Nothing to do.")
        client.close()
        return

    print(f"[INFO] Found {len(null_market_docs)} doc(s) with null market_id.")

    # ── Step 2: Get unique (state, market_name) pairs ─────────────────
    unique_mandis: dict[tuple[str, str], list] = {}
    for doc in null_market_docs:
        state       = (doc.get("state") or "").strip().lower()
        market_name = (doc.get("market_name") or "").strip().lower()

        if not state or not market_name:
            print(f"  [SKIP] Doc {doc['_id']} — missing state or market_name.")
            continue

        key = (state, market_name)
        unique_mandis.setdefault(key, []).append(doc["_id"])

    print(f"[INFO] {len(unique_mandis)} unique (state, market_name) pair(s) to process.\n")

    # ── Step 3: For each unique mandi, geocode → insert → backfill ────
    created  = 0
    skipped  = 0
    failed   = 0

    for (state, market_name), mc_ids in unique_mandis.items():
        print(f"─── Processing: {market_name} ({state}) ───")

        # 3a. Check if this mandi already exists in available_mandi
        #     (case-insensitive match on name + state)
        existing = mandi_col.find_one({
            "name":  {"$regex": re.compile(f"^{re.escape(market_name)}$", re.IGNORECASE)},
            "state": {"$regex": re.compile(f"^{re.escape(state)}$", re.IGNORECASE)},
        })

        if existing:
            mandi_id = existing["_id"]
            print(f"  [EXISTS] Already in available_mandi → _id = {mandi_id}")
            skipped += 1
        else:
            # 3b. Geocode the mandi
            geo_doc = get_mandi_geo_doc(market_name, state)
            if geo_doc is None:
                print(f"  [FAIL] Geocoding failed — skipping this mandi.")
                failed += 1
                continue

            # 3c. Insert into available_mandi (handle duplicate key race)
            try:
                result = mandi_col.insert_one(geo_doc)
                mandi_id = result.inserted_id
                print(f"  [NEW] Inserted into available_mandi → _id = {mandi_id}")
                created += 1
            except DuplicateKeyError:
                # Another doc with same (state, name) exists — fetch its _id
                existing = mandi_col.find_one(
                    {"state": geo_doc["state"], "name": geo_doc["name"]},
                    {"_id": 1},
                )
                if existing:
                    mandi_id = existing["_id"]
                    print(f"  [DUP] Already exists (unique index) → _id = {mandi_id}")
                    skipped += 1
                else:
                    print(f"  [ERROR] DuplicateKeyError but doc not found — skipping.")
                    failed += 1
                    continue

        # 3d. Update market_id in all matching markets_commodities docs
        mc_result = master_col.update_many(
            {"_id": {"$in": mc_ids}},
            {"$set": {"market_id": mandi_id}},
        )
        print(f"  [UPDATE] markets_commodities: {mc_result.modified_count} doc(s) updated.")

        # 3e. Update market_id in price_records where market_commodity_id
        #     matches any of the master doc _ids
        pr_result = price_col.update_many(
            {"market_commodity_id": {"$in": mc_ids}},
            {"$set": {"market_id": mandi_id}},
        )
        print(f"  [UPDATE] price_records: {pr_result.modified_count} doc(s) updated.\n")

    # ── Summary ───────────────────────────────────────────────────────
    print("=" * 50)
    print(f"[DONE] Created {created} new mandi(s) in available_mandi.")
    print(f"       Skipped {skipped} (already existed).")
    print(f"       Failed  {failed} (geocoding error).")
    print("=" * 50)

    client.close()
    print("[INFO] Connection closed.")


if __name__ == "__main__":
    main()
