"""
upload_commodity_aliases.py
============================
Reads null_alias_commodities.csv and:

  1. Upserts documents into commodity_alias_lookup:
     - Handles rows with or without crop_master_id (sets None if blank).
     - Handles `duplicates` column: if specified, attaches commodity as an alias
       to the duplicate target document instead of creating a separate entry.
     - Automatically normalises aliases (unique, lower-case, stripped).

  2. Backfills commodity_alias_lookup_id into markets_commodities
     for all matching records where it is currently null.

  3. Backfills commodity_alias_lookup_id into price_records.

Usage
-----
    # Dry-run (preview only):
    python upload_commodity_aliases.py

    # Execute writes:
    python upload_commodity_aliases.py --execute
"""

import argparse
import csv
import os
import re
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from bson import ObjectId

load_dotenv()

MONGO_URI            = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME              = os.getenv("MANDI_DB_NAME", "").strip()
MASTER_COLLECTION    = os.getenv("MASTER_COLLECTION", "").strip()
PRICE_COLLECTION     = os.getenv("PRICE_COLLECTION", "").strip()
COMMODITY_ALIAS_COLL = "commodity_alias_lookup"

DEFAULT_CSV = "null_alias_commodities.csv"


def _norm(s: str | None) -> str:
    return s.strip().lower() if s else ""


def _parse_aliases(raw_extra: str, *base_names: str) -> list[str]:
    """Build unique lowercased alias list."""
    aliases = set()
    for name in base_names:
        n = _norm(name)
        if n and n != "(no commodity_name)":
            aliases.add(n)
    if raw_extra:
        for part in raw_extra.split(","):
            a = _norm(part)
            if a:
                aliases.add(a)
    return sorted(aliases)


def _log(level: str, msg: str):
    prefix = {"OK": "✅", "WARN": "⚠️ ", "ERR": "❌", "INFO": "ℹ️ ", "DRY": "🔎"}
    print(f"  {prefix.get(level, '  ')} [{level}] {msg}")


def main(csv_path: str, dry_run: bool):
    print()
    print("=" * 70)
    print("  Commodity Alias Uploader")
    print(f"  CSV      : {csv_path}")
    print(f"  Mode     : {'DRY-RUN (no writes)' if dry_run else 'EXECUTE (writing to DB)'}")
    print(f"  Database : {DB_NAME}")
    print("=" * 70)

    if not os.path.exists(csv_path):
        print(f"\n  ❌ File not found: {csv_path}")
        sys.exit(1)

    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            comm_name = _norm(row.get("commodity_name"))
            if not comm_name or comm_name == "(no commodity_name)":
                continue

            canon_name = _norm(row.get("canonical_name")) or comm_name
            crop_id_raw = (row.get("crop_master_id") or "").strip()
            crop_id = crop_id_raw if crop_id_raw else None
            additional = (row.get("additional_aliases") or "").strip()
            duplicates = _norm(row.get("duplicates"))
            notes = (row.get("notes") or "").strip()

            rows.append({
                "line": i,
                "commodity_name": comm_name,
                "canonical_name": canon_name,
                "crop_master_id": crop_id,
                "additional": additional,
                "duplicates": duplicates,
                "notes": notes,
            })

    print(f"\n  Parsed {len(rows)} valid commodity row(s) to process.\n")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    print("  Connected to MongoDB ✓\n")

    db         = client[DB_NAME]
    alias_coll = db[COMMODITY_ALIAS_COLL]
    master     = db[MASTER_COLLECTION]
    prices     = db[PRICE_COLLECTION]

    # Map raw commodity_name -> alias doc _id (for backfill)
    commodity_to_alias_id: dict[str, object] = {}

    print("─" * 70)
    print("  STEP 1 — Upserting into commodity_alias_lookup")
    print("─" * 70)

    # First pass: standard rows (no duplicates)
    for row in rows:
        comm_name = row["commodity_name"]
        canon_name = row["canonical_name"]
        crop_id = row["crop_master_id"]
        duplicate_target = row["duplicates"]

        # If this is a duplicate pointing to another commodity, handle in pass 2
        if duplicate_target:
            continue

        aliases_list = _parse_aliases(row["additional"], comm_name, canon_name)

        if dry_run:
            existing = alias_coll.find_one({
                "$or": [
                    {"canonical_name": canon_name},
                    {"aliases": canon_name},
                    {"aliases": comm_name},
                ]
            })
            if existing:
                _log("DRY", f"Existing alias doc for '{canon_name}' -> _id={existing['_id']}")
                commodity_to_alias_id[comm_name] = existing["_id"]
            else:
                _log("DRY", f"Would INSERT '{canon_name}' (crop_id={crop_id}, aliases={aliases_list})")
                commodity_to_alias_id[comm_name] = f"new_{canon_name}"
            continue

        # Execute mode:
        existing = alias_coll.find_one({
            "$or": [
                {"canonical_name": canon_name},
                {"aliases": canon_name},
                {"aliases": comm_name},
            ]
        })

        if existing:
            update_fields = {
                "updatedAt": datetime.now(timezone.utc),
            }
            if crop_id:
                update_fields["crop_master_id"] = crop_id
            if row["notes"]:
                update_fields["notes"] = row["notes"]

            alias_coll.update_one(
                {"_id": existing["_id"]},
                {
                    "$set": update_fields,
                    "$addToSet": {"aliases": {"$each": aliases_list}},
                },
            )
            alias_id = existing["_id"]
            _log("OK", f"Updated existing alias doc '{canon_name}' -> _id={alias_id}")
        else:
            doc = {
                "canonical_name": canon_name,
                "crop_master_id": crop_id,
                "aliases": aliases_list,
                "active": True,
                "confidence": 1.0 if crop_id else 0.0,
                "notes": row["notes"] or None,
                "source_tags": ["csv_import"],
                "createdAt": datetime.now(timezone.utc),
                "updatedAt": datetime.now(timezone.utc),
            }
            res = alias_coll.insert_one(doc)
            alias_id = res.inserted_id
            _log("OK", f"Inserted new alias doc '{canon_name}' -> _id={alias_id}")

        commodity_to_alias_id[comm_name] = alias_id

    # Second pass: duplicate rows (merge with target)
    for row in rows:
        duplicate_target = row["duplicates"]
        if not duplicate_target:
            continue

        comm_name = row["commodity_name"]
        aliases_to_add = _parse_aliases(row["additional"], comm_name, row["canonical_name"])

        print(f"\n  [DUPLICATE MERGE] '{comm_name}' -> target '{duplicate_target}'")

        if dry_run:
            target_id = commodity_to_alias_id.get(duplicate_target)
            if not target_id:
                existing = alias_coll.find_one({
                    "$or": [
                        {"canonical_name": duplicate_target},
                        {"aliases": duplicate_target},
                    ]
                })
                target_id = existing["_id"] if existing else f"target_{duplicate_target}"
            commodity_to_alias_id[comm_name] = target_id
            _log("DRY", f"Would merge '{comm_name}' aliases into target '{duplicate_target}' (id={target_id})")
            continue

        # Execute mode: Find target alias doc
        target_id = commodity_to_alias_id.get(duplicate_target)
        if not target_id or isinstance(target_id, str):
            existing = alias_coll.find_one({
                "$or": [
                    {"canonical_name": duplicate_target},
                    {"aliases": duplicate_target},
                ]
            })
            if existing:
                target_id = existing["_id"]
            else:
                # If target doc doesn't exist, create it
                res = alias_coll.insert_one({
                    "canonical_name": duplicate_target,
                    "crop_master_id": row["crop_master_id"],
                    "aliases": [duplicate_target] + aliases_to_add,
                    "active": True,
                    "createdAt": datetime.now(timezone.utc),
                    "updatedAt": datetime.now(timezone.utc),
                })
                target_id = res.inserted_id

        alias_coll.update_one(
            {"_id": target_id},
            {
                "$addToSet": {"aliases": {"$each": aliases_to_add}},
                "$set": {"updatedAt": datetime.now(timezone.utc)},
            },
        )
        commodity_to_alias_id[comm_name] = target_id
        _log("OK", f"Merged '{comm_name}' into target '{duplicate_target}' -> _id={target_id}")

    # ── STEP 2: Backfill markets_commodities ─────────────────────────────
    print()
    print("─" * 70)
    print("  STEP 2 — Backfilling markets_commodities.commodity_alias_lookup_id")
    print("─" * 70)

    total_mc_updated = 0
    for comm_name, alias_id in commodity_to_alias_id.items():
        if isinstance(alias_id, str) and dry_run:
            query = {
                "commodity_name": {"$regex": f"^{re.escape(comm_name)}$", "$options": "i"},
                "commodity_alias_lookup_id": None,
            }
            n = master.count_documents(query)
            if n > 0:
                _log("DRY", f"'{comm_name}' — would update {n} master doc(s)")
                total_mc_updated += n
            continue

        query = {
            "commodity_name": {"$regex": f"^{re.escape(comm_name)}$", "$options": "i"},
            "commodity_alias_lookup_id": None,
        }
        res = master.update_many(query, {"$set": {"commodity_alias_lookup_id": alias_id}})
        if res.modified_count > 0:
            _log("OK", f"'{comm_name}' — updated {res.modified_count} master doc(s)")
            total_mc_updated += res.modified_count

    # ── STEP 3: Backfill price_records ────────────────────────────────────
    print()
    print("─" * 70)
    print("  STEP 3 — Backfilling price_records.commodity_alias_lookup_id")
    print("─" * 70)

    total_pr_updated = 0
    for comm_name, alias_id in commodity_to_alias_id.items():
        matching_mc = list(master.find(
            {"commodity_name": {"$regex": f"^{re.escape(comm_name)}$", "$options": "i"}},
            {"_id": 1}
        ))
        if not matching_mc:
            continue

        mc_ids = [m["_id"] for m in matching_mc]

        if isinstance(alias_id, str) and dry_run:
            pr_q = {
                "market_commodity_id": {"$in": mc_ids},
                "commodity_alias_lookup_id": None,
            }
            n = prices.count_documents(pr_q)
            if n > 0:
                _log("DRY", f"'{comm_name}' — would update {n} price record(s)")
                total_pr_updated += n
            continue

        pr_q = {
            "market_commodity_id": {"$in": mc_ids},
            "commodity_alias_lookup_id": None,
        }
        pr_res = prices.update_many(pr_q, {"$set": {"commodity_alias_lookup_id": alias_id}})
        if pr_res.modified_count > 0:
            _log("OK", f"'{comm_name}' — updated {pr_res.modified_count} price record(s)")
            total_pr_updated += pr_res.modified_count

    # ── SUMMARY ───────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    if dry_run:
        print(f"  DRY-RUN Complete:")
        print(f"  - Commodities processed : {len(commodity_to_alias_id)}")
        print(f"  - Master docs to update  : {total_mc_updated}")
        print(f"  - Price records to update: {total_pr_updated}")
        print(f"\n  Run with --execute to perform the database updates.")
    else:
        print(f"  EXECUTION Complete:")
        print(f"  - Commodities processed : {len(commodity_to_alias_id)}")
        print(f"  - Master docs updated    : {total_mc_updated}")
        print(f"  - Price records updated  : {total_pr_updated}")
    print("=" * 70)
    print()

    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload commodity aliases from CSV")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="CSV path")
    parser.add_argument("--execute", action="store_true", help="Execute writes")
    args = parser.parse_args()

    main(csv_path=args.csv, dry_run=not args.execute)
