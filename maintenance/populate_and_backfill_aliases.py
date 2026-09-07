"""
populate_and_backfill_aliases.py
================================
Populate commodity_alias_lookup using agriai.crop_master and scraped name mapping,
then backfill commodity_alias_lookup_id into markets_commodities and price_records.

Usage:
    .venv/bin/python populate_and_backfill_aliases.py
"""

from __future__ import annotations

import os
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateMany

# Load .env dynamically from workspace root
_ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=_ROOT_DIR / ".env", override=True)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = (os.getenv("MANDI_DB_NAME") or "Price").strip()
MASTER_COLLECTION = (os.getenv("MASTER_COLLECTION") or "markets_commodities").strip()
PRICE_COLLECTION = (os.getenv("PRICE_COLLECTION") or "price_records").strip()
LOOKUP_COLLECTION = "commodity_alias_lookup"

AGRIAI_DB_NAME = (os.getenv("MANDI_DB_NAME_ANNAM") or "agriai").strip()
CROP_MASTER_COLLECTION = (os.getenv("COLLECTION_ANNAM") or "crop_master").strip()

MIN_SCORE = 0.78
MIN_MARGIN = 0.04


def _norm(val) -> str:
    if val is None:
        return ""
    s = unicodedata.normalize("NFKC", str(val)).strip().lower()
    return re.sub(r"\s+", " ", s)


def _compact(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s)


def _candidates(name: str) -> list[str]:
    name = _norm(name)
    if not name:
        return []
    parts = [name, _compact(name)]
    for chunk in re.split(r"[/(),\-]", name):
        chunk = _norm(chunk)
        if chunk:
            parts.append(chunk)
            parts.append(_compact(chunk))
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    ca, cb = _compact(a), _compact(b)
    if ca and ca == cb:
        return 0.99
    r = SequenceMatcher(None, a, b).ratio()
    if ca and cb:
        r = max(r, SequenceMatcher(None, ca, cb).ratio())
    if ca and cb and len(min(ca, cb, key=len)) >= 4:
        if ca in cb or cb in ca:
            r = max(r, 0.88)
    return r


def _score_doc(name: str, doc: dict) -> float:
    cands = _candidates(name)
    best = 0.0
    canonical = _norm(doc.get("canonical_name"))
    for cand in cands:
        best = max(best, _ratio(cand, canonical))
        for alias in doc.get("aliases") or []:
            best = max(best, _ratio(cand, _norm(alias)))
            if best >= 0.99:
                return best
    return best


def _best_lookup(name: str, lookup_docs: list[dict]) -> tuple[dict | None, float, float]:
    ranked = sorted(
        ((_score_doc(name, doc), doc) for doc in lookup_docs),
        key=lambda x: x[0],
        reverse=True,
    )
    if not ranked:
        return None, 0.0, 0.0
    top_score, top_doc = ranked[0]
    second = ranked[1][0] if len(ranked) > 1 else 0.0
    return top_doc, top_score, second


def main() -> None:
    print(f"[INFO] Connecting to MongoDB... (DB: {DB_NAME})")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    db = client[DB_NAME]
    lookup = db[LOOKUP_COLLECTION]
    master = db[MASTER_COLLECTION]
    price = db[PRICE_COLLECTION] if PRICE_COLLECTION else None

    # agriai database connection
    db_agriai = client[AGRIAI_DB_NAME]
    crop_master = db_agriai[CROP_MASTER_COLLECTION]
    crop_docs = list(crop_master.find({}))
    print(f"[INFO] Reference agriai.crop_master docs loaded: {len(crop_docs)}")

    # Step 1: Backup commodity_alias_lookup
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_name = f"{LOOKUP_COLLECTION}_backup_{stamp}"
    print(f"[INFO] Duplicating {LOOKUP_COLLECTION!r} → {backup_name!r}")
    list(lookup.aggregate([{"$match": {}}, {"$out": backup_name}]))
    backup_n = db[backup_name].count_documents({})
    src_n = lookup.count_documents({})
    if backup_n != src_n:
        raise SystemExit(f"[ERROR] Backup count mismatch: source={src_n} backup={backup_n}")
    print(f"[OK] Backup verified with {backup_n} documents.")

    # Step 2: Fetch active lookup docs
    lookup_docs = list(lookup.find({"active": {"$ne": False}}))
    print(f"[INFO] Active lookup docs in {LOOKUP_COLLECTION!r}: {len(lookup_docs)}")

    # Step 3: Fetch unmatched scraped names from markets_commodities
    unmatched = list(
        master.aggregate(
            [
                {"$match": {"commodity_alias_lookup_id": None}},
                {
                    "$group": {
                        "_id": {"$toLower": {"$ifNull": ["$commodity_name", ""]}},
                        "n": {"$sum": 1},
                        "ids": {"$push": "$_id"},
                    }
                },
            ],
            allowDiskUse=True,
        )
    )
    print(f"[INFO] Unique unmatched commodity names: {len(unmatched)}")

    # Step 4: Resolution & Enrichment Phase using agriai.crop_master and Candidate Matching
    new_lookup_inserted = 0
    alias_attached_count = 0

    # Build index of crop_docs
    crop_map = []
    for cdoc in crop_docs:
        cname = (cdoc.get("name") or cdoc.get("canonical_name") or "").strip()
        if not cname:
            continue
        aliases = [cname.lower()]
        for item in (cdoc.get("aliases") or []):
            if isinstance(item, str):
                aliases.append(item.strip().lower())
            elif isinstance(item, dict):
                for k in ["english_representation", "alias", "name", "native_representation"]:
                    v = item.get(k)
                    if v and isinstance(v, str):
                        aliases.append(v.strip().lower())
        crop_map.append({"doc": cdoc, "title": cname, "aliases": set(aliases)})

    for row in unmatched:
        name = (row["_id"] or "").strip()
        if not name:
            continue

        # Check if already matched in existing lookup
        top_doc, score, second = _best_lookup(name, lookup_docs)
        if top_doc and score >= MIN_SCORE and (score - second >= MIN_MARGIN or score >= 0.99):
            continue  # Will be mapped in backfill step

        cands = _candidates(name)
        clean_name = re.sub(r"[\(\)/\-]", " ", name).strip()
        words = [
            w
            for w in clean_name.split()
            if len(w) >= 3
            and w not in ["wet", "dry", "raw", "small", "big", "fresh", "green", "red", "white", "black", "seeds", "seed", "leaf", "leaves", "split"]
        ]

        found_crop = None
        for cmap in crop_map:
            # Candidate / alias matching against agriai crop_master
            matched_alias = False
            for cand in cands:
                if cand in cmap["aliases"]:
                    matched_alias = True
                    break
            if not matched_alias and any(clean_name == a or clean_name in a for a in cmap["aliases"]):
                matched_alias = True
            if not matched_alias and words and any(w in a for a in cmap["aliases"] for w in words):
                matched_alias = True

            if matched_alias:
                found_crop = (cmap["doc"], cmap["title"])
                break

        if found_crop:
            cdoc, crop_title = found_crop
            crop_title_lower = crop_title.lower()

            # Check if a commodity_alias_lookup record exists for this crop title (or fuzzy best lookup)
            existing_lookup, l_score, _ = _best_lookup(crop_title_lower, lookup_docs)
            if not existing_lookup or l_score < 0.85:
                existing_lookup = next(
                    (ld for ld in lookup_docs if (ld.get("canonical_name") or "").strip().lower() == crop_title_lower),
                    None,
                )

            if existing_lookup:
                res = lookup.update_one(
                    {"_id": existing_lookup["_id"]},
                    {
                        "$addToSet": {"aliases": name},
                        "$set": {"updatedAt": datetime.now(timezone.utc)},
                    },
                )
                if res.modified_count > 0:
                    if "aliases" not in existing_lookup or existing_lookup["aliases"] is None:
                        existing_lookup["aliases"] = []
                    if name not in existing_lookup["aliases"]:
                        existing_lookup["aliases"].append(name)
                    alias_attached_count += 1
            else:
                new_doc = {
                    "canonical_name": crop_title,
                    "aliases": sorted(list(set([name, crop_title_lower]))),
                    "active": True,
                    "createdAt": datetime.now(timezone.utc),
                    "updatedAt": datetime.now(timezone.utc),
                }
                res = lookup.insert_one(new_doc)
                new_doc["_id"] = res.inserted_id
                lookup_docs.append(new_doc)
                new_lookup_inserted += 1

    print(f"[INFO] Enrichment Summary:")
    print(f"       - New canonical lookup entries inserted : {new_lookup_inserted}")
    print(f"       - Scraped aliases attached to lookup    : {alias_attached_count}")

    # Re-fetch active lookup docs after enrichment
    lookup_docs = list(lookup.find({"active": {"$ne": False}}))

    # Step 5: Backfill commodity_alias_lookup_id into markets_commodities
    mapped: list[tuple] = []
    skipped: list[tuple] = []
    alias_ops: dict = defaultdict(set)
    master_ops: list[UpdateMany] = []

    for row in unmatched:
        name = (row["_id"] or "").strip()
        ids = row["ids"]
        n = row["n"]
        if not name:
            skipped.append((n, name, "empty commodity_name", None, 0.0))
            continue

        doc, score, second = _best_lookup(name, lookup_docs)
        if doc is None:
            skipped.append((n, name, "no lookup docs", None, 0.0))
            continue
        if score < MIN_SCORE:
            skipped.append(
                (n, name, f"score {score:.3f} < {MIN_SCORE}", doc.get("canonical_name"), score)
            )
            continue
        if (score - second) < MIN_MARGIN and score < 0.99:
            skipped.append(
                (
                    n,
                    name,
                    f"ambiguous top={score:.3f} second={second:.3f}",
                    doc.get("canonical_name"),
                    score,
                )
            )
            continue

        lid = doc["_id"]
        alias_ops[lid].add(name)
        master_ops.append(
            UpdateMany(
                {"_id": {"$in": ids}},
                {"$set": {"commodity_alias_lookup_id": lid}},
            )
        )
        mapped.append((n, name, doc.get("canonical_name"), score, second, lid))

    print(f"\n[INFO] Mapped names: {len(mapped)} | Skipped names: {len(skipped)}")

    # Apply alias updates
    alias_modified = 0
    for lid, names in alias_ops.items():
        res = lookup.update_one(
            {"_id": lid},
            {
                "$addToSet": {"aliases": {"$each": sorted(names)}},
                "$set": {"updatedAt": datetime.now(timezone.utc)},
            },
        )
        alias_modified += res.modified_count

    # Apply master collection updates
    master_modified = 0
    for op in master_ops:
        res = master.bulk_write([op], ordered=False)
        master_modified += res.modified_count

    # Step 6: Backfill commodity_alias_lookup_id into price_records
    price_modified = 0
    if price is not None:
        print("[INFO] Backfilling price_records...")
        for row in unmatched:
            name = (row["_id"] or "").strip()
            if not name:
                continue
            hit = next((m for m in mapped if m[1] == name), None)
            if not hit:
                continue
            lid = hit[5]
            ids = row["ids"]
            BATCH = 500
            for i in range(0, len(ids), BATCH):
                res = price.update_many(
                    {
                        "market_commodity_id": {"$in": ids[i : i + BATCH]},
                        "commodity_alias_lookup_id": None,
                    },
                    {"$set": {"commodity_alias_lookup_id": lid}},
                )
                price_modified += res.modified_count

    remaining_master_null = master.count_documents({"commodity_alias_lookup_id": None})
    remaining_price_null = price.count_documents({"commodity_alias_lookup_id": None}) if price is not None else 0

    print()
    print("=" * 66)
    print(f"[DONE] Backup Collection           : {backup_name}")
    print(f"       New Lookup Docs Inserted    : {new_lookup_inserted}")
    print(f"       Lookup Docs Updated         : {alias_modified}")
    print(f"       Unique Names Mapped         : {len(mapped)}")
    print(f"       Unique Names Skipped        : {len(skipped)}")
    print(f"       markets_commodities Updated : {master_modified} doc(s)")
    print(f"       price_records Updated       : {price_modified} doc(s)")
    print(f"       Remaining Master Null CAL   : {remaining_master_null}")
    print(f"       Remaining Price Null CAL    : {remaining_price_null}")
    print("=" * 66)

    print("\n--- Mapped Commodities (Top 30 by volume) ---")
    for n, name, canon, score, second, lid in sorted(mapped, key=lambda x: -x[0])[:30]:
        print(
            f"  n={n:5d}  {name!r:40s} → {canon!r:30s}  score={score:.3f} id={lid}"
        )

    client.close()
    print("\n[INFO] Connection closed successfully.")


if __name__ == "__main__":
    main()
