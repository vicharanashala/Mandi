"""
Back up commodity_alias_lookup, attach unmatched scraped names as aliases
on the closest canonical (or alias) lookup row, then fill
commodity_alias_lookup_id on markets_commodities and price_records.

Usage:
    python backfill_commodity_aliases.py
"""

from __future__ import annotations

import os
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateMany

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = (os.getenv("MANDI_DB_NAME") or "").strip()
MASTER_COLLECTION = (os.getenv("MASTER_COLLECTION") or "").strip()
PRICE_COLLECTION = (os.getenv("PRICE_COLLECTION") or "").strip()
LOOKUP_COLLECTION = "commodity_alias_lookup"

# Reject weak / ambiguous mappings rather than attach the wrong crop.
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
    """Scraped-name variants used for similarity (Agmarknet 'a(b)' style)."""
    name = _norm(name)
    if not name:
        return []
    parts = [name, _compact(name)]
    for chunk in re.split(r"[/(),\-]", name):
        chunk = _norm(chunk)
        if chunk:
            parts.append(chunk)
            parts.append(_compact(chunk))
    # unique, longest first so exact-ish strings win ties
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
    # contained token (min 5 chars) — coriander(leaves) vs coriander
    if ca and cb and len(min(ca, cb, key=len)) >= 5:
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
    if not DB_NAME or not MASTER_COLLECTION:
        raise SystemExit("[ERROR] MANDI_DB_NAME / MASTER_COLLECTION missing from .env")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    db = client[DB_NAME]
    lookup = db[LOOKUP_COLLECTION]
    master = db[MASTER_COLLECTION]
    price = db[PRICE_COLLECTION] if PRICE_COLLECTION else None

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_name = f"{LOOKUP_COLLECTION}_backup_{stamp}"
    print(f"[INFO] Connected. db={DB_NAME!r}")
    print(f"[INFO] Duplicating {LOOKUP_COLLECTION!r} → {backup_name!r}")
    list(lookup.aggregate([{"$match": {}}, {"$out": backup_name}]))
    backup_n = db[backup_name].count_documents({})
    src_n = lookup.count_documents({})
    if backup_n != src_n:
        raise SystemExit(
            f"[ERROR] Backup count mismatch: source={src_n} backup={backup_n}"
        )
    print(f"[OK] Backup has {backup_n} document(s).")

    lookup_docs = list(
        lookup.find(
            {"active": {"$ne": False}},
            {"canonical_name": 1, "aliases": 1},
        )
    )
    print(f"[INFO] Active lookup docs: {len(lookup_docs)}")

    unmatched = list(
        master.aggregate(
            [
                {"$match": {"commodity_alias_lookup_id": None}},
                {
                    "$group": {
                        "_id": {
                            "$toLower": {"$ifNull": ["$commodity_name", ""]}
                        },
                        "n": {"$sum": 1},
                        "ids": {"$push": "$_id"},
                    }
                },
            ],
            allowDiskUse=True,
        )
    )
    print(f"[INFO] Unique unmatched commodity names: {len(unmatched)}")

    mapped: list[tuple] = []
    skipped: list[tuple] = []
    alias_ops: dict = defaultdict(set)  # lookup_id -> aliases to add
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

    print(f"[INFO] Mapped names: {len(mapped)}  skipped: {len(skipped)}")

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

    master_modified = 0
    for op in master_ops:
        res = master.bulk_write([op], ordered=False)
        master_modified += res.modified_count

    price_modified = 0
    if price is not None:
        # Fill prices that still have null CAL for just-updated masters.
        for row in unmatched:
            name = (row["_id"] or "").strip()
            if not name:
                continue
            # find mapping
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

    remaining = master.count_documents({"commodity_alias_lookup_id": None})
    print()
    print("=" * 64)
    print(f"[DONE] backup collection     : {backup_name}")
    print(f"       lookup docs updated   : {alias_modified}")
    print(f"       unique names mapped   : {len(mapped)}")
    print(f"       unique names skipped  : {len(skipped)}")
    print(f"       markets_commodities   : {master_modified} doc(s) set")
    print(f"       price_records         : {price_modified} doc(s) set")
    print(f"       remaining null CAL    : {remaining}")
    print("=" * 64)

    print("\n--- Mapped (top 40 by volume) ---")
    for n, name, canon, score, second, lid in sorted(mapped, key=lambda x: -x[0])[:40]:
        print(
            f"  n={n:5d}  {name!r:40s} → {canon!r:30s}  "
            f"score={score:.3f} margin={score-second:.3f}  id={lid}"
        )

    print("\n--- Skipped (top 30 by volume) ---")
    for n, name, reason, canon, score in sorted(skipped, key=lambda x: -x[0])[:30]:
        print(f"  n={n:5d}  {name!r:40s}  {reason}  closest={canon!r}")

    client.close()
    print("\n[INFO] Connection closed.")


if __name__ == "__main__":
    main()
