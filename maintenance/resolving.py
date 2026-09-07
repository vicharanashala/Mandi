"""
resolving.py
============
Diagnose duplicate / near-duplicate problems in `markets_commodities`.

The unique index on
  (source_system, state, market_name, commodity_name, variety)
prevents *exact* key collisions. Apparent duplicates usually come from:

  1. Cross-source copies   — leftover `agmarknet2` vs `agmarknet` labels
                             (canonical source_system is `agmarknet` only)
  2. Market-name aliases   — different market_name strings that resolve
                             to the same market_id (e.g. pune / pune-khadki)
  3. Commodity-alias merges — different commodity_name strings sharing one
                             commodity_alias_lookup_id
  4. Data-quality holes    — null market_id / commodity_alias_lookup_id /
                             market_name

Usage
-----
  python resolving.py
  python resolving.py --samples 10
  python resolving.py --json report.json
  python resolving.py --merge --execute --skip-diagnose
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, DuplicateKeyError


def _log(msg: str = "") -> None:
    print(msg, flush=True)

load_dotenv(Path(__file__).resolve().parent / ".env")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = (os.getenv("MANDI_DB_NAME") or "").strip()
MASTER_COLLECTION = (os.getenv("MASTER_COLLECTION") or "").strip()
PRICE_COLLECTION = (os.getenv("PRICE_COLLECTION") or "").strip()

# Logical identity used by the uploader
MASTER_KEY_FIELDS = (
    "source_system",
    "state",
    "market_name",
    "commodity_name",
    "variety",
)


def _norm_str(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip().lower()
    return s or None


def connect():
    if not DB_NAME or not MASTER_COLLECTION:
        raise SystemExit(
            "[ERROR] MANDI_DB_NAME / MASTER_COLLECTION missing from .env"
        )
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    db = client[DB_NAME]
    return client, db[MASTER_COLLECTION], db[PRICE_COLLECTION] if PRICE_COLLECTION else None


def _print_header(title: str) -> None:
    _log("\n" + "=" * 72)
    _log(title)
    _log("=" * 72)


def check_indexes(master_coll) -> dict:
    info = master_coll.index_information()
    unique = [
        name
        for name, meta in info.items()
        if meta.get("unique") and name != "_id_"
    ]
    _print_header("1. INDEXES")
    for name, meta in info.items():
        print(f"  {name}: key={meta.get('key')} unique={bool(meta.get('unique'))}")
    if "unique_market_commodity" not in info:
        print("  [PROBLEM] Expected unique_market_commodity index is MISSING.")
    elif not info["unique_market_commodity"].get("unique"):
        print("  [PROBLEM] unique_market_commodity exists but is NOT unique.")
    else:
        print("  [OK] unique_market_commodity unique index is present.")
    return {"indexes": list(info.keys()), "unique_indexes": unique}


def check_exact_master_key_dups(master_coll, samples: int) -> dict:
    """Exact duplicates on the unique compound key (should be ~0)."""
    pipeline = [
        {
            "$group": {
                "_id": {f: f"${f}" for f in MASTER_KEY_FIELDS},
                "count": {"$sum": 1},
                "ids": {"$push": "$_id"},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
    ]
    groups = list(master_coll.aggregate(pipeline, allowDiskUse=True))
    extra = sum(g["count"] - 1 for g in groups)

    _print_header("2. EXACT MASTER-KEY DUPLICATES")
    print(f"  Duplicate groups : {len(groups)}")
    print(f"  Extra documents  : {extra}")
    if not groups:
        print("  [OK] No exact (source, state, market, commodity, variety) dups.")
    else:
        print("  [PROBLEM] Unique index is not enforcing uniqueness (or was added late).")
        for g in groups[:samples]:
            print(f"    n={g['count']} key={g['_id']} ids={g['ids'][:5]}")

    return {
        "duplicate_groups": len(groups),
        "extra_documents": extra,
        "samples": [
            {"key": g["_id"], "count": g["count"], "ids": [str(i) for i in g["ids"][:10]]}
            for g in groups[:samples]
        ],
    }


def check_cross_source_dups(master_coll, samples: int) -> dict:
    """
    Same physical market+commodity+variety appearing under multiple
    source_system values (almost always agmarknet + agmarknet2).
    """
    pipeline = [
        {
            "$group": {
                "_id": {
                    "state": {"$toLower": {"$ifNull": ["$state", ""]}},
                    "market_name": {"$toLower": {"$ifNull": ["$market_name", ""]}},
                    "commodity_name": {
                        "$toLower": {"$ifNull": ["$commodity_name", ""]}
                    },
                    "variety": "$variety",
                },
                "count": {"$sum": 1},
                "sources": {"$addToSet": "$source_system"},
                "ids": {"$push": {"id": "$_id", "source": "$source_system"}},
            }
        },
        {"$match": {"sources.1": {"$exists": True}}},
        {"$sort": {"count": -1}},
    ]
    groups = list(master_coll.aggregate(pipeline, allowDiskUse=True))
    extra = sum(g["count"] - 1 for g in groups)

    pair_counts: Counter = Counter()
    for g in groups:
        pair_counts[tuple(sorted(g["sources"]))] += 1

    _print_header("3. CROSS-SOURCE DUPLICATES (same market+commodity+variety)")
    print(f"  Overlap groups   : {len(groups)}")
    print(f"  Extra documents  : {extra}  (docs that are copies of another source)")
    print("  Source-set breakdown:")
    for pair, n in pair_counts.most_common():
        print(f"    {n:6d}  {pair}")

    if groups:
        print("\n  [PROBLEM] Same logical row ingested from multiple sources.")
        print("  Dominant cause is usually leftover agmarknet2 vs agmarknet.")
        print(f"  Samples (first {samples}):")
        for g in groups[:samples]:
            print(
                f"    n={g['count']} sources={sorted(g['sources'])} "
                f"key={g['_id']}"
            )

    return {
        "overlap_groups": len(groups),
        "extra_documents": extra,
        "source_set_counts": {
            " + ".join(p): n for p, n in pair_counts.most_common()
        },
        "samples": [
            {
                "key": g["_id"],
                "count": g["count"],
                "sources": sorted(g["sources"]),
                "ids": [
                    {"id": str(x["id"]), "source": x["source"]}
                    for x in g["ids"][:10]
                ],
            }
            for g in groups[:samples]
        ],
    }


def check_market_id_name_variants(master_coll, samples: int) -> dict:
    """
    Within one source: different market_name strings that share market_id
    for the same commodity+variety (alias / spelling variants).
    """
    pipeline = [
        {"$match": {"market_id": {"$ne": None}}},
        {
            "$group": {
                "_id": {
                    "source_system": "$source_system",
                    "market_id": "$market_id",
                    "commodity_name": "$commodity_name",
                    "variety": "$variety",
                },
                "count": {"$sum": 1},
                "market_names": {"$addToSet": "$market_name"},
                "states": {"$addToSet": "$state"},
                "ids": {"$push": "$_id"},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
    ]
    groups = list(master_coll.aggregate(pipeline, allowDiskUse=True))
    name_variants = [g for g in groups if len(g["market_names"]) > 1]
    extra = sum(g["count"] - 1 for g in name_variants)

    _print_header("4. MARKET-NAME VARIANTS (same market_id within a source)")
    print(f"  Variant groups   : {len(name_variants)}")
    print(f"  Extra documents  : {extra}")
    if name_variants:
        print(
            "  [PROBLEM] Distinct market_name strings resolve to one market_id "
            "(e.g. pune vs pune-khadki, shahabad apmc vs shahabad(new mandi) apmc)."
        )
        for g in name_variants[:samples]:
            print(
                f"    n={g['count']} src={g['_id']['source_system']} "
                f"names={sorted(g['market_names'])} "
                f"commodity={g['_id']['commodity_name']!r} "
                f"variety={g['_id']['variety']!r}"
            )
    else:
        print("  [OK] No same-source market_name variants sharing market_id.")

    return {
        "variant_groups": len(name_variants),
        "extra_documents": extra,
        "samples": [
            {
                "source_system": g["_id"]["source_system"],
                "market_id": str(g["_id"]["market_id"]),
                "commodity_name": g["_id"]["commodity_name"],
                "variety": g["_id"]["variety"],
                "market_names": sorted(g["market_names"]),
                "states": sorted(g["states"]),
                "count": g["count"],
                "ids": [str(i) for i in g["ids"][:10]],
            }
            for g in name_variants[:samples]
        ],
    }


def check_commodity_alias_collisions(master_coll, samples: int) -> dict:
    """Many commodity_name strings pointing at one commodity_alias_lookup_id."""
    pipeline = [
        {"$match": {"commodity_alias_lookup_id": {"$ne": None}}},
        {
            "$group": {
                "_id": "$commodity_alias_lookup_id",
                "names": {"$addToSet": "$commodity_name"},
                "count": {"$sum": 1},
            }
        },
        {"$project": {"names": 1, "count": 1, "n_names": {"$size": "$names"}}},
        {"$match": {"n_names": {"$gt": 1}}},
        {"$sort": {"n_names": -1, "count": -1}},
    ]
    groups = list(master_coll.aggregate(pipeline, allowDiskUse=True))

    _print_header("5. COMMODITY ALIAS COLLISIONS (many names → one lookup id)")
    print(f"  Alias ids with >1 commodity_name : {len(groups)}")
    if groups:
        print(
            "  [INFO] Not always a bug — aliases like green chili/chilli are expected.\n"
            "  Flag over-broad merges (unrelated crops sharing one id)."
        )
        for g in groups[:samples]:
            names = sorted(n for n in g["names"] if n is not None)
            print(
                f"    n_names={g['n_names']} docs={g['count']} "
                f"id={g['_id']} names={names}"
            )
    else:
        print("  [OK] No multi-name alias collisions.")

    return {
        "alias_ids_with_multiple_names": len(groups),
        "samples": [
            {
                "commodity_alias_lookup_id": str(g["_id"]),
                "n_names": g["n_names"],
                "doc_count": g["count"],
                "names": sorted(n for n in g["names"] if n is not None),
            }
            for g in groups[:samples]
        ],
    }


def check_market_id_commodity_alias_dups(master_coll, samples: int) -> dict:
    """
    Same market_id + commodity_alias_lookup_id + variety + source, but
    different commodity_name / market_name — semantic duplicates via lookups.
    """
    pipeline = [
        {
            "$match": {
                "market_id": {"$ne": None},
                "commodity_alias_lookup_id": {"$ne": None},
            }
        },
        {
            "$group": {
                "_id": {
                    "source_system": "$source_system",
                    "market_id": "$market_id",
                    "commodity_alias_lookup_id": "$commodity_alias_lookup_id",
                    "variety": "$variety",
                },
                "count": {"$sum": 1},
                "market_names": {"$addToSet": "$market_name"},
                "commodity_names": {"$addToSet": "$commodity_name"},
                "ids": {"$push": "$_id"},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
    ]
    groups = list(master_coll.aggregate(pipeline, allowDiskUse=True))
    extra = sum(g["count"] - 1 for g in groups)

    _print_header(
        "6. LOOKUP-KEY DUPLICATES "
        "(same market_id + commodity_alias_lookup_id + variety + source)"
    )
    print(f"  Duplicate groups : {len(groups)}")
    print(f"  Extra documents  : {extra}")
    if groups:
        print(
            "  [PROBLEM] Different name strings collapse to the same lookup ids.\n"
            "  Often caused by over-broad commodity aliases or market aliases."
        )
        for g in groups[:samples]:
            print(
                f"    n={g['count']} src={g['_id']['source_system']} "
                f"markets={sorted(g['market_names'])} "
                f"commodities={sorted(g['commodity_names'])} "
                f"variety={g['_id']['variety']!r}"
            )
    else:
        print("  [OK] No lookup-key collisions.")

    return {
        "duplicate_groups": len(groups),
        "extra_documents": extra,
        "samples": [
            {
                "source_system": g["_id"]["source_system"],
                "market_id": str(g["_id"]["market_id"]),
                "commodity_alias_lookup_id": str(
                    g["_id"]["commodity_alias_lookup_id"]
                ),
                "variety": g["_id"]["variety"],
                "market_names": sorted(g["market_names"]),
                "commodity_names": sorted(
                    n for n in g["commodity_names"] if n is not None
                ),
                "count": g["count"],
                "ids": [str(i) for i in g["ids"][:10]],
            }
            for g in groups[:samples]
        ],
    }


def check_data_quality(master_coll) -> dict:
    total = master_coll.count_documents({})
    by_source = {
        row["_id"]: row["n"]
        for row in master_coll.aggregate(
            [
                {"$group": {"_id": "$source_system", "n": {"$sum": 1}}},
                {"$sort": {"n": -1}},
            ]
        )
    }
    stats = {
        "total_documents": total,
        "by_source_system": by_source,
        "null_market_id": master_coll.count_documents({"market_id": None}),
        "null_commodity_alias_lookup_id": master_coll.count_documents(
            {"commodity_alias_lookup_id": None}
        ),
        "null_market_name": master_coll.count_documents({"market_name": None}),
        "null_commodity_name": master_coll.count_documents(
            {"commodity_name": None}
        ),
        "null_variety": master_coll.count_documents({"variety": None}),
        "empty_variety": master_coll.count_documents({"variety": ""}),
    }

    _print_header("7. DATA QUALITY / HOLES")
    print(f"  Total documents              : {stats['total_documents']}")
    print("  By source_system:")
    for src, n in by_source.items():
        print(f"    {n:6d}  {src}")
    print(f"  null market_id               : {stats['null_market_id']}")
    print(
        f"  null commodity_alias_lookup_id: "
        f"{stats['null_commodity_alias_lookup_id']}"
    )
    print(f"  null market_name             : {stats['null_market_name']}")
    print(f"  null commodity_name          : {stats['null_commodity_name']}")
    print(f"  null variety                 : {stats['null_variety']}")
    print(f"  empty variety ''             : {stats['empty_variety']}")

    problems = []
    if stats["null_market_id"]:
        problems.append(
            f"{stats['null_market_id']} docs missing market_id "
            "(run check_db.py / improve available_mandi aliases)"
        )
    if stats["null_commodity_alias_lookup_id"]:
        problems.append(
            f"{stats['null_commodity_alias_lookup_id']} docs missing "
            "commodity_alias_lookup_id"
        )
    if stats["null_market_name"]:
        problems.append(
            f"{stats['null_market_name']} docs have null market_name "
            "(breaks useful uniqueness for those rows)"
        )
    if problems:
        print("\n  [PROBLEMS]")
        for p in problems:
            print(f"    - {p}")

    return stats


def check_price_orphan_refs(master_coll, price_coll, samples: int) -> dict | None:
    if price_coll is None:
        return None

    # Master ids referenced by more than one source-overlap set are handled
    # above; here we only spot orphan price rows whose market_commodity_id
    # no longer exists (cleanup leftover risk after dedupe).
    pipeline = [
        {
            "$lookup": {
                "from": MASTER_COLLECTION,
                "localField": "market_commodity_id",
                "foreignField": "_id",
                "as": "master",
            }
        },
        {"$match": {"master": {"$size": 0}}},
        {"$limit": max(samples, 1)},
        {"$project": {"_id": 1, "market_commodity_id": 1, "date": 1}},
    ]
    # Counting orphans fully can be expensive; do a cheap distinct-size compare.
    price_refs = price_coll.distinct("market_commodity_id")
    master_ids = set(master_coll.distinct("_id"))
    orphan_ids = [i for i in price_refs if i not in master_ids]

    _print_header("8. PRICE_RECORDS ORPHAN market_commodity_id")
    print(f"  Distinct price FKs           : {len(price_refs)}")
    print(f"  Orphan FKs (no master doc)   : {len(orphan_ids)}")
    if orphan_ids:
        print("  [PROBLEM] price_records point at deleted/missing master docs.")
        for oid in orphan_ids[:samples]:
            n = price_coll.count_documents({"market_commodity_id": oid})
            print(f"    market_commodity_id={oid} price_rows={n}")
    else:
        print("  [OK] No orphan price FK references found.")

    return {
        "distinct_price_fks": len(price_refs),
        "orphan_fk_count": len(orphan_ids),
        "orphan_samples": [str(i) for i in orphan_ids[:samples]],
    }


def summarise(report: dict) -> None:
    _print_header("SUMMARY — what is actually wrong")
    cross = report["cross_source"]
    market_var = report["market_name_variants"]
    lookup = report["lookup_key_dups"]
    exact = report["exact_master_key"]
    quality = report["data_quality"]

    lines = []
    if exact["duplicate_groups"]:
        lines.append(
            f"Exact master-key duplicates: {exact['duplicate_groups']} groups "
            f"({exact['extra_documents']} extra docs)."
        )
    else:
        lines.append(
            "No exact master-key duplicates — unique index is doing its job."
        )

    if cross["overlap_groups"]:
        lines.append(
            f"MAIN ISSUE: {cross['overlap_groups']} cross-source overlaps "
            f"({cross['extra_documents']} extra docs). "
            f"Pairs: {cross['source_set_counts']}"
        )
    if market_var["variant_groups"]:
        lines.append(
            f"Market-name alias variants: {market_var['variant_groups']} groups "
            f"({market_var['extra_documents']} extra docs) share one market_id."
        )
    if lookup["duplicate_groups"]:
        lines.append(
            f"Lookup-key collapses: {lookup['duplicate_groups']} groups "
            f"({lookup['extra_documents']} extra docs) share "
            "market_id+commodity_alias_lookup_id+variety."
        )
    if quality["null_market_id"] or quality["null_commodity_alias_lookup_id"]:
        lines.append(
            f"Unresolved lookups: market_id null={quality['null_market_id']}, "
            f"commodity_alias_lookup_id null="
            f"{quality['null_commodity_alias_lookup_id']}."
        )

    for i, line in enumerate(lines, 1):
        print(f"  {i}. {line}")

    print(
        "\n  Next steps (not done by this script):\n"
        "    - If any source_system='agmarknet2' remains, run:\n"
        "      python resolving.py --merge --execute --skip-diagnose\n"
        "    - Tighten available_mandi aliases so one mandi ≠ many names\n"
        "      unless intentional sub-yards.\n"
        "    - Review over-broad commodity_alias_lookup merges."
    )


def _find_agmarknet_overlap_pairs(master_coll) -> list[dict]:
    """
    Return overlap pairs where the same (state, market, commodity, variety)
    exists under both `agmarknet` and `agmarknet2`.

    Keep `agmarknet`, drop `agmarknet2` (canonical label is agmarknet only).
    """
    pipeline = [
        {"$match": {"source_system": {"$in": ["agmarknet", "agmarknet2"]}}},
        {
            "$group": {
                "_id": {
                    "state": "$state",
                    "market_name": "$market_name",
                    "commodity_name": "$commodity_name",
                    "variety": "$variety",
                },
                "docs": {
                    "$push": {
                        "id": "$_id",
                        "src": "$source_system",
                        "market_id": "$market_id",
                        "cal": "$commodity_alias_lookup_id",
                    }
                },
                "sources": {"$addToSet": "$source_system"},
            }
        },
        {"$match": {"sources": {"$all": ["agmarknet", "agmarknet2"]}}},
    ]
    pairs = []
    for g in master_coll.aggregate(pipeline, allowDiskUse=True):
        by_src: dict[str, list] = {}
        for d in g["docs"]:
            by_src.setdefault(d["src"], []).append(d)
        # Keep canonical agmarknet; remove leftover agmarknet2.
        old = by_src["agmarknet2"][0]
        keep = by_src["agmarknet"][0]
        pairs.append({"key": g["_id"], "old": old, "keep": keep})
    return pairs


def cleanup_orphan_prices(master_coll, price_coll, *, dry_run: bool = True) -> dict:
    """Delete price_records whose market_commodity_id no longer exists in master."""
    _print_header(
        f"CLEANUP orphan price_records  ({'DRY-RUN' if dry_run else 'LIVE'})"
    )
    master_ids = set(master_coll.distinct("_id"))
    price_fks = price_coll.distinct("market_commodity_id")
    orphans = [i for i in price_fks if i not in master_ids]
    _log(f"  Orphan market_commodity_id values: {len(orphans)}")
    deleted = 0
    if orphans:
        if dry_run:
            deleted = price_coll.count_documents(
                {"market_commodity_id": {"$in": orphans}}
            )
            _log(f"  [DRY-RUN] Would delete {deleted} orphan price row(s)")
        else:
            for i in range(0, len(orphans), 2000):
                res = price_coll.delete_many(
                    {"market_commodity_id": {"$in": orphans[i : i + 2000]}}
                )
                deleted += res.deleted_count
            _log(f"  Deleted {deleted} orphan price row(s)")
    else:
        _log("  [OK] No orphans.")
    return {"orphan_fks": len(orphans), "price_rows_deleted": deleted, "dry_run": dry_run}


def _rename_agmarknet2_only(
    master_coll,
    price_coll,
    *,
    dry_run: bool = True,
) -> dict:
    """
    Rename agmarknet2-only masters to agmarknet.
    If a key already exists under agmarknet, treat it as an overlap and merge
    instead of a blind rename (avoids DuplicateKeyError).
    """
    _log("  Resolving remaining agmarknet2 → agmarknet renames…")
    pipeline = [
        {"$match": {"source_system": {"$in": ["agmarknet", "agmarknet2"]}}},
        {
            "$group": {
                "_id": {
                    "state": "$state",
                    "market_name": "$market_name",
                    "commodity_name": "$commodity_name",
                    "variety": "$variety",
                },
                "docs": {
                    "$push": {
                        "id": "$_id",
                        "src": "$source_system",
                        "market_id": "$market_id",
                        "cal": "$commodity_alias_lookup_id",
                    }
                },
                "sources": {"$addToSet": "$source_system"},
            }
        },
    ]
    rename_ids: list = []
    collide_pairs: list[dict] = []
    for g in master_coll.aggregate(pipeline, allowDiskUse=True):
        by_src: dict[str, list] = {}
        for d in g["docs"]:
            by_src.setdefault(d["src"], []).append(d)
        if "agmarknet2" not in by_src:
            continue
        if "agmarknet" in by_src:
            collide_pairs.append(
                {
                    "key": g["_id"],
                    "old": by_src["agmarknet2"][0],
                    "keep": by_src["agmarknet"][0],
                }
            )
        else:
            rename_ids.extend(d["id"] for d in by_src["agmarknet2"])

    _log(f"  Safe to rename          : {len(rename_ids)}")
    _log(f"  Still colliding (merge) : {len(collide_pairs)}")

    renamed = 0
    if rename_ids:
        if dry_run:
            renamed = len(rename_ids)
            _log(f"  [DRY-RUN] Would rename {renamed} docs → agmarknet")
        else:
            for i in range(0, len(rename_ids), 2000):
                res = master_coll.update_many(
                    {"_id": {"$in": rename_ids[i : i + 2000]}},
                    {"$set": {"source_system": "agmarknet"}},
                )
                renamed += res.modified_count
            _log(f"  Renamed {renamed} agmarknet2-only masters → agmarknet")

    return {
        "rename_ids": len(rename_ids),
        "renamed": renamed,
        "collide_pairs": collide_pairs,
    }


def merge_agmarknet_into_agmarknet2(
    master_coll,
    price_coll,
    *,
    dry_run: bool = True,
    rename_unique: bool = True,
    max_passes: int = 5,
) -> dict:
    """
    Normalize source_system to `agmarknet` only.

    For each overlapping (state, market, commodity, variety):
      1. Remap price_records from agmarknet2 master → agmarknet master.
      2. On date collision, keep the agmarknet price and drop the agmarknet2 one.
      3. Delete the agmarknet2 master doc.

    Optionally rename remaining agmarknet2-only masters to source_system=agmarknet.
    """
    if price_coll is None:
        raise SystemExit("[ERROR] PRICE_COLLECTION is not configured.")

    _print_header(
        f"MERGE agmarknet2 → agmarknet  ({'DRY-RUN' if dry_run else 'LIVE'})"
    )

    totals = {
        "dry_run": dry_run,
        "overlap_pairs": 0,
        "prices_remapped": 0,
        "prices_dropped_collision": 0,
        "collision_pairs": 0,
        "masters_deleted": 0,
        "market_id_backfilled": 0,
        "masters_renamed_to_agmarknet": 0,
        "orphan_prices_deleted": 0,
        "passes": 0,
        "errors": [],
    }

    for pass_no in range(1, max_passes + 1):
        pairs = _find_agmarknet_overlap_pairs(master_coll)
        _log(f"\n  Pass {pass_no}/{max_passes}: overlap pairs = {len(pairs)}")
        if not pairs:
            break

        totals["passes"] = pass_no
        totals["overlap_pairs"] += len(pairs)
        pass_stats = _merge_overlap_pairs(
            master_coll, price_coll, pairs, dry_run=dry_run
        )
        for k in (
            "prices_remapped",
            "prices_dropped_collision",
            "collision_pairs",
            "masters_deleted",
            "market_id_backfilled",
        ):
            totals[k] += pass_stats[k]
        totals["errors"].extend(pass_stats["errors"])

        if dry_run:
            break

    orphan_stats = cleanup_orphan_prices(master_coll, price_coll, dry_run=dry_run)
    totals["orphan_prices_deleted"] = orphan_stats["price_rows_deleted"]

    if rename_unique:
        rename_stats = _rename_agmarknet2_only(
            master_coll, price_coll, dry_run=dry_run
        )
        totals["masters_renamed_to_agmarknet"] = rename_stats["renamed"]
        if rename_stats["collide_pairs"]:
            _log(
                f"  Merging {len(rename_stats['collide_pairs'])} rename-collision "
                f"pairs…"
            )
            if not dry_run:
                pass_stats = _merge_overlap_pairs(
                    master_coll,
                    price_coll,
                    rename_stats["collide_pairs"],
                    dry_run=False,
                )
                for k in (
                    "prices_remapped",
                    "prices_dropped_collision",
                    "collision_pairs",
                    "masters_deleted",
                    "market_id_backfilled",
                ):
                    totals[k] += pass_stats[k]
                totals["errors"].extend(pass_stats["errors"])
                rename_stats2 = _rename_agmarknet2_only(
                    master_coll, price_coll, dry_run=False
                )
                totals["masters_renamed_to_agmarknet"] += rename_stats2["renamed"]
            else:
                totals["overlap_pairs"] += len(rename_stats["collide_pairs"])

    left = master_coll.count_documents({"source_system": "agmarknet2"})
    totals["agmarknet2_remaining"] = left
    _log("\n  Merge result:")
    for k, v in totals.items():
        if k != "errors":
            _log(f"    {k}: {v}")
    if totals["errors"]:
        _log(f"    errors ({len(totals['errors'])}):")
        for e in totals["errors"][:10]:
            _log(f"      - {e}")
    if left and not dry_run:
        _log(
            f"\n  [WARN] {left} agmarknet2 docs still remain. "
            f"Re-run: python resolving.py --merge --execute --skip-diagnose"
        )
    return totals


def _merge_overlap_pairs(
    master_coll,
    price_coll,
    pairs: list[dict],
    *,
    dry_run: bool,
) -> dict:
    prices_remapped = 0
    prices_dropped_collision = 0
    masters_deleted = 0
    collision_pairs = 0
    mid_filled = 0
    errors: list[str] = []

    BATCH = 500
    for start in range(0, len(pairs), BATCH):
        chunk = pairs[start : start + BATCH]
        # Skip pairs whose old doc was already deleted
        old_ids = []
        keep_by_old = {}
        existing_old = {
            d["_id"]: d
            for d in master_coll.find(
                {"_id": {"$in": [p["old"]["id"] for p in chunk]}},
                {"_id": 1},
            )
        }
        for p in chunk:
            if p["old"]["id"] not in existing_old:
                continue
            old_ids.append(p["old"]["id"])
            keep_by_old[p["old"]["id"]] = p["keep"]

        if not old_ids:
            continue

        keep_ids = list({keep_by_old[oid]["id"] for oid in old_ids})
        keep_dates: dict[Any, set] = {kid: set() for kid in keep_ids}
        for row in price_coll.find(
            {"market_commodity_id": {"$in": keep_ids}},
            {"market_commodity_id": 1, "date": 1},
        ):
            keep_dates[row["market_commodity_id"]].add(row["date"])

        to_drop_ids: list = []
        to_remap: dict[Any, tuple[dict, list]] = {}
        pairs_with_collision: set = set()

        for row in price_coll.find(
            {"market_commodity_id": {"$in": old_ids}},
            {"_id": 1, "market_commodity_id": 1, "date": 1},
        ):
            old_id = row["market_commodity_id"]
            keep = keep_by_old[old_id]
            keep_id = keep["id"]
            if row.get("date") in keep_dates[keep_id]:
                to_drop_ids.append(row["_id"])
                pairs_with_collision.add(old_id)
            else:
                if keep_id not in to_remap:
                    fields = {"market_commodity_id": keep_id}
                    if keep.get("market_id") is not None:
                        fields["market_id"] = keep["market_id"]
                    if keep.get("cal") is not None:
                        fields["commodity_alias_lookup_id"] = keep["cal"]
                    to_remap[keep_id] = (fields, [])
                to_remap[keep_id][1].append(row["_id"])
                keep_dates[keep_id].add(row.get("date"))

        collision_pairs += len(pairs_with_collision)

        if dry_run:
            prices_dropped_collision += len(to_drop_ids)
            prices_remapped += sum(len(ids) for _, ids in to_remap.values())
            masters_deleted += len(old_ids)
            for oid in old_ids:
                keep = keep_by_old[oid]
                old = next(p["old"] for p in chunk if p["old"]["id"] == oid)
                if keep.get("market_id") is None and old.get("market_id") is not None:
                    mid_filled += 1
        else:
            if to_drop_ids:
                res = price_coll.delete_many({"_id": {"$in": to_drop_ids}})
                prices_dropped_collision += res.deleted_count

            for keep_id, (fields, price_ids) in to_remap.items():
                for i in range(0, len(price_ids), 2000):
                    part = price_ids[i : i + 2000]
                    try:
                        res = price_coll.update_many(
                            {"_id": {"$in": part}},
                            {"$set": fields},
                        )
                        prices_remapped += res.modified_count
                    except (DuplicateKeyError, BulkWriteError):
                        for pid in part:
                            try:
                                price_coll.update_one({"_id": pid}, {"$set": fields})
                                prices_remapped += 1
                            except DuplicateKeyError:
                                price_coll.delete_one({"_id": pid})
                                prices_dropped_collision += 1

            fill_ops = []
            for oid in old_ids:
                keep = keep_by_old[oid]
                old = next(p["old"] for p in chunk if p["old"]["id"] == oid)
                if keep.get("market_id") is None and old.get("market_id") is not None:
                    fill_ops.append(
                        UpdateOne(
                            {"_id": keep["id"], "market_id": None},
                            {"$set": {"market_id": old["market_id"]}},
                        )
                    )
            if fill_ops:
                try:
                    res = master_coll.bulk_write(fill_ops, ordered=False)
                    mid_filled += res.modified_count
                except BulkWriteError as exc:
                    errors.append(f"market_id backfill: {exc.details}")

            # Remap any leftover prices still pointing at old ids, then delete
            leftover = list(
                price_coll.find(
                    {"market_commodity_id": {"$in": old_ids}},
                    {"_id": 1, "market_commodity_id": 1, "date": 1},
                )
            )
            for row in leftover:
                keep = keep_by_old[row["market_commodity_id"]]
                keep_id = keep["id"]
                fields = {"market_commodity_id": keep_id}
                if keep.get("market_id") is not None:
                    fields["market_id"] = keep["market_id"]
                if keep.get("cal") is not None:
                    fields["commodity_alias_lookup_id"] = keep["cal"]
                try:
                    price_coll.update_one({"_id": row["_id"]}, {"$set": fields})
                    prices_remapped += 1
                except DuplicateKeyError:
                    price_coll.delete_one({"_id": row["_id"]})
                    prices_dropped_collision += 1

            res = master_coll.delete_many({"_id": {"$in": old_ids}})
            masters_deleted += res.deleted_count

        done = min(start + len(chunk), len(pairs))
        _log(
            f"  … {done}/{len(pairs)} pairs "
            f"(remapped={prices_remapped}, dropped={prices_dropped_collision}, "
            f"masters_deleted={masters_deleted})"
        )

    return {
        "prices_remapped": prices_remapped,
        "prices_dropped_collision": prices_dropped_collision,
        "collision_pairs": collision_pairs,
        "masters_deleted": masters_deleted,
        "market_id_backfilled": mid_filled,
        "errors": errors,
    }


def run(
    samples: int = 5,
    json_path: str | None = None,
    *,
    diagnose: bool = True,
    merge: bool = False,
    dry_run: bool = True,
    rename_unique: bool = True,
) -> dict:
    client, master_coll, price_coll = connect()
    print(
        f"[INFO] Connected. db={DB_NAME!r} "
        f"collection={MASTER_COLLECTION!r}"
    )

    report: dict[str, Any] = {
        "db": DB_NAME,
        "collection": MASTER_COLLECTION,
    }

    if diagnose:
        report.update(
            {
                "indexes": check_indexes(master_coll),
                "exact_master_key": check_exact_master_key_dups(
                    master_coll, samples
                ),
                "cross_source": check_cross_source_dups(master_coll, samples),
                "market_name_variants": check_market_id_name_variants(
                    master_coll, samples
                ),
                "commodity_alias_collisions": check_commodity_alias_collisions(
                    master_coll, samples
                ),
                "lookup_key_dups": check_market_id_commodity_alias_dups(
                    master_coll, samples
                ),
                "data_quality": check_data_quality(master_coll),
                "price_orphans": check_price_orphan_refs(
                    master_coll, price_coll, samples
                ),
            }
        )
        summarise(report)

    if merge:
        report["merge"] = merge_agmarknet_into_agmarknet2(
            master_coll,
            price_coll,
            dry_run=dry_run,
            rename_unique=rename_unique,
        )
        if not dry_run and diagnose:
            print("\n[INFO] Re-checking cross-source overlaps after merge…")
            report["cross_source_after"] = check_cross_source_dups(
                master_coll, samples=3
            )

    if json_path:
        path = Path(json_path)
        path.write_text(json.dumps(report, indent=2, default=str))
        print(f"\n[INFO] Wrote JSON report → {path}")

    client.close()
    print("\n[INFO] Connection closed.")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnose / merge duplicate problems in markets_commodities"
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=5,
        help="How many example groups to print per check (default: 5)",
    )
    parser.add_argument(
        "--json",
        dest="json_path",
        default=None,
        help="Optional path to write the full report as JSON",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge leftover agmarknet2 into agmarknet (canonical label)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually write changes (without this, --merge is a dry-run)",
    )
    parser.add_argument(
        "--no-rename-unique",
        action="store_true",
        help="Do not rename remaining agmarknet2-only masters to agmarknet",
    )
    parser.add_argument(
        "--skip-diagnose",
        action="store_true",
        help="Skip diagnostic checks (useful with --merge --execute)",
    )
    args = parser.parse_args()
    run(
        samples=args.samples,
        json_path=args.json_path,
        diagnose=not args.skip_diagnose,
        merge=args.merge,
        dry_run=not args.execute,
        rename_unique=not args.no_rename_unique,
    )


if __name__ == "__main__":
    main()
