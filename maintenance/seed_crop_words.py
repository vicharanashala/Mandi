"""
seed_crop_words.py — fill the farmer word list (v2) from existing word lists.

    python maintenance/seed_crop_words.py --source Price --target Price --prefix v2_
    python maintenance/seed_crop_words.py --source Price --target PriceV2

The farmer's question reaches the price tool already translated into English,
so only English words are kept: other names and spellings for a crop that are
not the crop or product name itself ("dry chillies", "blackgram"). This
builds <prefix>crop_words: one row per word key, pointing at one crop (the price
tool uses the crop's default product), from the v1 commodity_alias_lookup
aliases (source "v1_alias"), which are only read. Words in an Indian script
are skipped.

A word starts "approved" (decided_by "seed", needs_check true) only when every
source agrees on one crop and the word does not look unrelated to it. A word
claimed by two crops ("rice": paddy and aromatic rice) or that looks wrong
("hen" under groundnut) starts "pending" with the candidates as a suggestion.

Safe to re-run: an existing row only gains sources; its status
and target are never changed, so reviewers' decisions stay.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "maintenance"))
load_dotenv(ROOT / ".env")

from catalog import word_key  # noqa: E402
from name_checks import looks_wrong  # noqa: E402


def seed_crop_words(src, dst, coll: dict[str, str], now: datetime | None = None) -> dict:
    """
    src: v1 database (commodity_alias_lookup). dst: v2 database.
    coll: v2 collection names, keys "crops", "products", "crop_words".
    """
    now = now or datetime.now(timezone.utc)
    crops = list(dst[coll["crops"]].find({}, {"name": 1, "legacy_group_ids": 1}))
    crop_by_group = {gid: c for c in crops for gid in (c.get("legacy_group_ids") or [])}
    crop_by_key = {word_key(c["name"]): c for c in crops}
    crop_name = {c["_id"]: c["name"] for c in crops}
    # Words that already resolve by a crop or product name need no entry.
    name_keys = set(crop_by_key) | {word_key(p["name"]) for p in dst[coll["products"]].find({}, {"name": 1})}

    # key -> {"word", "targets": {crop_id: set(sources)}, "wrong": set(crop_id)}
    cand: dict[str, dict] = defaultdict(lambda: {"word": None, "targets": defaultdict(set), "wrong": set()})

    def add(text: str, crop, source: str, aliases: list[str]) -> None:
        key = word_key(text)
        if not key or len(key) < 2 or key in name_keys or crop is None or not str(text).isascii():
            return
        c = cand[key]
        c["word"] = c["word"] or " ".join(str(text).split()).lower()
        c["targets"][crop["_id"]].add(source)
        # v1 aliases are polluted ("hen" under groundnut).
        if looks_wrong(text, crop["name"], aliases):
            c["wrong"].add(crop["_id"])

    for g in src.commodity_alias_lookup.find({}, {"aliases": 1}):
        crop = crop_by_group.get(g["_id"])
        aliases = [a for a in (g.get("aliases") or []) if isinstance(a, str)]
        for a in aliases:
            add(a, crop, "v1_alias", aliases)

    ops, counts = [], {"approved": 0, "pending": 0}
    for key, c in cand.items():
        targets = c["targets"]
        wrong = c["wrong"]
        ok = len(targets) == 1 and not wrong
        crop_id = next(iter(targets)) if ok else None
        sources = sorted({s for v in targets.values() for s in v})
        if ok:
            suggestion = None
        else:
            reason = (f"claimed by {len(targets)} crops" if len(targets) > 1
                      else f"does not look like {crop_name.get(next(iter(wrong)))}")
            suggestion = {"reason": reason, "candidates": [
                {"crop_id": cid, "crop_name": crop_name.get(cid), "sources": sorted(src_set)} for cid, src_set in targets.items()
            ]}
        counts["approved" if ok else "pending"] += 1
        ops.append(UpdateOne(
            {"key": key},
            {
                "$setOnInsert": {
                    "key": key, "word": c["word"], "crop_id": crop_id, "product_id": None,
                    "status": "approved" if ok else "pending", "suggestion": suggestion,
                    "decided_by": "seed" if ok else None, "decided_at": now if ok else None,
                    "needs_check": ok, "notes": None, "created_at": now,
                },
                "$addToSet": {"sources": {"$each": sources}},
            },
            upsert=True,
        ))
    dst[coll["crop_words"]].create_index("key", unique=True, name="unique_word_key")
    dst[coll["crop_words"]].create_index("status", name="status")
    if ops:
        res = dst[coll["crop_words"]].bulk_write(ops, ordered=False)
        counts["new"] = res.upserted_count
    return counts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="Price")
    ap.add_argument("--target", required=True)
    ap.add_argument("--prefix", default="")
    ap.add_argument("--uri", default=os.getenv("MONGO_URI"))
    args = ap.parse_args()
    if not args.uri:
        raise SystemExit("Set MONGO_URI (or pass --uri).")
    if args.source == args.target and not args.prefix:
        raise SystemExit("Same database for v1 and v2: pass --prefix (e.g. v2_).")
    client = MongoClient(args.uri, serverSelectionTimeoutMS=15000)
    p = args.prefix
    coll = {"crops": f"{p}crops", "products": f"{p}products", "crop_words": f"{p}crop_words"}
    counts = seed_crop_words(client[args.source], client[args.target], coll)
    print(f"[DONE] farmer words: {counts.get('new', 0)} new; of all words seen {counts['approved']} approved by seed, "
          f"{counts['pending']} pending. v1 was only read.")


if __name__ == "__main__":
    main()
