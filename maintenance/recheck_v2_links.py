"""
recheck_v2_links.py — re-run the "looks wrong" check on links nobody has reviewed yet.

    python maintenance/recheck_v2_links.py --target Price --prefix v2_            # report only
    python maintenance/recheck_v2_links.py --target Price --prefix v2_ --apply    # write

Looks only at links approved automatically and never checked by a person:
  crop_names  decided_by "migration", needs_check true
  crop_words  decided_by "seed",      needs_check true, sourced only from v1 aliases

Any of them that now looks wrong (e.g. after the check was tightened so that
"chana" no longer passes for "chafa") goes back to pending, keeping the old
answer as a suggestion. For crop names, the affected listings lose their
product straight away, so farmers see no price instead of a wrong one.
Every change is written to <prefix>audit_log. Decisions made by people are never touched.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "maintenance"))
load_dotenv(ROOT / ".env")

from name_checks import looks_wrong  # noqa: E402


def _same_text(text: str) -> dict:
    parts = [re.escape(p) for p in str(text).split()]
    return {"$regex": r"^\s*" + r"\s+".join(parts) + r"\s*$", "$options": "i"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True, help="database holding the v2 collections")
    ap.add_argument("--prefix", default="")
    ap.add_argument("--source", default="Price", help="v1 database, for the v1 alias lists (read only)")
    ap.add_argument("--uri", default=os.getenv("MONGO_URI"))
    ap.add_argument("--apply", action="store_true", help="write the changes (default: report only)")
    args = ap.parse_args()
    if not args.uri:
        raise SystemExit("Set MONGO_URI (or pass --uri).")
    client = MongoClient(args.uri, serverSelectionTimeoutMS=15000)
    db, v1 = client[args.target], client[args.source]
    P = args.prefix
    now = datetime.now(timezone.utc)

    crops = {c["_id"]: c for c in db[f"{P}crops"].find({}, {"name": 1, "legacy_group_ids": 1, "default_product_id": 1})}
    products = {p["_id"]: p for p in db[f"{P}products"].find({}, {"name": 1, "crop_id": 1})}
    aliases_of_group = {g["_id"]: [a for a in (g.get("aliases") or []) if isinstance(a, str)]
                        for g in v1.commodity_alias_lookup.find({}, {"aliases": 1})}

    def aliases_for(crop) -> list[str]:
        return [a for gid in (crop or {}).get("legacy_group_ids") or [] for a in aliases_of_group.get(gid, [])]

    # ── crop names approved by the migration ─────────────────────────────
    to_pending = []
    for row in db[f"{P}crop_names"].find({"status": "approved", "decided_by": "migration", "needs_check": True}):
        product = products.get(row.get("product_id"))
        crop = crops.get((product or {}).get("crop_id"))
        if product and looks_wrong(row["raw_name"], (crop or {}).get("name") or product["name"], aliases_for(crop)):
            to_pending.append((row, product))

    # ── farmer words approved by the seed from v1 aliases only ──────────
    words_to_pending = []
    for w in db[f"{P}crop_words"].find({"status": "approved", "decided_by": "seed", "needs_check": True}):
        if any(not s.startswith("v1_alias") for s in w.get("sources") or []):
            continue  # crop_master names are curated local names; spelling cannot judge them
        crop = crops.get(w.get("crop_id"))
        if crop and looks_wrong(w["word"], crop["name"], aliases_for(crop)):
            words_to_pending.append((w, crop))

    print(f"Crop names approved by the migration that now look wrong: {len(to_pending)}")
    for row, product in sorted(to_pending, key=lambda x: -(x[0].get("listings") or 0)):
        print(f"  {row['raw_name']!r:40} ({row['source']}, {row.get('listings', 0)} listings) was → {product['name']}")
    print(f"Farmer words approved by the seed that now look wrong: {len(words_to_pending)}")
    for w, crop in words_to_pending:
        print(f"  {w['word']!r:40} was → {crop['name']}")

    if not args.apply:
        print("\nReport only. Re-run with --apply to send these back to pending.")
        return

    listings_cleared = 0
    for row, product in to_pending:
        reason = f"looks unrelated to {product['name']} (re-check {now:%Y-%m-%d})"
        db[f"{P}crop_names"].update_one({"_id": row["_id"], "status": "approved", "decided_by": "migration"}, {"$set": {
            "status": "pending", "product_id": None, "needs_check": False, "decided_by": None, "decided_at": None,
            "suggestion": {"product_id": product["_id"], "product_name": product["name"], "reason": reason},
        }})
        f = {"source_system": _same_text(row["source"]), "commodity_name": _same_text(row["raw_name"])}
        if row.get("raw_variety") not in (None, "*"):
            f["variety"] = _same_text(row["raw_variety"])
        listings_cleared += db[f"{P}listings"].update_many({**f, "product_id": product["_id"]}, {"$set": {"product_id": None}}).modified_count
    for w, crop in words_to_pending:
        db[f"{P}crop_words"].update_one({"_id": w["_id"], "status": "approved", "decided_by": "seed"}, {"$set": {
            "status": "pending", "needs_check": False, "decided_by": None, "decided_at": None,
            "suggestion": {"reason": f"looks unrelated to {crop['name']}", "candidates": [{"crop_id": crop["_id"], "crop_name": crop["name"], "sources": w.get("sources") or []}]},
        }})
    db[f"{P}audit_log"].insert_one({
        "action": "recheck.looks_wrong", "user": "recheck_v2_links.py", "role": "script", "at": now,
        "summary": f"Sent {len(to_pending)} crop names and {len(words_to_pending)} farmer words back to pending",
        "target": {"crop_names": [r["_id"] for r, _ in to_pending], "crop_words": [w["_id"] for w, _ in words_to_pending]},
        "counts": {"crop_names": len(to_pending), "crop_words": len(words_to_pending), "listings": listings_cleared},
    })
    print(f"\n[DONE] {len(to_pending)} crop names and {len(words_to_pending)} farmer words back to pending; "
          f"{listings_cleared} listings no longer show a price until reviewed.")


if __name__ == "__main__":
    main()
