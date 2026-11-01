"""
migrate_v2.py — build the v2 database from the v1 database. v1 is only read.

    python maintenance/migrate_v2.py --source Price --target PriceV2
    python maintenance/migrate_v2.py --source Price --target PriceV2 --replace   # rebuild

    # v2 next to v1 in the SAME database, every v2 collection prefixed "v2_":
    python maintenance/migrate_v2.py --source Price --target Price --prefix v2_
    In this mode available_mandi is not copied: v2 reads it in place and never
    changes it, and --replace drops only the v2_ collections.

Reads from v1 (never writes to it):
    markets_commodities, price_records, available_mandi, commodity_alias_lookup

Writes the v2 database:
    available_mandi   copy, schema unchanged
    price_records     copy, without the copied commodity_alias_lookup_id / market_id
    listings          copy of markets_commodities with the same _id, plus state_code,
                      product_id, mandi_id; the old ids are kept under `legacy`
    states            one per state, with every spelling seen
    crops, products   one crop and one default product per v1 crop group
    crop_names        one per (source, crop name)
    mandi_names       one per (source, state, mandi name)
    ingestion_runs, review_items, audit_log   empty, indexed

Which links start as approved
    The v1 matches were never checked by a person, so none can be trusted
    outright. A link starts "approved" (decided_by "migration", needs_check
    true) only when it is unambiguous and plausible:
      crop  — the name is an exact alias of exactly one v1 group, the listings
              agree with that group, and the name does not look unrelated to it;
      mandi — the listings' mandi has this exact name (or the same name with or
              without " apmc") in the same state.
    Everything else starts "pending", with the v1 answer kept as a suggestion,
    so farmers see no price for it until a reviewer approves it.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, UpdateMany

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "maintenance"))
load_dotenv(ROOT / ".env")

# catalog.py and review_log.py read their collection names at import time.
if "--prefix" in sys.argv[:-1]:
    os.environ["V2_COLLECTION_PREFIX"] = sys.argv[sys.argv.index("--prefix") + 1]

from catalog import ANY_VARIETY, ensure_catalog_indexes, norm  # noqa: E402
from name_checks import looks_wrong  # noqa: E402
from seed_crop_words import seed_crop_words  # noqa: E402
import review_log  # noqa: E402

# Spellings of one state, canonical name first. Codes are slugs of the canonical name.
STATE_SPELLINGS = [
    ("keralam", "kerala"),
    ("chhattisgarh", "chattisgarh"),
    ("delhi", "nct of delhi", "nct delhi"),
    ("puducherry", "pondicherry"),
    ("odisha", "orissa"),
    ("uttarakhand", "uttaranchal"),
    ("jammu and kashmir", "jammu & kashmir"),
    ("andaman and nicobar islands", "andaman and nicobar", "andaman & nicobar", "andaman & nicobar islands"),
    ("dadra and nagar haveli and daman and diu", "dadra and nagar haveli", "daman and diu"),
]

MANDI_SUFFIX = re.compile(r"\s*(apmc|mandi|market|sub yard|yard)\b")


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", norm(text)).strip("-")


def copy_indexes(src, dst, skip_keys: set[str] = frozenset()) -> None:
    for name, info in src.index_information().items():
        if name == "_id_":
            continue
        keys = info["key"]
        if any(k in skip_keys for k, _ in keys):
            continue
        opts = {k: v for k, v in info.items() if k in ("unique", "sparse", "2dsphereIndexVersion", "partialFilterExpression", "expireAfterSeconds")}
        dst.create_index(keys, name=name, **opts)


def mandi_name_variants(raw: str) -> set[str]:
    k = norm(raw)
    stripped = MANDI_SUFFIX.sub("", k).strip()
    return {v for v in (k, k + " apmc", stripped, stripped + " apmc") if v}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=os.getenv("MANDI_DB_NAME") or "Price")
    ap.add_argument("--target", required=True)
    ap.add_argument("--replace", action="store_true", help="drop the target (or, with --prefix, only its v2 collections) first")
    ap.add_argument("--prefix", default="", help="put v2 next to v1, e.g. v2_ (allows --target == --source)")
    ap.add_argument("--uri", default=os.getenv("MONGO_URI"))
    ap.add_argument("--crop-master-db", default="agriai", help="database with crop_master (farmer words); '' to skip")
    args = ap.parse_args()
    if not args.uri:
        raise SystemExit("Set MONGO_URI (or pass --uri).")
    prefix = args.prefix.strip()
    same_db = args.source == args.target
    if same_db and not prefix:
        raise SystemExit("Source and target are the same database: pass --prefix (e.g. v2_) so v1 collections are never touched.")

    def name(base: str) -> str:
        return f"{prefix}{base}"

    V1 = {"markets_commodities", "price_records", "available_mandi", "commodity_alias_lookup"}
    C = {b: name(b) for b in ("listings", "price_records", "states", "crops", "products", "crop_names",
                              "mandi_names", "crop_words", "ingestion_runs", "review_items", "audit_log", "migration_info")}
    if same_db and V1 & set(C.values()):
        raise SystemExit(f"Prefix {prefix!r} would reuse a v1 collection name.")

    client = MongoClient(args.uri, serverSelectionTimeoutMS=15000)
    src, dst = client[args.source], client[args.target]
    mandi_coll_name = "available_mandi" if same_db else name("available_mandi")
    existing = [c for c in dst.list_collection_names() if not same_db or c in C.values()]
    if existing and same_db:
        has_data = [c for c in existing if dst[c].estimated_document_count() > 0]
        if has_data and not args.replace:
            raise SystemExit(f"v2 collections already have data: {has_data}. Use --replace to rebuild them.")
        for c in existing:
            assert c.startswith(prefix) and c not in V1
            print(f"[INFO] Dropping v2 collection {c!r}")
            dst.drop_collection(c)
    elif existing:
        has_data = [c for c in existing if dst[c].estimated_document_count() > 0]
        if args.replace:
            print(f"[INFO] Dropping target database {args.target!r}")
            client.drop_database(args.target)
        elif has_data:
            raise SystemExit(f"Target database {args.target!r} already has data in {has_data}. Use --replace to rebuild it.")
        else:
            # e.g. a database created in the Atlas UI, which requires a first (empty) collection
            print(f"[INFO] Target {args.target!r} has only empty collections {existing}; using it.")
    now = datetime.now(timezone.utc)

    # ── 1. Server-side copies (v1 is only read) ──────────────────────────
    print("[1/8] Copying price_records and listings" + ("" if same_db else " and available_mandi") + " …")
    if not same_db:
        src.available_mandi.aggregate([{"$match": {}}, {"$out": {"db": args.target, "coll": mandi_coll_name}}], allowDiskUse=True)
        copy_indexes(src.available_mandi, dst[mandi_coll_name])
    mandi_coll = dst[mandi_coll_name]
    src.price_records.aggregate(
        [{"$unset": ["commodity_alias_lookup_id", "market_id"]}, {"$out": {"db": args.target, "coll": C['price_records']}}],
        allowDiskUse=True,
    )
    copy_indexes(src.price_records, dst[C['price_records']], skip_keys={"commodity_alias_lookup_id", "market_id"})
    src.markets_commodities.aggregate(
        [
            {"$set": {"legacy": {"commodity_alias_lookup_id": "$commodity_alias_lookup_id", "market_id": "$market_id"},
                      "state_code": None, "product_id": None, "mandi_id": None}},
            {"$unset": ["commodity_alias_lookup_id", "market_id"]},
            {"$out": {"db": args.target, "coll": C['listings']}},
        ],
        allowDiskUse=True,
    )
    copy_indexes(src.markets_commodities, dst[C['listings']], skip_keys={"commodity_alias_lookup_id", "market_id"})
    dst[C['listings']].create_index([("product_id", ASCENDING), ("mandi_id", ASCENDING)], name="idx_listing_product_mandi")
    dst[C['listings']].create_index([("mandi_id", ASCENDING)], name="idx_listing_mandi")
    dst[C['listings']].create_index([("state_code", ASCENDING)], name="idx_listing_state_code")
    print(f"      listings {dst[C['listings']].estimated_document_count()}, prices {dst[C['price_records']].estimated_document_count()}, "
          f"mandis {mandi_coll.estimated_document_count()}")

    # ── 2. States ────────────────────────────────────────────────────────
    print("[2/8] Building states …")
    seen = {norm(s) for s in dst[C['listings']].distinct("state") + mandi_coll.distinct("state") if s}
    states: dict[str, dict] = {}
    for group in STATE_SPELLINGS:
        states[slug(group[0])] = {"_id": slug(group[0]), "name": group[0], "spellings": sorted(set(group)), "lgd_code": None}
    for spelling in seen:
        if not any(spelling in s["spellings"] for s in states.values()):
            states.setdefault(slug(spelling), {"_id": slug(spelling), "name": spelling, "spellings": [], "lgd_code": None})
            states[slug(spelling)]["spellings"] = sorted(set(states[slug(spelling)]["spellings"]) | {spelling})
    dst[C['states']].insert_many(list(states.values()))
    dst[C['states']].create_index([("spellings", ASCENDING)], name="spellings")
    code_of = {sp: s["_id"] for s in states.values() for sp in s["spellings"]}
    print(f"      {len(states)} states from {len(seen)} spellings")

    # ── 3. Crops and products from v1 groups ─────────────────────────────
    print("[3/8] Building crops and products …")
    groups = list(src.commodity_alias_lookup.find({}))
    by_name: dict[str, list[dict]] = defaultdict(list)
    for g in groups:
        by_name[norm(g.get("canonical_name")) or f"group {g['_id']}"].append(g)
    product_of_group: dict = {}
    crop_info: dict = {}  # product_id -> (canonical, aliases)
    for name, gs in sorted(by_name.items()):
        crop = {"name": name, "category": None,
                "crop_master_id": next((g.get("crop_master_id") for g in gs if g.get("crop_master_id")), None),
                "default_product_id": None, "active": any(g.get("active") is not False for g in gs),
                "legacy_group_ids": [g["_id"] for g in gs], "created_at": now, "updated_at": now}
        crop_id = dst[C['crops']].insert_one(crop).inserted_id
        product = {"crop_id": crop_id, "name": name, "form": "raw", "price_unit": "Rs/quintal", "active": crop["active"],
                   "legacy_group_ids": crop["legacy_group_ids"], "created_at": now, "updated_at": now}
        product_id = dst[C['products']].insert_one(product).inserted_id
        dst[C['crops']].update_one({"_id": crop_id}, {"$set": {"default_product_id": product_id}})
        aliases = sorted({norm(a) for g in gs for a in (g.get("aliases") or []) if norm(a)})
        crop_info[product_id] = (name, aliases)
        for g in gs:
            product_of_group[g["_id"]] = product_id
    dst[C['crops']].create_index([("name", ASCENDING)], unique=True, name="unique_crop_name")
    dst[C['products']].create_index([("crop_id", ASCENDING), ("name", ASCENDING)], unique=True, name="unique_product_name")
    print(f"      {dst[C['crops']].count_documents({})} crops, {dst[C['products']].count_documents({})} products from {len(groups)} groups")

    # ── 4. Crop name links ───────────────────────────────────────────────
    print("[4/8] Building crop_names …")
    holders: dict[str, set] = defaultdict(set)  # alias -> product ids holding it
    for g in groups:
        for a in g.get("aliases") or []:
            if norm(a):
                holders[norm(a)].add(product_of_group[g["_id"]])
    rows = src.markets_commodities.aggregate([
        {"$group": {"_id": {"source": "$source_system", "name": "$commodity_name", "gid": "$commodity_alias_lookup_id"}, "n": {"$sum": 1}}},
    ], allowDiskUse=True)
    per_name: dict[tuple[str, str], Counter] = defaultdict(Counter)
    raw_crop: dict[tuple[str, str], set] = defaultdict(set)  # normalised key -> (source, name) as stored
    for r in rows:
        source, name = norm(r["_id"].get("source")), norm(r["_id"].get("name"))
        if source and name:
            per_name[(source, name)][product_of_group.get(r["_id"].get("gid"))] += r["n"]
            raw_crop[(source, name)].add((r["_id"].get("source"), r["_id"].get("name")))
    crop_docs, approved_crop = [], Counter()
    for (source, name), counts in per_name.items():
        listings = sum(counts.values())
        used = [p for p, _ in counts.most_common() if p is not None]
        held = holders.get(name, set())
        suggestion_pid = used[0] if used else (next(iter(held)) if len(held) == 1 else None)
        reason, ok = None, False
        if len(held) > 1:
            reason = f"name is listed in {len(held)} v1 groups"
        elif not held:
            reason = "v1 matched it only by a partial-name guess" if used else "v1 had no group for it"
        elif used and set(used) != held:
            reason = "listings point to a different v1 group than the one listing the name"
        elif looks_wrong(name, *crop_info[next(iter(held))]):
            reason = f"name does not look like {crop_info[next(iter(held))][0]}"
        else:
            ok = True
        doc = {"source": source, "raw_name": name, "raw_variety": ANY_VARIETY,
               "product_id": next(iter(held)) if ok else None,
               "status": "approved" if ok else "pending",
               "suggestion": None if ok else ({"product_id": suggestion_pid, "product_name": crop_info[suggestion_pid][0], "reason": reason}
                                              if suggestion_pid else {"product_id": None, "product_name": None, "reason": reason}),
               "decided_by": "migration" if ok else None, "decided_at": now if ok else None,
               "needs_check": ok, "notes": None, "listings": listings, "first_seen": now, "last_seen": now}
        crop_docs.append(doc)
        approved_crop["approved" if ok else "pending"] += 1
    ensure_catalog_indexes(dst)
    if crop_docs:
        dst[C['crop_names']].insert_many(crop_docs)
    print(f"      {len(crop_docs)} crop names: {approved_crop['approved']} approved by migration, {approved_crop['pending']} pending")

    # ── 5. Mandi name links ──────────────────────────────────────────────
    print("[5/8] Building mandi_names …")
    mandis = {m["_id"]: m for m in mandi_coll.find({}, {"name": 1, "aliases": 1, "state": 1})}
    rows = src.markets_commodities.aggregate([
        {"$group": {"_id": {"source": "$source_system", "state": "$state", "name": "$market_name", "mid": "$market_id"}, "n": {"$sum": 1}}},
    ], allowDiskUse=True)
    per_mandi: dict[tuple[str, str, str], Counter] = defaultdict(Counter)
    raw_mandi: dict[tuple[str, str, str], set] = defaultdict(set)
    for r in rows:
        source, state, name = norm(r["_id"].get("source")), norm(r["_id"].get("state")), norm(r["_id"].get("name"))
        if source and name:
            key = (source, code_of.get(state) or "", name)
            per_mandi[key][r["_id"].get("mid")] += r["n"]
            raw_mandi[key].add((r["_id"].get("source"), r["_id"].get("state"), r["_id"].get("name")))
    mandi_docs, approved_mandi = [], Counter()
    for (source, state_code, name), counts in per_mandi.items():
        used = [m for m, _ in counts.most_common() if m is not None]
        mid = used[0] if used else None
        m = mandis.get(mid) if mid else None
        ok, reason = False, None
        if not m:
            reason = "v1 had no mandi for it" if not mid else "v1 mandi no longer exists"
        else:
            known = {norm(m.get("name"))} | {norm(a) for a in (m.get("aliases") or [])}
            same_state = code_of.get(norm(m.get("state"))) == (state_code or None)
            if len(used) > 1:
                reason = "listings point to more than one mandi"
            elif not same_state:
                reason = "v1 mandi is in another state"
            elif not (mandi_name_variants(name) & known):
                reason = "v1 matched it only by a partial-name guess"
            else:
                ok = True
        mandi_docs.append({
            "source": source, "state_code": state_code or None, "raw_name": name,
            "mandi_id": mid if ok else None, "status": "approved" if ok else "pending",
            "suggestion": None if ok else {"mandi_id": mid, "mandi_name": m.get("name") if m else None, "reason": reason},
            "decided_by": "migration" if ok else None, "decided_at": now if ok else None, "needs_check": ok,
            "notes": None, "listings": sum(counts.values()), "first_seen": now, "last_seen": now,
        })
        approved_mandi["approved" if ok else "pending"] += 1
    if mandi_docs:
        dst[C['mandi_names']].insert_many(mandi_docs)
    print(f"      {len(mandi_docs)} mandi names: {approved_mandi['approved']} approved by migration, {approved_mandi['pending']} pending")

    # ── 6. Fill listings from the links ──────────────────────────────────
    print("[6/8] Filling listings.state_code / product_id / mandi_id …")
    ops = [UpdateMany({"state": raw}, {"$set": {"state_code": code_of.get(norm(raw))}}) for raw in dst[C['listings']].distinct("state") if raw]
    for d in crop_docs:
        if d["status"] == "approved":
            for raw_source, raw_name in raw_crop[(d["source"], d["raw_name"])]:
                ops.append(UpdateMany({"source_system": raw_source, "commodity_name": raw_name}, {"$set": {"product_id": d["product_id"]}}))
    if ops:
        dst[C['listings']].bulk_write(ops, ordered=False)
    ops = [UpdateMany({"source_system": rs, "state": rst, "market_name": rn}, {"$set": {"mandi_id": d["mandi_id"]}})
           for d in mandi_docs if d["status"] == "approved"
           for rs, rst, rn in raw_mandi[(d["source"], d["state_code"] or "", d["raw_name"])]]
    if ops:
        dst[C['listings']].bulk_write(ops, ordered=False)

    # ── 7. Operational collections ───────────────────────────────────────
    print("[7/8] Creating ingestion_runs, review_items, audit_log …")
    review_log.ensure_indexes(dst)
    dst[C['audit_log']].create_index([("at", -1)], name="at_desc")
    print("[8/8] Seeding farmer words (crop_words) …")
    try:
        cm = client[args.crop_master_db]["crop_master"] if args.crop_master_db else None
        words = seed_crop_words(src, dst, {"crops": C["crops"], "products": C["products"], "crop_words": C["crop_words"]}, cm, now)
        print(f"      {words['approved']} words approved by seed, {words['pending']} pending")
    except Exception as exc:  # crop_master unreadable: seed from v1 aliases only
        print(f"[WARN] crop_master not used ({exc}); seeding from v1 aliases only")
        words = seed_crop_words(src, dst, {"crops": C["crops"], "products": C["products"], "crop_words": C["crop_words"]}, None, now)
    dst[C['migration_info']].insert_one({"source": args.source, "at": now, "crop_names": dict(approved_crop), "mandi_names": dict(approved_mandi)})

    total = dst[C['listings']].estimated_document_count()
    print("\n[DONE]")
    print(f"  listings with a product : {dst[C['listings']].count_documents({'product_id': {'$ne': None}})} / {total}")
    print(f"  listings with a mandi   : {dst[C['listings']].count_documents({'mandi_id': {'$ne': None}})} / {total}")
    print(f"  v1 collections in {args.source!r} were only read.")
    client.close()


if __name__ == "__main__":
    main()
