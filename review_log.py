"""
review_log.py
=============
Saves what each scrape run did, and the problems a person should look at,
so the review website can show them.

Two collections (names overridable via env):

  ingestion_runs  — one document per source per run (+ one "_pipeline" summary)
  review_items    — one document per problem, keyed so a repeat sighting
                    updates the same item instead of adding a new one

review_items types written here:
  run_failed          a source returned success=False
  unknown_state       a state spelling not in the states collection
  price_invalid       min > max, modal outside min–max, or a price <= 0

Crop and mandi names are NOT review items: a new name is stored as a pending
row in crop_names / mandi_names (see catalog.py), and that row is what a
reviewer approves. The run log still counts how many were unmatched.

Nothing in this module may break the pipeline: every write is best-effort.
"""

from __future__ import annotations

import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = (os.getenv("MANDI_DB_NAME") or "").strip()
RUNS_COLLECTION = (os.getenv("INGESTION_RUNS_COLLECTION") or "ingestion_runs").strip()
REVIEW_COLLECTION = (os.getenv("REVIEW_ITEMS_COLLECTION") or "review_items").strip()

# Cap per run so one broken source cannot flood the review list.
MAX_PRICE_ITEMS_PER_RUN = 500
SAMPLE_MARKETS = 10


# ─────────────────────────────────────────────
# Pure builders (no DB) — easy to test
# ─────────────────────────────────────────────

def price_problems(doc: dict) -> list[str]:
    """Return human-readable problems with one normalised price document."""
    lo, hi, modal = doc.get("min_price"), doc.get("max_price"), doc.get("modal_price")
    problems: list[str] = []
    for label, val in (("min", lo), ("max", hi), ("modal", modal)):
        if val is not None and val <= 0:
            problems.append(f"{label} price is {val:g}")
    if lo is not None and hi is not None and lo > hi:
        problems.append(f"min price {lo:g} is above max price {hi:g}")
    if modal is not None and lo is not None and hi is not None and lo <= hi and not (lo <= modal <= hi):
        problems.append(f"modal price {modal:g} is outside {lo:g}–{hi:g}")
    return problems


def summarise_documents(documents: list[dict]) -> dict:
    """
    Group normalised documents by source key (``source_state``) and state.

    Returns ``{source: {"records": n, "states": {state: {...}}}}`` plus the
    unmapped crop names, unknown mandis and price problems found.
    """
    by_source: dict = defaultdict(lambda: {"records": 0, "states": defaultdict(lambda: {
        "records": 0, "markets": set(), "unmapped_commodities": set(), "unknown_markets": set(),
    })})
    unmapped: dict = {}
    unknown_markets: dict = {}
    bad_prices: list[dict] = []

    for doc in documents:
        source = doc.get("source_state") or doc.get("source_system") or "unknown"
        state = doc.get("state") or "unknown"
        market = doc.get("market_name")
        commodity = doc.get("commodity_name")

        src = by_source[source]
        src["records"] += 1
        st = src["states"][state]
        st["records"] += 1
        if market:
            st["markets"].add(market)

        if commodity and doc.get("product_id") is None:
            st["unmapped_commodities"].add(commodity)
            item = unmapped.setdefault(commodity, {
                "records": 0, "states": set(), "sources": set(), "markets": set(),
            })
            item["records"] += 1
            item["states"].add(state)
            item["sources"].add(doc.get("source_system") or source)
            if market and len(item["markets"]) < SAMPLE_MARKETS:
                item["markets"].add(market)

        if market and doc.get("mandi_id") is None:
            st["unknown_markets"].add(market)
            item = unknown_markets.setdefault((state, market), {
                "records": 0, "sources": set(), "commodities": set(),
            })
            item["records"] += 1
            item["sources"].add(doc.get("source_system") or source)
            if commodity and len(item["commodities"]) < SAMPLE_MARKETS:
                item["commodities"].add(commodity)

        if len(bad_prices) < MAX_PRICE_ITEMS_PER_RUN:
            problems = price_problems(doc)
            if problems:
                bad_prices.append({"doc": doc, "problems": problems})

    return {
        "by_source": by_source,
        "unmapped": unmapped,
        "unknown_markets": unknown_markets,
        "bad_prices": bad_prices,
    }


def _date_key(value) -> str | None:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value) if value else None


def build_run_documents(
    merged: dict,
    summary: dict,
    upload_stats: dict,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    date_str: str,
) -> list[dict]:
    """One ingestion_runs document per source, plus a ``_pipeline`` summary."""
    docs: list[dict] = []
    by_source = summary["by_source"]
    base = {
        "run_id": run_id,
        "requested_date": date_str or None,
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": round((finished_at - started_at).total_seconds(), 1),
    }

    for source, payload in merged.items():
        payload = payload if isinstance(payload, dict) else {}
        data = payload.get("data") or []
        saved = by_source.get(source, {"records": 0, "states": {}})
        error = payload.get("error") or None
        if payload.get("details") and payload.get("details") != error:
            error = f"{error}: {payload['details']}" if error else payload["details"]
        docs.append({
            **base,
            "source": source,
            "success": bool(payload.get("success")),
            "error": (str(error)[:1000] if error else None),
            "records_fetched": len(data) if isinstance(data, (list, tuple)) else 0,
            "records_normalised": saved["records"],
            "states": sorted(
                (
                    {
                        "state": state,
                        "records": st["records"],
                        "markets": len(st["markets"]),
                        "unmapped_commodities": len(st["unmapped_commodities"]),
                        "unknown_markets": len(st["unknown_markets"]),
                    }
                    for state, st in saved["states"].items()
                ),
                key=lambda s: s["state"],
            ),
        })

    docs.append({
        **base,
        "source": "_pipeline",
        "success": all(d["success"] for d in docs) and not (upload_stats or {}).get("error"),
        "error": (upload_stats or {}).get("error"),
        "records_fetched": sum(d["records_fetched"] for d in docs),
        "records_normalised": sum(d["records_normalised"] for d in docs),
        "sources_failed": [d["source"] for d in docs if not d["success"]],
        "upload_stats": upload_stats or {},
        "unmapped_commodities": len(summary["unmapped"]),
        "unknown_markets": len(summary["unknown_markets"]),
        "price_problems": len(summary["bad_prices"]),
    })
    return docs


def build_review_ops(merged: dict, summary: dict, run_id: str, now: datetime) -> list[UpdateOne]:
    """Upserts for review_items. A repeat sighting refreshes the same item."""
    ops: list[UpdateOne] = []

    def upsert(key: str, item_type: str, title: str, details: dict, *,
               state=None, source=None, add_to_set: dict | None = None, records: int = 0) -> None:
        update = {
            "$setOnInsert": {"key": key, "type": item_type, "status": "open", "first_seen": now},
            "$set": {"title": title, "details": details, "last_seen": now, "last_run_id": run_id,
                     "state": state, "source": source},
            "$inc": {"times_seen": 1, "records_seen": records},
        }
        if add_to_set:
            update["$addToSet"] = {k: {"$each": sorted(v)} for k, v in add_to_set.items() if v}
            if not update["$addToSet"]:
                del update["$addToSet"]
        ops.append(UpdateOne({"key": key}, update, upsert=True))

    day = now.strftime("%Y-%m-%d")
    for source, payload in merged.items():
        if isinstance(payload, dict) and payload.get("success"):
            continue
        payload = payload if isinstance(payload, dict) else {}
        error = payload.get("details") or payload.get("error") or "invalid payload"
        upsert(f"run_failed|{source}|{day}", "run_failed", f"{source} scrape failed on {day}",
               {"error": str(error)[:1000], "date": day}, source=source)

    for spelling, count in (summary.get("unknown_states") or {}).items():
        upsert(f"unknown_state|{spelling}", "unknown_state", f"State spelling not known: {spelling}",
               {"spelling": spelling}, records=count)

    for bad in summary["bad_prices"]:
        doc = bad["doc"]
        date = _date_key(doc.get("date"))
        market, commodity = doc.get("market_name"), doc.get("commodity_name")
        upsert(
            f"price_invalid|{doc.get('source_system')}|{doc.get('state')}|{market}|{commodity}|{doc.get('variety')}|{date}",
            "price_invalid", f"Price looks wrong: {commodity} at {market} on {date}",
            {"problems": bad["problems"], "market_name": market, "commodity_name": commodity,
             "variety": doc.get("variety"), "date": date, "min_price": doc.get("min_price"),
             "max_price": doc.get("max_price"), "modal_price": doc.get("modal_price")},
            state=doc.get("state"), source=doc.get("source_system"), records=1,
        )
    return ops


# ─────────────────────────────────────────────
# DB writes
# ─────────────────────────────────────────────

def ensure_indexes(db) -> None:
    runs, review = db[RUNS_COLLECTION], db[REVIEW_COLLECTION]
    runs.create_index([("started_at", DESCENDING)], name="started_at_desc")
    runs.create_index([("source", ASCENDING), ("started_at", DESCENDING)], name="source_started_at")
    runs.create_index([("run_id", ASCENDING)], name="run_id")
    review.create_index([("key", ASCENDING)], name="key_unique", unique=True)
    review.create_index([("status", ASCENDING), ("type", ASCENDING), ("last_seen", DESCENDING)],
                        name="status_type_last_seen")
    review.create_index([("state", ASCENDING)], name="state")


def save_run(
    merged: dict,
    documents: list[dict],
    upload_stats: dict,
    started_at: datetime,
    date_str: str = "",
    uri: str = MONGO_URI,
    db_name: str = DB_NAME,
    run_id: str | None = None,
) -> str | None:
    """Write this run to ingestion_runs and its problems to review_items. Never raises."""
    if not db_name:
        print("[WARN] review_log: MANDI_DB_NAME not set — run not recorded.")
        return None
    run_id = run_id or uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    if started_at.tzinfo is None:
        started_at = started_at.astimezone(timezone.utc)
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        db = client[db_name]
        ensure_indexes(db)
        summary = summarise_documents(documents)
        summary["unknown_states"] = {s: 1 for s in (upload_stats or {}).get("unknown_states") or []}
        run_docs = build_run_documents(merged, summary, upload_stats, run_id, started_at, now, date_str)
        db[RUNS_COLLECTION].insert_many(run_docs)
        ops = build_review_ops(merged, summary, run_id, now)
        if ops:
            db[REVIEW_COLLECTION].bulk_write(ops, ordered=False)
        print(f"[INFO] review_log: run {run_id} saved ({len(run_docs)} run docs, {len(ops)} review items).")
        client.close()
        return run_id
    except Exception as exc:
        print(f"[WARN] review_log: could not save run: {exc}")
        return None
