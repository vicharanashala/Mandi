"""
Normalized Agricultural Market Data — Two-Collection Ingestion
==============================================================
Splits each incoming price record into:

  1. markets_commodities  — dimension / master table (written once per unique
                            market + commodity + state combination)
  2. price_records        — fact / time-series table (appended every ~2 hours;
                            null price/quantity fields are pruned before insert)

Design principles
-----------------
- $setOnInsert guarantees dimension fields are only written on first creation;
  duplicate upserts are no-ops on the dimension collection.
- Null pruning on price_records keeps the time-series collection lean.
- A module-level MongoClient (connection pool) is created once and reused.
- Index creation is idempotent: silently skips if index already exists.

Usage
-----
    from ingest_normalized import ingest_record

    incoming = { ... }   # one record matching the agmarknet unified schema
    ingest_record(incoming)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MANDI_DB_NAME", "mandi").strip()

MARKETS_COMMODITIES_COLL = "markets_commodities"
PRICE_RECORDS_COLL       = "price_records"

# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION  (singleton connection pool — created once per process)
# ─────────────────────────────────────────────────────────────────────────────

_client: MongoClient | None = None


def _get_db():
    """Return a cached MongoDatabase, creating the client on first call."""
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
        # Verify connectivity eagerly so failures surface at startup.
        _client.admin.command("ping")
        print(f"[INFO] Connected to MongoDB: {MONGO_URI!r}")
    return _client[DB_NAME]


# ─────────────────────────────────────────────────────────────────────────────
# INDEX MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def _create_index_safe(coll: Collection, keys: list, **kwargs) -> None:
    """Create an index, silently skipping if the same name already exists."""
    name = kwargs.get("name", "<unnamed>")
    try:
        coll.create_index(keys, **kwargs)
        print(f"[INFO] Index '{name}' ensured on '{coll.name}'.")
    except OperationFailure as exc:
        # Code 85 = IndexOptionsConflict, 86 = IndexKeySpecsConflict.
        if exc.code in (85, 86):
            print(f"[INFO] Index '{name}' already exists — skipping.")
        else:
            raise


def ensure_indexes_markets_commodities(coll: Collection) -> None:
    """
    Indexes for the markets_commodities (dimension) collection.

    Unique compound index on (market_name, commodity_id, state) is the
    upsert filter key — must exist before the first write.
    """
    # PRIMARY UPSERT KEY — unique compound on the three identity fields
    _create_index_safe(
        coll,
        [
            ("market_name",  ASCENDING),
            ("commodity_id", ASCENDING),
            ("state",        ASCENDING),
        ],
        unique=True,
        name="uq_market_commodity_state",
    )
    # Support queries by state
    _create_index_safe(coll, [("state", ASCENDING)], name="idx_mc_state")
    # Support queries by commodity
    _create_index_safe(coll, [("commodity_id", ASCENDING)], name="idx_mc_commodity_id")


def ensure_indexes_price_records(coll: Collection) -> None:
    """
    Indexes for the price_records (fact / time-series) collection.

    Compound index on (market_commodity_id, date) is the natural time-series
    access pattern; ingested_at supports time-range scans over recent loads.
    """
    # Time-series access pattern: one market-commodity over a date range
    _create_index_safe(
        coll,
        [
            ("market_commodity_id", ASCENDING),
            ("date",                ASCENDING),
        ],
        name="idx_pr_mc_date",
    )
    # Quickly fetch the latest ingest windows
    _create_index_safe(coll, [("ingested_at", ASCENDING)], name="idx_pr_ingested_at")
    # Direct date scan across all markets
    _create_index_safe(coll, [("date", ASCENDING)], name="idx_pr_date")


def ensure_all_indexes() -> None:
    """Create all indexes on both collections (idempotent — safe to call on startup)."""
    db = _get_db()
    ensure_indexes_markets_commodities(db[MARKETS_COMMODITIES_COLL])
    ensure_indexes_price_records(db[PRICE_RECORDS_COLL])
    print("[INFO] All indexes ensured.")


# ─────────────────────────────────────────────────────────────────────────────
# HELPER UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _strip_lower(val) -> str | None:
    """Lower-case and strip a string value; return None for falsy input."""
    if isinstance(val, str) and val.strip():
        return val.strip().lower()
    return None


def _prune_nulls(doc: dict) -> dict:
    """Return a shallow copy of *doc* with all None-valued keys removed."""
    return {k: v for k, v in doc.items() if v is not None}


# ─────────────────────────────────────────────────────────────────────────────
# COLLECTION 1 — markets_commodities  (dimension / master)
# ─────────────────────────────────────────────────────────────────────────────

def upsert_market_commodity(coll: Collection, incoming: dict) -> str:
    """
    Upsert the dimension record for a unique (market_name, commodity_id, state)
    combination and return its ``_id``.

    Strategy
    --------
    - ``$setOnInsert``: dimension fields are written ONLY on the first insert.
      Subsequent upserts with the same filter key are no-ops on the stored
      document — static metadata is never silently overwritten.
    - Returns the ``_id`` of either the newly inserted or the already-existing
      document.

    Parameters
    ----------
    coll     : the markets_commodities Collection handle
    incoming : one incoming unified price record (pre-normalised)

    Returns
    -------
    str  — the string representation of the document's ObjectId (_id)
    """
    # ── Build the compound filter key ─────────────────────────────────────────
    filter_doc = {
        "market_name":  _strip_lower(incoming.get("market_name")),
        "commodity_id": incoming.get("commodity_id"),          # already an ID str
        "state":        _strip_lower(incoming.get("state")),
    }

    # ── Dimension fields (written once, never overwritten) ─────────────────────
    set_on_insert = {
        "market_name":     _strip_lower(incoming.get("market_name")),
        "market_id":       incoming.get("market_id"),          # may be None
        "state":           _strip_lower(incoming.get("state")),
        "commodity_id":    incoming.get("commodity_id"),
        "commodity_name":  _strip_lower(incoming.get("commodity_name")),
        "commodity_group": _strip_lower(incoming.get("commodity_group")),
        "variety":         _strip_lower(incoming.get("variety")),
        "grade":           _strip_lower(incoming.get("grade")),
        "source_system":   _strip_lower(incoming.get("source_system")),
        "source_url":      incoming.get("source_url"),
        "source_name":     _strip_lower(incoming.get("source_name")),
        "method":          _strip_lower(incoming.get("method")),
        "created_at":      datetime.now(tz=timezone.utc),
    }

    result = coll.find_one_and_update(
        filter_doc,
        {"$setOnInsert": set_on_insert},
        upsert=True,
        # Return the document AFTER the operation so we always get the _id.
        # For an existing document, returnDocument=AFTER is equivalent to BEFORE
        # because $setOnInsert made no changes — the _id is stable either way.
        return_document=True,   # pymongo: True == ReturnDocument.AFTER
    )

    # find_one_and_update with upsert=True always returns a document.
    return str(result["_id"])


# ─────────────────────────────────────────────────────────────────────────────
# COLLECTION 2 — price_records  (fact / time-series)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_ingested_at(raw) -> datetime:
    """
    Return a timezone-aware datetime for ingested_at.

    Accepts:
      - datetime objects (returned as-is, UTC is forced if naive)
      - ISO 8601 strings like "2026-06-27T10:45:58.350Z"
      - Falls back to utcnow() if unparseable
    """
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, str):
        try:
            from dateutil import parser as _dp
            dt = _dp.parse(raw)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return datetime.now(tz=timezone.utc)


def insert_price_record(coll: Collection, market_commodity_id: str, incoming: dict) -> None:
    """
    Insert one price/quantity record into price_records.

    Null pruning
    ------------
    Fields whose value is ``None`` are omitted entirely from the stored
    document (not stored as explicit nulls).  This keeps the fact collection
    lean, especially for sources that never report min/max prices.

    Parameters
    ----------
    coll                 : the price_records Collection handle
    market_commodity_id  : _id returned by upsert_market_commodity()
    incoming             : one incoming unified price record
    """
    # ── Build the raw fact document ────────────────────────────────────────────
    raw_doc: dict = {
        "market_commodity_id": market_commodity_id,
        "date":                incoming.get("date"),            # str "YYYY-MM-DD" or datetime
        "arrival_quantity":    incoming.get("arrival_quantity"),
        "min_price":           incoming.get("min_price"),
        "max_price":           incoming.get("max_price"),
        "modal_price":         incoming.get("modal_price"),
        "ingested_at":         _parse_ingested_at(incoming.get("ingested_at")),
    }

    # ── Null pruning: drop every key whose value is None ──────────────────────
    # ingested_at is always set by _parse_ingested_at, so it is never pruned.
    pruned_doc = _prune_nulls(raw_doc)

    coll.insert_one(pruned_doc)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def ingest_record(incoming: dict) -> str:
    """
    Ingest a single unified price record into the two-collection schema.

    Steps
    -----
    1. Upsert into markets_commodities → receive market_commodity_id (_id).
    2. Insert into price_records using market_commodity_id as the FK reference.
    3. Return the market_commodity_id for caller inspection / logging.

    Parameters
    ----------
    incoming : dict
        One record conforming to the unified agmarknet schema, e.g.::

            {
                "source_system":   "agmarknet",
                "state":           "madhya pradesh",
                "date":            "2026-06-25",
                "market_name":     "a lot apmc",
                "market_id":       None,
                "commodity_id":    "69e27ab12323cbe6de5748e2",
                "commodity_group": "cereals",
                "commodity_name":  "wheat",
                "variety":         None,
                "grade":           None,
                "arrival_quantity": 104.42,
                "min_price":       None,
                "max_price":       None,
                "modal_price":     2333.55,
                "source_url":      "https://api.agmarknet.gov.in/v1",
                "source_name":     "agmarknet",
                "method":          "external_apis",
                "source_state":    "agmarknet",
                "ingested_at":     "2026-06-27T10:45:58.350Z",
            }

    Returns
    -------
    str  — the market_commodity_id used as foreign key in price_records
    """
    db = _get_db()
    mc_coll = db[MARKETS_COMMODITIES_COLL]
    pr_coll = db[PRICE_RECORDS_COLL]

    # Step 1 — Dimension upsert
    market_commodity_id = upsert_market_commodity(mc_coll, incoming)

    # Step 2 — Fact insert (null fields pruned)
    insert_price_record(pr_coll, market_commodity_id, incoming)

    return market_commodity_id


# ─────────────────────────────────────────────────────────────────────────────
# BULK INGESTION  (convenience wrapper for pipeline use)
# ─────────────────────────────────────────────────────────────────────────────

def ingest_batch(records: list[dict]) -> dict:
    """
    Ingest a list of unified price records.

    Ensures indexes once before processing, then calls ``ingest_record``
    for each record.  Errors on individual records are logged and skipped
    rather than aborting the entire batch.

    Parameters
    ----------
    records : list of unified schema dicts (one per price observation)

    Returns
    -------
    dict with keys:
        ``inserted``  – number of successfully ingested records
        ``failed``    – number of skipped / failed records
        ``errors``    – list of (index, error_message) tuples
    """
    ensure_all_indexes()

    inserted = 0
    failed   = 0
    errors: list[tuple[int, str]] = []

    for i, record in enumerate(records):
        try:
            mc_id = ingest_record(record)
            inserted += 1
            if inserted % 500 == 0:
                print(f"[INFO] Ingested {inserted} records so far …")
        except Exception as exc:
            failed += 1
            errors.append((i, str(exc)))
            print(f"[WARN] Record #{i} skipped — {exc}")

    print(
        f"[DONE] Batch complete — inserted: {inserted}, "
        f"failed: {failed} / {len(records)} total."
    )
    return {"inserted": inserted, "failed": failed, "errors": errors}


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT (manual / testing)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    # Ensure indexes exist before the first ingest run
    ensure_all_indexes()

    # ── Example: ingest a single record ───────────────────────────────────────
    sample = {
        "source_system":    "agmarknet",
        "state":            "madhya pradesh",
        "date":             "2026-06-25",
        "market_name":      "a lot apmc",
        "market_id":        None,
        "commodity_id":     "69e27ab12323cbe6de5748e2",
        "commodity_group":  "cereals",
        "commodity_name":   "wheat",
        "variety":          None,
        "grade":            None,
        "arrival_quantity": 104.42,
        "min_price":        None,
        "max_price":        None,
        "modal_price":      2333.55,
        "source_url":       "https://api.agmarknet.gov.in/v1",
        "source_name":      "agmarknet",
        "method":           "external_apis",
        "source_state":     "agmarknet",
        "ingested_at":      "2026-06-27T10:45:58.350Z",
    }

    mc_id = ingest_record(sample)
    print(f"[OK] market_commodity_id = {mc_id}")

    # ── Example: ingest from a JSON file (batch mode) ─────────────────────────
    # with open("agmarknet_records.json") as f:
    #     records = json.load(f)
    # stats = ingest_batch(records)
    # print(json.dumps(stats, indent=2))
