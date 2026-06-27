


"""
Agricultural Market Data — MongoDB Upload Script
=================================================
Normalises heterogeneous state-wise market data into a single
unified schema and bulk-inserts it into MongoDB via pymongo.

Usage
-----
  pip install pymongo python-dateutil
  python agri_market_upload.py

Set MONGO_URI as an environment variable or edit the constant below.
"""

import os
import re
from datetime import datetime, timezone
from dateutil import parser as date_parser
from pymongo import MongoClient, ReplaceOne, ASCENDING
from pymongo.errors import BulkWriteError, OperationFailure
from dotenv import load_dotenv
import json
load_dotenv()
from utils.sources import sources
# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME     = os.getenv("MANDI_DB_NAME").strip()
COLLECTION  = os.getenv("MANDI_COLLECTION")
# FOr Commodity

# ─────────────────────────────────────────────
# UNIFIED SCHEMA
# ─────────────────────────────────────────────
# Every document stored in MongoDB will follow this shape:
#
#  {
#    source_system    : str        – source identifier (e.g. 'agmarknet')
#    state            : str        – state name (lower-case)
#    date             : datetime   – UTC midnight of the price date
#    market_name      : str|None   – market / mandi name (lower-case)
#    market_id        : str|None   – unique market ID
#    commodity_id     : str|None   – unique commodity ID
#    commodity_group  : str|None   – commodity group (e.g. 'cereals')
#    commodity_name   : str        – commodity name (lower-case)
#    variety          : str|None   – variety / grade detail
#    grade            : str|None   – FAQ / Medium / etc.
#    arrival_quantity : float|None – arrival quantity
#    min_price        : float|None – minimum price (₹)
#    max_price        : float|None – maximum price (₹)
#    modal_price      : float|None – modal / average price (₹)
#    source_url       : str        – URL of data source
#    source_name      : str        – name of data source
#    method           : str        – scraping method used
#    source_state     : str        – original state key in raw data
#    ingested_at      : datetime   – UTC timestamp of this upload
#  }
#
#  Unique index : (state, market_name, commodity_name, variety, date)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _parse_date(raw: str) -> datetime | None:
    """Parse DD/MM/YYYY or any common date string → UTC midnight datetime."""
    if not raw:
        return None
    try:
        dt = date_parser.parse(str(raw), dayfirst=True)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    except Exception:
        return None


def _to_float(val) -> float | None:
    """Coerce numbers / price strings like 'Rs 5500 / Quintal' → float."""
    if val is None:
        return None
    s = str(val)
    # strip non-numeric chars except dot and minus
    cleaned = re.sub(r"[^\d.\-]", "", s.split("/")[0])
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _lower(val) -> str | None:
    return str(val).strip().lower() if val else None


def _lowercase_doc(doc: dict) -> dict:
    """Return a copy of *doc* with every string value lower-cased."""
    return {
        k: (v.strip().lower() if isinstance(v, str) else v)
        for k, v in doc.items()
    }


# ─────────────────────────────────────────────
# STATE-SPECIFIC NORMALISERS
# ─────────────────────────────────────────────

def _norm_karnataka(record: dict, state: str) -> dict:
    """Normalise a single record from the Karnataka KRAMA scraper.

    Karnataka (KRAMA) field reference
    ----------------------------------
    Market      – market / mandi name  (Title-case in source)
    Date        – price date  (DD/MM/YYYY)
    Commodity   – commodity name  (Title-case)
    Variety     – variety / grade detail
    Grade       – FAQ / Medium / etc.
    Unit        – Quintal / Kg / etc.
    Arrival     – arrival quantity in the given unit
    Min         – minimum price  (₹/unit)
    Max         – maximum price  (₹/unit)
    Modal       – modal price  (₹/unit)

    Source: https://krama.karnataka.gov.in  (web scraper)
    """
    return dict(
        source_system    = "krama",
        state            = state.lower(),
        date             = _parse_date(record.get("Date")),
        market_name      = _lower(record.get("Market")),
        market_id        = None,
        commodity_id     = get_commodity_id(_lower(record.get("Commodity"))),
        commodity_group  = _lower(record.get("Group")),
        commodity_name   = _lower(record.get("Commodity")),
        variety          = _lower(record.get("Variety")),
        grade            = _lower(record.get("Grade")),
        arrival_quantity = _to_float(record.get("Arrival")),
        min_price        = _to_float(record.get("Min")),
        max_price        = _to_float(record.get("Max")),
        modal_price      = _to_float(record.get("Modal")),
        source_url       = sources["karnataka"]["url"],
        method           = sources["karnataka"]["method"],
        source_name      = sources["karnataka"]["source_name"],
    )


def _norm_meghalaya(record: dict, state: str) -> dict:
    """Normalise a single record from the Meghalaya MEGAMB scraper.

    Meghalaya (MEGAMB) field reference
    ------------------------------------
    market                    – market / mandi name  (lowercase in source)
    date                      – price date  (YYYY-MM-DD or similar)
    commodity_name            – commodity name
    variety                   – variety / grade detail
    grade                     – FAQ / Medium / etc.
    Unit                      – unit of measurement  (note: Title-case key)
    arrival_quintals          – arrival quantity (quintals)
    min_price_rs_per_quintal  – minimum price  (₹/quintal)
    max_price_rs_per_quintal  – maximum price  (₹/quintal)
    Modal                     – modal price  (₹/quintal)  (note: Title-case key)

    Source: https://megamb.gov.in  (web scraper)
    """
    return dict(
        source_system    = "megamb",
        state            = state.lower(),
        date             = _parse_date(record.get("date")),
        market_name      = _lower(record.get("market")),
        market_id        = None,
        commodity_id     = get_commodity_id(_lower(record.get("commodity_name"))),
        commodity_group  = _lower(record.get("Group Name")),
        commodity_name   = _lower(record.get("commodity_name")),
        variety          = _lower(record.get("Variety")),
        grade            = _lower(record.get("Grade")),
        arrival_quantity = _to_float(record.get("arrival_quintals")),
        min_price        = _to_float(record.get("min_price_rs_per_quintal")),
        max_price        = _to_float(record.get("max_price_rs_per_quintal")),
        modal_price      = _to_float(record.get("Modal")),
        source_url       = sources["meghalaya"]["url"],
        method           = sources["meghalaya"]["method"],
        source_name      = sources["meghalaya"]["source_name"],
    )


def _norm_nagaland(record: dict, state: str) -> dict:
    """Normalise a single record from the Nagaland commodityonline scraper.

    Nagaland (commodityonline) field reference
    -------------------------------------------
    District     – district name  (Title-case)
    Market       – market / mandi name  (Title-case)
    Arrival Date – price / arrival date  (DD/MM/YYYY)
    Commodity    – commodity name  (Title-case)
    Variety      – variety detail  (Title-case)
    Min Price    – minimum price  (₹/quintal, implied)
    Max Price    – maximum price  (₹/quintal, implied)
    Avg price    – average / modal price  (₹/quintal, implied)

    Notes:
    - No arrival quantity is reported by this source.
    - Unit is always quintal (implied; not stated in the raw record).
    - Grade is not reported.

    Source: https://www.commodityonline.com  (web scraper)
    """
    return dict(
        source_system    = "commodity_online",
        state            = state.lower(),
        date             = _parse_date(record.get("Arrival Date")),
        market_name      = _lower(record.get("Market")),
        market_id        = None,
        commodity_id     = get_commodity_id(_lower(record.get("commodity_name"))),
        commodity_group  = None,
        commodity_name   = _lower(record.get("Commodity")),
        variety          = _lower(record.get("Variety")),
        grade            = None,
        arrival_quantity = None,
        min_price        = _to_float(record.get("Min Price")),
        max_price        = _to_float(record.get("Max Price")),
        modal_price      = _to_float(record.get("Avg price")),
        source_url       = sources["nagaland"]["url"],
        method           = sources["nagaland"]["method"],
        source_name      = sources["nagaland"]["source_name"],
    )


def _norm_maharashtra(record: dict, state: str) -> dict:
    """Normalise a single record from the Maharashtra MSAMB API.

    Maharashtra (MSAMB) field reference
    -------------------------------------
    Market       – market / mandi name  (Title-case key, mixed-case value)
    date         – price date  (DD/MM/YYYY or similar)
    commodity    – commodity name  (lowercase)
    variety      – variety detail  (lowercase)
    unit         – unit of measurement  (lowercase, e.g. "quintal")
    arrival      – arrival quantity in the given unit
    min_price    – minimum price  (₹/unit)
    max_price    – maximum price  (₹/unit)
    modal_price  – modal price  (₹/unit)

    Notes:
    - District and grade are not reported by this source.
    - Wholesale / retail prices are not reported.

    Source: https://www.msamb.com  (external API)
    """
    return dict(
        source_system    = "msamb",
        state            = state.lower(),
        date             = _parse_date(record.get("date")),
        market_name      = _lower(record.get("Market")),      # key is Title-case in source
        market_id        = None,
        commodity_id     = get_commodity_id(_lower(record.get("commodity"))),
        commodity_group  = None,
        commodity_name   = _lower(record.get("commodity")),
        variety          = _lower(record.get("variety")),
        grade            = None,
        arrival_quantity = _to_float(record.get("arrival")),
        min_price        = _to_float(record.get("min_price")),
        max_price        = _to_float(record.get("max_price")),
        modal_price      = _to_float(record.get("modal_price")),
        source_url       = sources["maharashtra"]["url"],
        method           = sources["maharashtra"]["method"],
        source_name      = sources["maharashtra"]["source_name"],
    )


def _norm_uttar_pradesh(record: dict, state: str) -> dict:
    """Normalise a single record from the Uttar Pradesh UP Mandi Prices API.

    Uttar Pradesh (upmandiprices.in) field reference
    --------------------------------------------------
    Market          – market / mandi name  (Title-case)
    Date            – price date  (DD/MM/YYYY)
    Commodity       – commodity name  (Title-case)
    arrival         – arrival quantity (quintals, lowercase key)
    Wholesale_rate  – wholesale price  (₹/quintal, Title-case key)
    Retail_price    – retail price  (₹/quintal, Title-case key)

    Notes:
    - District, variety, and grade are not reported.
    - Min / max / modal prices are NOT available; this source gives
      wholesale and retail rates instead.
    - Unit is always quintal (implied; not stated in raw record).

    Source: https://upmandiprices.in  (external API)
    """
    return dict(
        source_system    = "up_krishi",
        state            = state.lower(),
        date             = _parse_date(record.get("Date")),
        market_name      = None,                                # not provided by source
        market_id        = None,
        commodity_id     = get_commodity_id(_lower(record.get("ProductName"))),
        commodity_group  = _lower(record.get("MainProductName")),
        commodity_name   = _lower(record.get("ProductName")),
        variety          = None,
        grade            = None,
        arrival_quantity = _to_float(record.get("aavakRate")),
        min_price        = None,
        max_price        = None,
        modal_price      = None,
        source_url       = sources["uttar_pradesh"]["url"],
        method           = sources["uttar_pradesh"]["method"],
        source_name      = sources["uttar_pradesh"]["source_name"],
    )


def _norm_punjab(record: dict, state: str) -> dict:
    """Normalise a single record from the Punjab e-Mandikaran API.

    Punjab (e-Mandikaran) field reference
    ---------------------------------------
    DistrictName  – district name  (Title-case)
    Market        – market / mandi name  (Title-case)
    EntryDate     – price / entry date  (DD/MM/YYYY or ISO)
    CommodityName – commodity name  (Title-case)
    Quantity      – arrival quantity  (quintals)
    Minprice      – minimum price  (₹/quintal; note: inconsistent casing)
    MaxPrice      – maximum price  (₹/quintal)
    ModalPrice    – modal price  (₹/quintal)

    Notes:
    - Variety and grade are not reported by this source.
    - Wholesale / retail prices are not reported.
    - Unit is always quintal (implied; not stated in raw record).

    Source: https://emandikaran-pb.in  (external API)
    """
    return dict(
        source_system    = "emandikaran",
        state            = state.lower(),
        date             = _parse_date(record.get("EntryDate")),
        market_name      = _lower(record.get("BranchName")),
        market_id        = None,
        commodity_id     = get_commodity_id(_lower(record.get("CommodityName"))),
        commodity_group  = None,
        commodity_name   = _lower(record.get("CommodityName")),
        variety          = None,
        grade            = None,
        arrival_quantity = _to_float(record.get("Quantity")),
        min_price        = _to_float(record.get("Minprice")),     # note: lowercase 'p' in source
        max_price        = _to_float(record.get("MaxPrice")),
        modal_price      = _to_float(record.get("ModalPrice")),
        source_url       = sources["punjab"]["url"],
        method           = sources["punjab"]["method"],
        source_name      = sources["punjab"]["source_name"],
    )

def _norm_agmarknet(record: dict, state: str) -> dict:
    """Normalise a single record from the Agmarknet API response.

    Agmarknet field reference
    -------------------------
    cmdt_name            – commodity name
    cmdt_grp_name        – commodity group  (e.g. "Cereals")
    market               – market / mandi name  (lowercase in API)
    state                – state name  (lowercase in API)
    reported_date        – price date  (DD-MM-YYYY)
    as_on_price          – current modal/as-on price (₹/quintal)
    as_on_arrival        – current arrival quantity (quintal)
    msp_price            – minimum support price (₹/quintal)
    trend                – price trend direction ("up" / "down" / "stable")
    one_day_ago_price    – price 1 day earlier (nullable)
    two_day_ago_price    – price 2 days earlier (nullable)
    one_day_ago_arrival  – arrival 1 day earlier (nullable)
    two_day_ago_arrival  – arrival 2 days earlier (nullable)
    """
    # Prefer the state embedded in the record; fall back to the passed-in key.
    record_state = record.get("state") or state

    return dict(
        source_system    = "agmarknet",
        state            = _lower(record_state),
        date             = _parse_date(record.get("reported_date")),
        market_name      = _lower(record.get("market")),        # lowercase key
        market_id        = None,
        commodity_id     = get_commodity_id(_lower(record.get("cmdt_name"))),
        commodity_group  = _lower(record.get("cmdt_grp_name")), # e.g. "cereals"
        commodity_name   = _lower(record.get("cmdt_name")),
        variety          = None,
        grade            = None,
        arrival_quantity = _to_float(record.get("as_on_arrival")),
        min_price        = None,
        max_price        = None,
        modal_price      = _to_float(record.get("as_on_price")),  # as_on_price is the modal price
        source_url       = sources["agmarknet"]["url"],
        method           = sources["agmarknet"]["method"],
        source_name      = sources["agmarknet"]["source_name"],
    )



# Map state key → normaliser function
STATE_NORMALISERS = {
    "Karnataka":     _norm_karnataka,
    "Meghalaya":     _norm_meghalaya,
    "Nagaland":      _norm_nagaland,
    "Maharashtra":   _norm_maharashtra,
    "Uttar Pradesh": _norm_uttar_pradesh,
    "Punjab":        _norm_punjab,
    "agmarknet":     _norm_agmarknet,   # national Agmarknet feed (multi-state)
}


# ─────────────────────────────────────────────
# CORE PIPELINE
# ─────────────────────────────────────────────

UNIFIED_KEYS = [
    # ── source & identity ─────────────────────────
    "source_system",
    "state",
    "date",
    # ── market ────────────────────────────────────
    "market_name",
    "market_id",
    # ── commodity ─────────────────────────────────
    "commodity_id",
    "commodity_group",
    "commodity_name",
    "variety",
    "grade",
    # ── quantity & prices ─────────────────────────
    "arrival_quantity",
    "min_price",
    "max_price",
    "modal_price",
    # ── provenance ────────────────────────────────
    "source_url",
    "source_name",
    "method",
    "source_state",
    "ingested_at",
]


def normalise_all(raw_data: dict) -> list[dict]:
    """
    raw_data : { "Karnataka": {"success": True, "data": [...]}, ... }
    Returns   : list of unified documents ready for MongoDB insertion.
    """
    now = datetime.now(tz=timezone.utc)
    documents = []

    for state_key, payload in raw_data.items():
        if not payload.get("success"):
            print(f"[SKIP] {state_key} — success=False")
            continue

        records = payload.get("data", [])
        normaliser = STATE_NORMALISERS.get(state_key)

        if normaliser is None:
            print(f"[WARN] No normaliser for state '{state_key}', skipping.")
            continue

        for rec in records:
            try:
                doc = normaliser(rec, state=state_key)
                doc["source_state"] = state_key     # original key preserved
                doc["ingested_at"]  = now
                
                # Enforce key order strictly using UNIFIED_KEYS
                ordered_doc = {k: doc.get(k, None) for k in UNIFIED_KEYS}
                documents.append(ordered_doc)
            except Exception as exc:
                print(f"[ERROR] {state_key} record skipped — {exc}: {rec}")

    print(f"[INFO] Normalised {len(documents)} documents across "
          f"{len(raw_data)} states.")
    return documents


def _create_index_safe(collection, keys, **kwargs) -> None:
    """Create an index, silently skipping if the name already exists."""
    name = kwargs.get("name", "<unnamed>")
    try:
        collection.create_index(keys, **kwargs)
    except OperationFailure as exc:
        # Code 85 = IndexOptionsConflict, 86 = IndexKeySpecsConflict.
        # Both mean the index (or its name) already exists — safe to ignore.
        print(f"[INFO] Index '{name}' already exists, skipping: {exc}")


def ensure_indexes(collection) -> None:
    """
    Create indexes for fast querying and upsert matching.

    Drops stale indexes whose key patterns no longer match the current schema
    (e.g. old ``market``/``commodity`` field names) before recreating them.
    """
    try:
        existing = collection.index_information()

        # ── Drop stale unique_price_entry if its key pattern is wrong ─────────
        if "unique_price_entry" in existing:
            old_keys = {k for k, _ in existing["unique_price_entry"].get("key", [])}
            expected = {"source_system", "state", "market_name", "commodity_name", "variety", "date"}
            if old_keys != expected:
                collection.drop_index("unique_price_entry")
                print("[INFO] Dropped stale 'unique_price_entry' index (wrong field names).")
    except Exception as exc:
        print(f"[WARN] Could not inspect/drop old index: {exc}")

    # ── Recreate with correct fields ──────────────────────────────────────────
    _create_index_safe(
        collection,
        [
            ("source_system",  ASCENDING),
            ("state",          ASCENDING),
            ("market_name",    ASCENDING),
            ("commodity_name", ASCENDING),
            ("variety",        ASCENDING),
            ("date",           ASCENDING),
        ],
        unique=True,
        name="unique_price_entry",
    )
    _create_index_safe(collection, [("date",           ASCENDING)], name="idx_date")
    _create_index_safe(collection, [("state",          ASCENDING)], name="idx_state")
    _create_index_safe(collection, [("commodity_name", ASCENDING)], name="idx_commodity")
    print("[INFO] Indexes ensured.")


def upload_to_mongo(documents: list[dict],
                    uri: str       = MONGO_URI,
                    db_name: str   = DB_NAME,
                    coll_name: str = COLLECTION) -> None:
    """
    Upsert documents into MongoDB.

    Duplicate detection is AND-wise across three fields:
        commodity (name)  AND  market (apmc name)  AND  date
    A record is only inserted when ALL three of these values are new
    together.  Matching on any subset alone is NOT sufficient to trigger
    a skip — all three must match simultaneously.

    All string fields are lower-cased before storage to ensure that
    'Wheat', 'WHEAT', and 'wheat' are treated as the same commodity.
    """
    if not documents:
        print("[INFO] Nothing to upload.")
        return

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    # Verify connectivity
    client.admin.command("ping")
    print(f"[INFO] Connected to MongoDB at {uri}")

    db   = client[db_name]
    coll = db[coll_name]

    ensure_indexes(coll)

    # Build upsert operations using ReplaceOne to enforce dictionary key order.
    # The filter uses AND logic across commodity + market (apmc) + date so that
    # a record is skipped/updated only when ALL three fields match an existing
    # document.  Any other combination results in a fresh insert.
    ops = []
    skipped = 0
    for doc in documents:
        # Lower-case every string field before writing
        doc = _lowercase_doc(doc)

        commodity_lc     = (doc.get("commodity_name") or "").strip().lower() or None
        market_lc        = (doc.get("market_name")    or "").strip().lower() or None
        state_lc         = (doc.get("state")         or "").strip().lower() or None
        source_system_lc = (doc.get("source_system") or "").strip().lower() or None
        variety_lc       = (doc.get("variety")       or "").strip().lower() or None

        # Normalise date: store as "YYYY-MM-DD" string for consistent index matching.
        # _parse_date returns a datetime object; convert to ISO date string here.
        raw_date = doc.get("date")
        if isinstance(raw_date, datetime):
            date_str = raw_date.strftime("%Y-%m-%d")
            doc["date"] = date_str   # store as string in the document too
        else:
            date_str = str(raw_date) if raw_date else None

        # Skip degenerate records where all unique-key fields are null —
        # they would all map to the same index slot and cause E11000 errors.
        if not any([source_system_lc, market_lc, commodity_lc, variety_lc, date_str]):
            skipped += 1
            continue

        filter_key = {
            "source_system":  source_system_lc,  # AND – different feeds never collide
            "state":          state_lc,          # AND – same commodity across states
            "market_name":    market_lc,         # AND
            "commodity_name": commodity_lc,      # AND
            "variety":        variety_lc,        # AND – same commodity, different variety
            "date":           date_str,          # AND – normalised to YYYY-MM-DD string
        }
        ops.append(ReplaceOne(filter_key, doc, upsert=True))

    if skipped:
        print(f"[WARN] Skipped {skipped} degenerate record(s) with all-null index fields.")

    try:
        result = coll.bulk_write(ops, ordered=False)
        print(f"[OK] Upserted  : {result.upserted_count}")
        print(f"[OK] Modified  : {result.modified_count}")
        print(f"[OK] Matched   : {result.matched_count}")
    except BulkWriteError as bwe:
        n_err = len(bwe.details.get("writeErrors", []))
        print(f"[WARN] Bulk write completed with {n_err} errors.")
        for err in bwe.details.get("writeErrors", [])[:5]:   # show first 5
            print(f"       {err}")
    finally:
        client.close()
        print("[INFO] Connection closed.")


# ─────────────────────────────────────────────
# COMMODITY LOOKUP  (singleton client + cache)
# ─────────────────────────────────────────────

# Module-level MongoClient for the commodity alias lookup database.
# MongoClient manages an internal connection pool; create it once and
# reuse it for the lifetime of the process.
_lookup_client: MongoClient | None = None
_lookup_coll = None

# Simple in-memory cache  { normalised_name -> crop_master_id | None }
# Avoids a DB round-trip when the same commodity appears across many records.
_commodity_id_cache: dict[str, str | None] = {}


def _get_lookup_collection(
    uri: str       = MONGO_URI,
    db_name: str   = DB_NAME,
    coll_name: str = "commodity_alias_lookup",
):
    """Return the cached commodity lookup collection, initialising once if needed.

    Returns ``None`` (instead of raising) when any of the required env vars
    are missing or not a non-empty string — commodity_id lookups will then
    simply return ``None`` without crashing the normalisation pipeline.
    """
    global _lookup_client, _lookup_coll
    # ── Guard: all three params must be non-empty strings ─────────────
    if not (isinstance(uri, str) and uri.strip() and
            isinstance(db_name, str) and db_name.strip() and
            isinstance(coll_name, str) and coll_name.strip()):
        print("[WARN] Commodity alias lookup DB not configured — lookups disabled.")
        return None
    if _lookup_coll is None:
        _lookup_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        _lookup_coll   = _lookup_client[db_name][coll_name]
        print("[INFO] Commodity alias lookup client connected.")
    return _lookup_coll


def get_commodity_id(commodity_name: str) -> str | None:
    """
    Look up the ``crop_master_id`` for *commodity_name* using a single
    persistent MongoDB connection (connection pool) + an in-memory cache.

    Matching strategy
    -----------------
    1. Normalise the input to lower-case and check the in-memory cache
       first — no DB hit if the same commodity was already resolved.
    2. Look up the normalized commodity name in the ``aliases`` array
       where ``crop_master_id`` is non-null.
    3. If that fails, perform a case-insensitive regex match on ``aliases``
       with a non-null ``crop_master_id``.
    4. Cache the result (even ``None``) so repeated calls are O(1).

    Returns
    -------
    str | None
        The ``crop_master_id``, or ``None`` if no match / no id found.
    """
    if not isinstance(commodity_name, str) or not commodity_name.strip():
        return None

    key = commodity_name.strip().lower()

    # ── Cache hit ──────────────────────────────────────────────────────
    if key in _commodity_id_cache:
        return _commodity_id_cache[key]

    # ── DB lookup ──────────────────────────────────────────────────────
    try:
        coll = _get_lookup_collection()
        if coll is None:
            # Lookup DB not configured; cache None to skip future lookups
            _commodity_id_cache[key] = None
            return None

        # 1. Exact match on aliases array
        doc = coll.find_one(
            {
                "aliases": key,
                "crop_master_id": {"$nin": [None, ""]},
            },
            projection={"crop_master_id": 1, "_id": 0},
        )
        if doc:
            result = doc["crop_master_id"]
            _commodity_id_cache[key] = result
            return result

        # 2. Fallback: regex search on aliases array
        pattern = re.compile(re.escape(key), re.IGNORECASE)
        fallback = coll.find_one(
            {
                "aliases": {"$regex": pattern},
                "crop_master_id": {"$nin": [None, ""]},
            },
            projection={"crop_master_id": 1, "_id": 0},
        )
        result = fallback.get("crop_master_id") if fallback else None
        _commodity_id_cache[key] = result   # cache None too — avoids re-querying
        return result
    except Exception as exc:
        # Never let a lookup failure crash the normalisation pipeline.
        print(f"[WARN] crop_master_id lookup failed for '{key}': {exc}")
        _commodity_id_cache[key] = None
        return None




# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    with open("final_data.json", "r") as f:
        RAW_DATA = json.load(f)
    docs = normalise_all(RAW_DATA)
    upload_to_mongo(docs)