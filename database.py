


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
from pymongo.errors import BulkWriteError
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


# ─────────────────────────────────────────────
# UNIFIED SCHEMA
# ─────────────────────────────────────────────
# Every document stored in MongoDB will follow this shape:
#
#  {
#    state          : str        – state name (title-case)
#    district       : str|None   – district when available
#    market         : str        – market / mandi name (upper)
#    date           : datetime   – UTC midnight of the price date
#    commodity      : str        – commodity name (upper)
#    variety        : str|None   – variety / grade detail
#    grade          : str|None   – FAQ / Medium / etc.
#    unit           : str|None   – Quintal / Kg / etc.
#    arrival_qty    : float|None – arrival quantity in that unit
#    min_price      : float|None – minimum price (₹)
#    max_price      : float|None – maximum price (₹)
#    modal_price    : float|None – modal / average price (₹)
#    wholesale_rate : float|None – wholesale rate when given separately
#    retail_price   : float|None – retail price when given separately
#    source_state   : str        – original key in raw data
#    ingested_at    : datetime   – UTC timestamp of this upload
#  }
#
#  Unique index : (state, market, commodity, variety, date)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _parse_date(raw: str) -> str | None:
    """Parse DD/MM/YYYY or any common date string → YYYY-MM-DD string."""
    if not raw:
        return None
    try:
        dt = date_parser.parse(str(raw), dayfirst=True)
        return dt.strftime("%Y-%m-%d")
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
        state          = state.lower(),
        district       = None,                              # not provided by source
        market         = _lower(record.get("Market")),
        date           = _parse_date(record.get("Date")),
        commodity      = _lower(record.get("Commodity")),
        commodity_group= None,                              # not provided by source
        variety        = _lower(record.get("Variety")),
        grade          = _lower(record.get("Grade")),
        unit           = _lower(record.get("Unit")),
        arrival_qty    = _to_float(record.get("Arrival")),
        min_price      = _to_float(record.get("Min")),
        max_price      = _to_float(record.get("Max")),
        modal_price    = _to_float(record.get("Modal")),
        wholesale_rate = None,                              # not provided by source
        retail_price   = None,                              # not provided by source
        as_on_price    = None,
        msp_price      = None,
        trend          = None,
        one_day_ago_price   = None,
        two_day_ago_price   = None,
        one_day_ago_arrival = None,
        two_day_ago_arrival = None,
        source_url     = sources["karnataka"]["url"],
        method         = sources["karnataka"]["method"],
        source_name    = sources["karnataka"]["source_name"],
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
        state          = state.lower(),
        district       = None,                                          # not provided by source
        market         = _lower(record.get("market")),
        date           = _parse_date(record.get("date")),
        commodity      = _lower(record.get("commodity_name")),
        commodity_group= None,                                          # not provided by source
        variety        = _lower(record.get("variety")),
        grade          = _lower(record.get("grade")),
        unit           = _lower(record.get("Unit")),                    # key is Title-case in source
        arrival_qty    = _to_float(record.get("arrival_quintals")),
        min_price      = _to_float(record.get("min_price_rs_per_quintal")),
        max_price      = _to_float(record.get("max_price_rs_per_quintal")),
        modal_price    = _to_float(record.get("Modal")),                # key is Title-case in source
        wholesale_rate = None,                                          # not provided by source
        retail_price   = None,                                          # not provided by source
        as_on_price    = None,
        msp_price      = None,
        trend          = None,
        one_day_ago_price   = None,
        two_day_ago_price   = None,
        one_day_ago_arrival = None,
        two_day_ago_arrival = None,
        source_url     = sources["meghalaya"]["url"],
        method         = sources["meghalaya"]["method"],
        source_name    = sources["meghalaya"]["source_name"],
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
        state          = state.lower(),
        district       = _lower(record.get("District")),
        market         = _lower(record.get("Market")),
        date           = _parse_date(record.get("Arrival Date")),
        commodity      = _lower(record.get("Commodity")),
        commodity_group= None,                              # not provided by source
        variety        = _lower(record.get("Variety")),
        grade          = None,                              # not provided by source
        unit           = "quintal",                         # implied; not stated in raw record
        arrival_qty    = None,                              # not provided by source
        min_price      = _to_float(record.get("Min Price")),
        max_price      = _to_float(record.get("Max Price")),
        modal_price    = _to_float(record.get("Avg price")),
        wholesale_rate = None,                              # not provided by source
        retail_price   = None,                              # not provided by source
        as_on_price    = None,
        msp_price      = None,
        trend          = None,
        one_day_ago_price   = None,
        two_day_ago_price   = None,
        one_day_ago_arrival = None,
        two_day_ago_arrival = None,
        source_url     = sources["nagaland"]["url"],
        method         = sources["nagaland"]["method"],
        source_name    = sources["nagaland"]["source_name"],
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
        state          = state.lower(),
        district       = None,                              # not provided by source
        market         = _lower(record.get("Market")),      # key is Title-case in source
        date           = _parse_date(record.get("date")),
        commodity      = _lower(record.get("commodity")),
        commodity_group= None,                              # not provided by source
        variety        = _lower(record.get("variety")),
        grade          = None,                              # not provided by source
        unit           = _lower(record.get("unit")),
        arrival_qty    = _to_float(record.get("arrival")),
        min_price      = _to_float(record.get("min_price")),
        max_price      = _to_float(record.get("max_price")),
        modal_price    = _to_float(record.get("modal_price")),
        wholesale_rate = None,                              # not provided by source
        retail_price   = None,                              # not provided by source
        as_on_price    = None,
        msp_price      = None,
        trend          = None,
        one_day_ago_price   = None,
        two_day_ago_price   = None,
        one_day_ago_arrival = None,
        two_day_ago_arrival = None,
        source_url     = sources["maharashtra"]["url"],
        method         = sources["maharashtra"]["method"],
        source_name    = sources["maharashtra"]["source_name"],
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
        state          = state.lower(),
        district       = None,                                  # not provided by source
        market         = _lower(record.get("Market")),
        date           = _parse_date(record.get("Date")),
        commodity      = _lower(record.get("Commodity")),
        commodity_group= None,                                  # not provided by source
        variety        = None,                                  # not provided by source
        grade          = None,                                  # not provided by source
        unit           = "quintal",                             # implied; not stated in raw record
        arrival_qty    = _to_float(record.get("arrival")),      # key is lowercase in source
        min_price      = None,                                  # not provided; use wholesale_rate
        max_price      = None,                                  # not provided; use wholesale_rate
        modal_price    = None,                                  # not provided; use wholesale_rate
        wholesale_rate = _to_float(record.get("Wholesale_rate")),
        retail_price   = _to_float(record.get("Retail_price")),
        as_on_price    = None,
        msp_price      = None,
        trend          = None,
        one_day_ago_price   = None,
        two_day_ago_price   = None,
        one_day_ago_arrival = None,
        two_day_ago_arrival = None,
        source_url     = sources["uttar_pradesh"]["url"],
        method         = sources["uttar_pradesh"]["method"],
        source_name    = sources["uttar_pradesh"]["source_name"],
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
        state          = state.lower(),
        district       = _lower(record.get("DistrictName")),
        market         = _lower(record.get("Market")),
        date           = _parse_date(record.get("EntryDate")),
        commodity      = _lower(record.get("CommodityName")),
        commodity_group= None,                                  # not provided by source
        variety        = None,                                  # not provided by source
        grade          = None,                                  # not provided by source
        unit           = "quintal",                             # implied; not stated in raw record
        arrival_qty    = _to_float(record.get("Quantity")),
        min_price      = _to_float(record.get("Minprice")),     # note: lowercase 'p' in source
        max_price      = _to_float(record.get("MaxPrice")),
        modal_price    = _to_float(record.get("ModalPrice")),
        wholesale_rate = None,                                  # not provided by source
        retail_price   = None,                                  # not provided by source
        as_on_price    = None,
        msp_price      = None,
        trend          = None,
        one_day_ago_price   = None,
        two_day_ago_price   = None,
        one_day_ago_arrival = None,
        two_day_ago_arrival = None,
        source_url     = sources["punjab"]["url"],
        method         = sources["punjab"]["method"],
        source_name    = sources["punjab"]["source_name"],
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
        state                = _lower(record_state),
        district             = None,
        market               = _lower(record.get("market")),        # lowercase key
        date                 = _parse_date(record.get("reported_date")),
        commodity            = _lower(record.get("cmdt_name")),
        commodity_group      = _lower(record.get("cmdt_grp_name")), # e.g. "cereals"
        variety              = None,
        grade                = None,
        unit                 = "quintal",
        arrival_qty          = _to_float(record.get("as_on_arrival")),
        min_price            = None,
        max_price            = None,
        modal_price          = None,
        wholesale_rate       = None,
        retail_price         = None,
        as_on_price          = _to_float(record.get("as_on_price")),
        msp_price            = _to_float(record.get("msp_price")),
        trend                = _lower(record.get("trend")),
        one_day_ago_price    = _to_float(record.get("one_day_ago_price")),
        two_day_ago_price    = _to_float(record.get("two_day_ago_price")),
        one_day_ago_arrival  = _to_float(record.get("one_day_ago_arrival")),
        two_day_ago_arrival  = _to_float(record.get("two_day_ago_arrival")),
        source_url           = sources["agmarknet"]["url"],
        method               = sources["agmarknet"]["method"],
        source_name          = sources["agmarknet"]["source_name"]
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
    # ── identity ──────────────────────────────────
    "state",
    "district",
    "market",
    "date",
    # ── commodity ─────────────────────────────────
    "commodity",
    "commodity_group",      # agmarknet: e.g. "cereals"
    "variety",
    "grade",
    "unit",
    # ── quantity ──────────────────────────────────
    "arrival_qty",
    # ── prices ────────────────────────────────────
    "min_price",
    "max_price",
    "modal_price",
    "wholesale_rate",
    "retail_price",
    "as_on_price",          # agmarknet: current price
    "msp_price",            # agmarknet: minimum support price
    "trend",                # agmarknet: "up" / "down" / "stable"
    # ── historical (agmarknet) ────────────────────
    "one_day_ago_price",
    "two_day_ago_price",
    "one_day_ago_arrival",
    "two_day_ago_arrival",
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


def ensure_indexes(collection) -> None:
    """Create indexes for fast querying and upsert matching."""
    collection.create_index(
        [
            ("state",     ASCENDING),
            ("market",    ASCENDING),
            ("commodity", ASCENDING),
            ("variety",   ASCENDING),
            ("date",      ASCENDING),
        ],
        unique=True,
        name="unique_price_entry",
        background=True,
    )
    collection.create_index([("date", ASCENDING)],      name="idx_date")
    collection.create_index([("state", ASCENDING)],     name="idx_state")
    collection.create_index([("commodity", ASCENDING)], name="idx_commodity")
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
    for doc in documents:
        # Lower-case every string field before writing
        doc = _lowercase_doc(doc)

        # Dedup key: commodity AND apmc (market) AND date — all three must match
        commodity_lc = (doc.get("commodity") or "").strip().lower() or None
        market_lc    = (doc.get("market")    or "").strip().lower() or None
        date_val     = doc.get("date")   # already a YYYY-MM-DD string or None

        filter_key = {
            "commodity": commodity_lc,   # AND
            "market":    market_lc,      # AND
            "date":      date_val,       # AND
        }
        ops.append(ReplaceOne(filter_key, doc, upsert=True))

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
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    with open("final_data.json", "r") as f:
        RAW_DATA = json.load(f)
    docs = normalise_all(RAW_DATA)
    upload_to_mongo(docs)