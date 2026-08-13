"""
Agricultural Market Data — MongoDB Upload Script
=================================================
Normalises heterogeneous state-wise market data into a unified schema,
then splits it into two MongoDB collections:

  1. markets_commodities  — master/dimension table (written once per unique combo)
  2. price_records        — fact/time-series table  (inserted every 2 hours)

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
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError, OperationFailure
from dotenv import load_dotenv
import json

load_dotenv()
from utils.sources import sources

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
MONGO_URI              = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME                = os.getenv("MANDI_DB_NAME", "").strip()
MASTER_COLLECTION      = os.getenv('MASTER_COLLECTION').strip()  # dimension table
PRICE_COLLECTION       = os.getenv('PRICE_COLLECTION').strip()         # fact / time-series table

ALL_MANDI_COLLECTION = os.getenv('ALL_MANDI_COLLECTION').strip()

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

_ISO_YMD = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_YMD_COMPACT = re.compile(r"^(\d{4})(\d{2})(\d{2})$")


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_date(raw: str) -> datetime | None:
    """Parse a date string → UTC-aware datetime (stored as BSON Date).

    ISO ``YYYY-MM-DD`` / ``YYYYMMDD`` are parsed with strptime, not dateutil.
    ``dateutil.parse(..., dayfirst=True)`` treats ``2026-07-09`` as
    ``YYYY-DD-MM`` → 2026-09-07, which is what produced future AP dates.
    Indian ``DD/MM/YYYY`` strings still go through dateutil with dayfirst.
    """
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        return _as_utc(raw)

    s = str(raw).strip()
    iso = _ISO_YMD.match(s)
    if iso:
        try:
            return datetime(
                int(iso.group(1)), int(iso.group(2)), int(iso.group(3)),
                tzinfo=timezone.utc,
            )
        except ValueError:
            return None

    compact = _YMD_COMPACT.match(s)
    if compact:
        try:
            return datetime(
                int(compact.group(1)), int(compact.group(2)), int(compact.group(3)),
                tzinfo=timezone.utc,
            )
        except ValueError:
            return None

    try:
        dt = date_parser.parse(s, dayfirst=True, yearfirst=False)
        return _as_utc(dt)
    except Exception:
        return None


def _is_future_date(dt: datetime | None, *, today: datetime | None = None) -> bool:
    if dt is None:
        return False
    today = today or datetime.now(timezone.utc)
    return _as_utc(dt).date() > _as_utc(today).date()


def _swap_dayfirst_iso_misparse(dt: datetime) -> datetime:
    """Undo dateutil dayfirst on ISO dates: 2026-09-07 ↔ 2026-07-09.

    Only dates with day <= 12 were ambiguous; day > 12 was already correct.
    """
    dt = _as_utc(dt)
    if dt.day > 12:
        return dt
    return dt.replace(month=dt.day, day=dt.month)


def _to_float(val) -> float | None:
    """Coerce numbers / price strings like 'Rs 5500 / Quintal' → float."""
    if val is None:
        return None
    s = str(val)
    cleaned = re.sub(r"[^\d.\-]", "", s.split("/")[0])
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _lower(val) -> str | None:
    return str(val).strip().lower() if val else None


def _strip_nulls(doc: dict) -> dict:
    """Remove keys whose value is None — reduces document size."""
    return {k: v for k, v in doc.items() if v is not None}


# ─────────────────────────────────────────────
# STATE-SPECIFIC NORMALISERS
# Each returns a FULL flat dict; splitting happens in the pipeline.
# ─────────────────────────────────────────────

def _norm_karnataka(record: dict, state: str) -> dict:
    market_name = _lower(record.get("Market"))
    return dict(
        source_system              = "krama",
        state                      = state.lower(),
        date                       = _parse_date(record.get("Date")),
        market_name                = market_name,
        market_id                  = get_market_id(market_name, state),
        commodity_alias_lookup_id  = get_commodity_alias_lookup_id(_lower(record.get("Commodity"))),
        commodity_group            = _lower(record.get("Group")),
        commodity_name             = _lower(record.get("Commodity")),
        variety                    = _lower(record.get("Variety")),
        grade                      = _lower(record.get("Grade")),
        arrival_quantity           = _to_float(record.get("Arrival")),
        min_price                  = _to_float(record.get("Min")),
        max_price                  = _to_float(record.get("Max")),
        modal_price                = _to_float(record.get("Modal")),
        source_url                 = sources["karnataka"]["url"],
        method                     = sources["karnataka"]["method"],
        source_name                = sources["karnataka"]["source_name"],
    )


def _norm_meghalaya(record: dict, state: str) -> dict:
    market_name = _lower(record.get("market"))
    return dict(
        source_system              = "megamb",
        state                      = state.lower(),
        date                       = _parse_date(record.get("date")),
        market_name                = market_name,
        market_id                  = get_market_id(market_name, state),
        commodity_alias_lookup_id  = get_commodity_alias_lookup_id(_lower(record.get("commodity_name"))),
        commodity_group            = _lower(record.get("Group Name")),
        commodity_name             = _lower(record.get("commodity_name")),
        variety                    = _lower(record.get("Variety")),
        grade                      = _lower(record.get("Grade")),
        arrival_quantity           = _to_float(record.get("arrival_quintals")),
        min_price                  = _to_float(record.get("min_price_rs_per_quintal")),
        max_price                  = _to_float(record.get("max_price_rs_per_quintal")),
        modal_price                = _to_float(record.get("Modal")),
        source_url                 = sources["meghalaya"]["url"],
        method                     = sources["meghalaya"]["method"],
        source_name                = sources["meghalaya"]["source_name"],
    )


def _norm_nagaland(record: dict, state: str) -> dict:
    market_name = _lower(record.get("Market"))
    return dict(
        source_system              = "commodity_online",
        state                      = state.lower(),
        date                       = _parse_date(record.get("Arrival Date")),
        market_name                = market_name,
        market_id                  = get_market_id(market_name, state),
        commodity_alias_lookup_id  = get_commodity_alias_lookup_id(_lower(record.get("commodity_name"))),
        commodity_group            = None,
        commodity_name             = _lower(record.get("Commodity")),
        variety                    = _lower(record.get("Variety")),
        grade                      = None,
        arrival_quantity           = None,
        min_price                  = _to_float(record.get("Min Price")),
        max_price                  = _to_float(record.get("Max Price")),
        modal_price                = _to_float(record.get("Avg price")),
        source_url                 = sources["nagaland"]["url"],
        method                     = sources["nagaland"]["method"],
        source_name                = sources["nagaland"]["source_name"],
    )


def _norm_maharashtra(record: dict, state: str) -> dict:
    market_name = _lower(record.get("Market"))
    return dict(
        source_system              = "msamb",
        state                      = state.lower(),
        date                       = _parse_date(record.get("date")),
        market_name                = market_name,
        market_id                  = get_market_id(market_name, state),
        commodity_alias_lookup_id  = get_commodity_alias_lookup_id(_lower(record.get("commodity"))),
        commodity_group            = None,
        commodity_name             = _lower(record.get("commodity")),
        variety                    = _lower(record.get("variety")),
        grade                      = None,
        arrival_quantity           = _to_float(record.get("arrival")),
        min_price                  = _to_float(record.get("min_price")),
        max_price                  = _to_float(record.get("max_price")),
        modal_price                = _to_float(record.get("modal_price")),
        source_url                 = sources["maharashtra"]["url"],
        method                     = sources["maharashtra"]["method"],
        source_name                = sources["maharashtra"]["source_name"],
    )


def _norm_uttar_pradesh(record: dict, state: str) -> dict:
    return dict(
        source_system              = "up_krishi",
        state                      = state.lower(),
        date                       = _parse_date(record.get("Date")),
        market_name                = None,
        market_id                  = None,   # UP records carry no market name
        commodity_alias_lookup_id  = get_commodity_alias_lookup_id(_lower(record.get("ProductName"))),
        commodity_group            = _lower(record.get("MainProductName")),
        commodity_name             = _lower(record.get("ProductName")),
        variety                    = None,
        grade                      = None,
        arrival_quantity           = _to_float(record.get("aavakRate")),
        min_price                  = None,
        max_price                  = None,
        modal_price                = None,
        source_url                 = sources["uttar_pradesh"]["url"],
        method                     = sources["uttar_pradesh"]["method"],
        source_name                = sources["uttar_pradesh"]["source_name"],
    )


def _norm_punjab(record: dict, state: str) -> dict:
    market_name = _lower(record.get("BranchName"))
    return dict(
        source_system              = "emandikaran",
        state                      = state.lower(),
        date                       = _parse_date(record.get("EntryDate")),
        market_name                = market_name,
        market_id                  = get_market_id(market_name, state),
        commodity_alias_lookup_id  = get_commodity_alias_lookup_id(_lower(record.get("CommodityName"))),
        commodity_group            = None,
        commodity_name             = _lower(record.get("CommodityName")),
        variety                    = None,
        grade                      = None,
        arrival_quantity           = _to_float(record.get("Quantity")),
        min_price                  = _to_float(record.get("Minprice")),
        max_price                  = _to_float(record.get("MaxPrice")),
        modal_price                = _to_float(record.get("ModalPrice")),
        source_url                 = sources["punjab"]["url"],
        method                     = sources["punjab"]["method"],
        source_name                = sources["punjab"]["source_name"],
    )


def _norm_agmarknet(record: dict, state: str) -> dict:
    # Field name precedence (newest → oldest):
    #   1. data.gov.in new API (all lowercase):  state, market, commodity, arrival_date, ...
    #   2. data.gov.in legacy PascalCase:        State, Market, Commodity, Arrival_Date, ...
    #   3. old agmarknet API names:              reported_date, cmdt_name, as_on_price, ...
    record_state = (
        record.get("state") or record.get("State") or state
    )
    market_name = _lower(
        record.get("market") or record.get("Market")
    )
    commodity_name = _lower(
        record.get("commodity") or record.get("Commodity") or record.get("cmdt_name")
    )
    return dict(
        source_system              = "agmarknet",
        state                      = _lower(record_state),
        date                       = _parse_date(
            record.get("arrival_date") or record.get("Arrival_Date") or record.get("reported_date")
        ),
        market_name                = market_name,
        market_id                  = get_market_id(market_name, record_state),
        commodity_alias_lookup_id  = get_commodity_alias_lookup_id(commodity_name),
        commodity_group            = _lower(record.get("cmdt_grp_name")),
        commodity_name             = commodity_name,
        variety                    = _lower(
            record.get("variety") or record.get("Variety")
        ),
        grade                      = _lower(
            record.get("grade") or record.get("Grade")
        ),
        arrival_quantity           = _to_float(record.get("as_on_arrival")),
        min_price                  = _to_float(
            record.get("min_price") or record.get("Min_Price")
        ),
        max_price                  = _to_float(
            record.get("max_price") or record.get("Max_Price")
        ),
        modal_price                = _to_float(
            record.get("modal_price") or record.get("Modal_Price") or record.get("as_on_price")
        ),
        source_url                 = sources["agmarknet"]["url"],
        method                     = sources["agmarknet"]["method"],
        source_name                = sources["agmarknet"]["source_name"],
    )


def _norm_andhra_pradesh(record: dict, state: str) -> dict:
    market_name = _lower(record.get("mandi"))
    ap_source = sources.get("andhra_pradesh", {})
    return dict(
        source_system              = "agriculture.ap.gov.in",
        state                      = state.lower(),
        date                       = _parse_date(record.get("tranDate")),
        market_name                = market_name,
        market_id                  = get_market_id(market_name, state),
        commodity_alias_lookup_id  = get_commodity_alias_lookup_id(_lower(record.get("commodity"))),
        commodity_group            = None,
        commodity_name             = _lower(record.get("commodity")),
        variety                    = _lower(record.get("variety")),
        grade                      = None,
        arrival_quantity           = _to_float(record.get("arrivalQty")),
        min_price                  = _to_float(record.get("minPrice")),
        max_price                  = _to_float(record.get("maxPrice")),
        modal_price                = _to_float(record.get("modalPrice")),
        source_url                 = ap_source.get("url", "https://agriculture.ap.gov.in/staging/api/emarket/getMarketPriceData"),
        method                     = ap_source.get("method", "external_apis"),
        source_name                = ap_source.get("source_name", "state_level_website"),
    )


STATE_NORMALISERS = {
    "Karnataka":      _norm_karnataka,
    "Meghalaya":      _norm_meghalaya,
    "Nagaland":       _norm_nagaland,
    "Maharashtra":    _norm_maharashtra,
    "Uttar Pradesh":  _norm_uttar_pradesh,
    "Punjab":         _norm_punjab,
    "agmarknet":      _norm_agmarknet,
    "Andhra Pradesh": _norm_andhra_pradesh,
}


# ─────────────────────────────────────────────
# SPLIT: flat doc → (master_doc, price_doc)
# ─────────────────────────────────────────────

# Fields that belong to the master/dimension table
MASTER_FIELDS = {
    "source_system", "state", "market_name", "market_id",
    "commodity_alias_lookup_id", "commodity_group", "commodity_name",
    "variety", "grade", "source_url", "source_name", "method",
}

# Fields that belong to the price fact table
# market_id and commodity_alias_lookup_id are also written here so that
# price records can be queried independently without a join.
PRICE_FIELDS = {
    "date", "arrival_quantity", "min_price", "max_price",
    "modal_price", "ingested_at", "market_id", "commodity_alias_lookup_id",
}

# Compound key that uniquely identifies a market+commodity combo
MASTER_KEY_FIELDS = ("source_system", "state", "market_name", "commodity_name", "variety")


def split_document(doc: dict) -> tuple[dict, dict]:
    """
    Split a flat normalised document into:
      - master_doc : static meta fields (written once)
      - price_doc  : price + quantity fields (written every 2 hours)
    """
    master_doc = {k: doc[k] for k in MASTER_FIELDS if k in doc}
    price_doc  = {k: doc[k] for k in PRICE_FIELDS  if k in doc}
    return master_doc, price_doc


# ─────────────────────────────────────────────
# CORE PIPELINE
# ─────────────────────────────────────────────

def normalise_all(raw_data: dict) -> list[dict]:
    """
    raw_data : { "Karnataka": {"success": True, "data": [...]}, ... }
    Returns   : list of unified flat documents (splitting happens at upload time).
    """
    now = datetime.now(tz=timezone.utc)
    documents = []
    skipped_by_state: dict[str, int] = {}
    skipped_samples: list[str] = []

    for state_key, payload in raw_data.items():
        if not payload.get("success"):
            print(f"[SKIP] {state_key} — success=False")
            continue

        records    = payload.get("data", [])
        normaliser = STATE_NORMALISERS.get(state_key)

        if normaliser is None:
            print(f"[WARN] No normaliser for state '{state_key}', skipping.")
            continue

        for rec in records:
            try:
                doc = normaliser(rec, state=state_key)
                if _is_future_date(doc.get("date"), today=now):
                    skipped_by_state[state_key] = skipped_by_state.get(state_key, 0) + 1
                    if len(skipped_samples) < 5:
                        skipped_samples.append(
                            f"{state_key}: future date {doc.get('date')!r}"
                        )
                    continue
                doc["source_state"] = state_key
                doc["ingested_at"]  = now
                documents.append(doc)
            except Exception as exc:
                skipped_by_state[state_key] = skipped_by_state.get(state_key, 0) + 1
                if len(skipped_samples) < 5:
                    skipped_samples.append(f"{state_key}: {exc}")

    if skipped_by_state:
        total_skipped = sum(skipped_by_state.values())
        print(f"[WARN] Normalisation skipped {total_skipped} bad record(s): {skipped_by_state}")
        for sample in skipped_samples:
            print(f"       {sample}")

    print(f"[INFO] Normalised {len(documents)} documents across {len(raw_data)} states.")
    return documents


# ─────────────────────────────────────────────
# INDEX MANAGEMENT
# ─────────────────────────────────────────────

def _create_index_safe(collection, keys, **kwargs) -> None:
    name = kwargs.get("name", "<unnamed>")
    try:
        collection.create_index(keys, **kwargs)
    except OperationFailure as exc:
        print(f"[INFO] Index '{name}' already exists, skipping: {exc}")


def ensure_indexes(master_coll, price_coll) -> None:
    """Create indexes on both collections."""

    # ── markets_commodities ───────────────────────────────────────────
    # Unique compound key: the combination that identifies one master record
    _create_index_safe(
        master_coll,
        [
            ("source_system",  ASCENDING),
            ("state",          ASCENDING),
            ("market_name",    ASCENDING),
            ("commodity_name", ASCENDING),
            ("variety",        ASCENDING),
        ],
        unique=True,
        name="unique_market_commodity",
    )
    _create_index_safe(master_coll, [("state",          ASCENDING)], name="idx_mc_state")
    _create_index_safe(master_coll, [("commodity_name", ASCENDING)], name="idx_mc_commodity")

    # ── price_records ─────────────────────────────────────────────────
    # Unique: one price entry per master combo per date
    _create_index_safe(
        price_coll,
        [
            ("market_commodity_id", ASCENDING),
            ("date",                ASCENDING),
        ],
        unique=True,
        name="unique_price_entry",
    )
    _create_index_safe(price_coll, [("date",                ASCENDING)], name="idx_pr_date")
    _create_index_safe(price_coll, [("market_commodity_id", ASCENDING)], name="idx_pr_mc_id")

    print("[INFO] Indexes ensured on both collections.")


# ─────────────────────────────────────────────
# UPLOAD — two-collection normalised approach
# ─────────────────────────────────────────────

def upload_to_mongo(
    documents: list[dict],
    uri: str     = MONGO_URI,
    db_name: str = DB_NAME,
) -> None:
    """
    For each normalised document:
      1. Upsert into markets_commodities using $setOnInsert
         (meta fields written only on first insert, never overwritten).
      2. Use the returned _id as market_commodity_id in price_records.
      3. Upsert into price_records (one price row per combo per date).

    Null fields are stripped before writing to save storage.
    """
    if not documents:
        print("[INFO] Nothing to upload.")
        return

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print(f"[INFO] Connected to MongoDB at {uri}")

    db          = client[db_name]
    master_coll = db[MASTER_COLLECTION]
    price_coll  = db[PRICE_COLLECTION]

    ensure_indexes(master_coll, price_coll)

    master_ops      = []   # bulk upserts for markets_commodities
    price_ops_meta  = []   # metadata needed to build price ops after bulk

    skipped = 0

    for doc in documents:
        master_doc, price_doc = split_document(doc)

        # ── Build the compound lookup key ──────────────────────────────
        filter_key = {
            field: (master_doc.get(field) or "").strip().lower() or None
            for field in MASTER_KEY_FIELDS
        }

        # Skip degenerate records where every key field is null
        if not any(filter_key.values()):
            skipped += 1
            continue

        # Normalise strings for static identity fields (written only on first insert).
        # Strip nulls here because these fields must have real values.
        static_fields = {
            "source_system", "state", "market_name", "commodity_group",
            "commodity_name", "variety", "grade",
            "source_url", "source_name", "method",
        }
        set_on_insert = _strip_nulls({
            k: (v.strip().lower() if isinstance(v, str) else v)
            for k, v in master_doc.items()
            if k in static_fields
        })

        # market_id and commodity_alias_lookup_id go into $set so they are
        # refreshed on every run and always present — even as null — so every
        # document has a consistent schema regardless of lookup success.
        set_always = {
            "market_id":                master_doc.get("market_id"),
            "commodity_alias_lookup_id": master_doc.get("commodity_alias_lookup_id"),
        }

        master_ops.append(UpdateOne(
            filter_key,
            {
                "$set":         set_always,
                "$setOnInsert": set_on_insert,
            },
            upsert=True,
        ))

        # Store filter_key + price_doc so we can look up _id after bulk write
        price_ops_meta.append((filter_key, price_doc))

    if skipped:
        print(f"[WARN] Skipped {skipped} degenerate record(s) with all-null key fields.")

    # ── Step 1: bulk upsert master records ────────────────────────────
    if master_ops:
        try:
            result = master_coll.bulk_write(master_ops, ordered=False)
            print(f"[OK] markets_commodities — upserted: {result.upserted_count}, "
                  f"matched: {result.matched_count}")
        except BulkWriteError as bwe:
            n_err = len(bwe.details.get("writeErrors", []))
            print(f"[WARN] markets_commodities bulk write had {n_err} error(s).")
            for err in bwe.details.get("writeErrors", [])[:5]:
                print(f"       {err}")

    # ── Step 2: batch-resolve ALL master _ids in ONE query ────────────
    # Build a composite string key → (filter_key, [price_docs]) map
    # so we can match returned documents back to their price records.
    def _combo_key(fk: dict) -> str:
        """Deterministic string key from a filter dict."""
        return "||".join(str(fk.get(f) or "") for f in MASTER_KEY_FIELDS)

    # Group price_ops_meta by combo key (multiple price docs can share one master)
    combo_map: dict[str, dict]       = {}   # combo_key → filter_key
    price_map: dict[str, list[dict]] = {}   # combo_key → list of price_docs

    for filter_key, price_doc in price_ops_meta:
        ck = _combo_key(filter_key)
        combo_map[ck] = filter_key
        price_map.setdefault(ck, []).append(price_doc)

    # Fetch all matching master docs in a SINGLE round trip using $or
    or_filters   = list(combo_map.values())
    master_cursor = master_coll.find(
        {"$or": or_filters},
        projection={f: 1 for f in MASTER_KEY_FIELDS} | {"_id": 1},
    )

    # Build  combo_key → ObjectId  lookup dict from the results
    id_lookup: dict[str, object] = {}
    for mdoc in master_cursor:
        ck = _combo_key(mdoc)
        id_lookup[ck] = mdoc["_id"]

    print(f"[INFO] Resolved {len(id_lookup)} master _ids in one query "
          f"(out of {len(combo_map)} unique combos).")

    # ── Step 3: build price upserts using the in-memory id_lookup ─────
    price_ops = []
    now_str   = datetime.now(tz=timezone.utc).isoformat()
    unresolved = 0

    for ck, pdocs in price_map.items():
        mc_id = id_lookup.get(ck)
        if mc_id is None:
            unresolved += 1
            continue

        for price_doc in pdocs:
            date_val = price_doc.get("date")
            if not date_val:
                continue

            # Keep null fields explicit so documents have a consistent schema.
            # min_price / max_price / market_id / commodity_alias_lookup_id
            # must appear as null rather than be absent from the document.
            clean_price = {
                "market_commodity_id":       mc_id,
                "date":                      date_val,
                "market_id":                 price_doc.get("market_id"),
                "commodity_alias_lookup_id": price_doc.get("commodity_alias_lookup_id"),
                "arrival_quantity":          price_doc.get("arrival_quantity"),
                "min_price":                 price_doc.get("min_price"),
                "max_price":                 price_doc.get("max_price"),
                "modal_price":               price_doc.get("modal_price"),
                "ingested_at":               price_doc.get("ingested_at", now_str),
            }

            # Skip records where all three price fields are null — nothing useful to store.
            if (clean_price["modal_price"] is None
                    and clean_price["min_price"] is None
                    and clean_price["max_price"] is None):
                continue

            price_ops.append(UpdateOne(
                {"market_commodity_id": mc_id, "date": date_val},
                {"$set": clean_price},
                upsert=True,
            ))

    if unresolved:
        print(f"[WARN] {unresolved} combo(s) could not be resolved to a master _id — skipped.")

    # ── Step 4: bulk upsert price records ─────────────────────────────
    if price_ops:
        try:
            result = price_coll.bulk_write(price_ops, ordered=False)
            print(f"[OK] price_records — upserted: {result.upserted_count}, "
                  f"matched (updated): {result.matched_count}")
        except BulkWriteError as bwe:
            n_err = len(bwe.details.get("writeErrors", []))
            print(f"[WARN] price_records bulk write had {n_err} error(s).")
            for err in bwe.details.get("writeErrors", [])[:5]:
                print(f"       {err}")

    client.close()
    print("[INFO] Connection closed.")


# ─────────────────────────────────────────────
# COMMODITY LOOKUP  (singleton client + cache)
# ─────────────────────────────────────────────

_lookup_client = None
_lookup_coll   = None
_commodity_id_cache: dict[str, str | None] = {}
_commodity_alias_lookup_id_cache: dict[str, object | None] = {}


def _get_lookup_collection(
    uri: str       = MONGO_URI,
    db_name: str   = DB_NAME,
    coll_name: str = "commodity_alias_lookup",
):
    global _lookup_client, _lookup_coll
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
    """Return crop_master_id from commodity_alias_lookup for the given name."""
    if not isinstance(commodity_name, str) or not commodity_name.strip():
        return None

    key = commodity_name.strip().lower()

    if key in _commodity_id_cache:
        return _commodity_id_cache[key]

    try:
        coll = _get_lookup_collection()
        if coll is None:
            _commodity_id_cache[key] = None
            return None

        doc = coll.find_one(
            {"aliases": key, "crop_master_id": {"$nin": [None, ""]}},
            projection={"crop_master_id": 1, "_id": 0},
        )
        if doc:
            result = doc["crop_master_id"]
            _commodity_id_cache[key] = result
            return result

        pattern  = re.compile(re.escape(key), re.IGNORECASE)
        fallback = coll.find_one(
            {"aliases": {"$regex": pattern}, "crop_master_id": {"$nin": [None, ""]}},
            projection={"crop_master_id": 1, "_id": 0},
        )
        result = fallback.get("crop_master_id") if fallback else None
        _commodity_id_cache[key] = result
        return result
    except Exception as exc:
        print(f"[WARN] crop_master_id lookup failed for '{key}': {exc}")
        _commodity_id_cache[key] = None
        return None


def get_commodity_alias_lookup_id(commodity_name: str) -> object | None:
    """
    Return the ObjectId (_id) of the commodity_alias_lookup document
    that matches the given commodity name via its aliases array.
    This is distinct from get_commodity_id() which returns crop_master_id.
    """
    if not isinstance(commodity_name, str) or not commodity_name.strip():
        return None

    key = commodity_name.strip().lower()

    if key in _commodity_alias_lookup_id_cache:
        return _commodity_alias_lookup_id_cache[key]

    try:
        coll = _get_lookup_collection()
        if coll is None:
            _commodity_alias_lookup_id_cache[key] = None
            return None

        # 1. Exact alias match
        doc = coll.find_one(
            {"aliases": key},
            projection={"_id": 1},
        )
        if doc:
            result = doc["_id"]
            _commodity_alias_lookup_id_cache[key] = result
            return result

        # 2. Case-insensitive regex fallback
        pattern  = re.compile(re.escape(key), re.IGNORECASE)
        fallback = coll.find_one(
            {"aliases": {"$regex": pattern}},
            projection={"_id": 1},
        )
        result = fallback["_id"] if fallback else None
        _commodity_alias_lookup_id_cache[key] = result
        return result
    except Exception as exc:
        print(f"[WARN] commodity_alias_lookup _id lookup failed for '{key}': {exc}")
        _commodity_alias_lookup_id_cache[key] = None
        return None


# ─────────────────────────────────────────────
# MARKET LOOKUP  (singleton client + cache)
# Looks up ALL_MANDI_COLLECTION by `name` first,
# then falls back to the `aliases` array.
# ─────────────────────────────────────────────

_market_lookup_client = None
_market_lookup_coll   = None
_market_id_cache: dict[str, object | None] = {}


def _get_market_lookup_collection(
    uri: str       = MONGO_URI,
    db_name: str   = DB_NAME,
    coll_name: str = ALL_MANDI_COLLECTION,
):
    global _market_lookup_client, _market_lookup_coll
    if not (isinstance(uri, str) and uri.strip() and
            isinstance(db_name, str) and db_name.strip() and
            isinstance(coll_name, str) and coll_name.strip()):
        print("[WARN] Market lookup DB not configured — market_id lookups disabled.")
        return None
    if _market_lookup_coll is None:
        _market_lookup_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        _market_lookup_coll   = _market_lookup_client[db_name][coll_name]
        print("[INFO] Market lookup client connected.")
    return _market_lookup_coll


def get_market_id(market_name: str, state: str | None = None) -> object | None:
    """
    Resolve a market name to its MongoDB _id from ALL_MANDI_COLLECTION.

    Lookup order:
      1. Exact match on `name`  (case-insensitive, optionally filtered by state)
      2. Exact match on `aliases` array element
      3. Regex fallback on `aliases` (for partial / title-case mismatches)

    Results are cached in-process to avoid repeated round trips.
    Returns the document's ObjectId, or None if not found.
    """
    if not isinstance(market_name, str) or not market_name.strip():
        return None

    key = market_name.strip().lower()
    state_lower = state.strip().lower() if isinstance(state, str) and state.strip() else None
    cache_key = f"{state_lower}||{key}" if state_lower else key

    if cache_key in _market_id_cache:
        return _market_id_cache[cache_key]

    try:
        coll = _get_market_lookup_collection()
        if coll is None:
            _market_id_cache[cache_key] = None
            return None

        state_filter = {"state": {"$regex": re.compile(re.escape(state_lower), re.IGNORECASE)}} \
            if state_lower else {}

        # 1. Exact match on `name`
        doc = coll.find_one(
            {"name": {"$regex": re.compile(f"^{re.escape(key)}$", re.IGNORECASE)},
             **state_filter},
            projection={"_id": 1},
        )
        if doc:
            _market_id_cache[cache_key] = doc["_id"]
            return doc["_id"]

        # 2. Exact element match on `aliases`
        doc = coll.find_one(
            {"aliases": {"$regex": re.compile(f"^{re.escape(key)}$", re.IGNORECASE)},
             **state_filter},
            projection={"_id": 1},
        )
        if doc:
            _market_id_cache[cache_key] = doc["_id"]
            return doc["_id"]

        # 3. Regex / partial match on `aliases` (looser fallback)
        pattern  = re.compile(re.escape(key), re.IGNORECASE)
        fallback = coll.find_one(
            {"aliases": {"$regex": pattern}, **state_filter},
            projection={"_id": 1},
        )
        result = fallback["_id"] if fallback else None
        _market_id_cache[cache_key] = result
        return result

    except Exception as exc:
        print(f"[WARN] market_id lookup failed for '{key}' (state={state_lower}): {exc}")
        _market_id_cache[cache_key] = None
        return None


# ─────────────────────────────────────────────
# REPAIR — Andhra Pradesh ISO dates swapped by dateutil dayfirst
# ─────────────────────────────────────────────

def repair_ap_iso_dayfirst_dates(*, dry_run: bool = True) -> dict:
    """
    Fix agriculture.ap.gov.in prices stored as YYYY-DD-MM.

    Only touches rows with date > today.  dateutil.parse('2026-08-12',
    dayfirst=True) → 2026-12-08; swap month/day back.  If the target
    date already exists, delete the future duplicate instead of
    flipping already-correct history.
    """
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    db = client[DB_NAME]
    master_coll = db[MASTER_COLLECTION]
    price_coll = db[PRICE_COLLECTION]

    today = datetime.now(timezone.utc)
    ap_ids = [d["_id"] for d in master_coll.find(
        {"source_system": "agriculture.ap.gov.in"},
        {"_id": 1},
    )]
    future_q = {
        "market_commodity_id": {"$in": ap_ids},
        "date": {"$gt": today},
    }
    future_n = price_coll.count_documents(future_q) if ap_ids else 0
    print(
        f"[INFO] AP masters={len(ap_ids)} future_prices={future_n} "
        f"dry_run={dry_run}"
    )
    if not ap_ids or future_n == 0:
        print("[INFO] No future AP dates — skip repair (already fixed).")
        client.close()
        return {
            "ap_masters": len(ap_ids),
            "future_prices": future_n,
            "would_move": 0,
            "moved": 0,
            "deleted_still_future": 0,
            "dry_run": dry_run,
        }

    cursor = price_coll.find(
        future_q,
        {"_id": 1, "market_commodity_id": 1, "date": 1},
    )
    moves: list[tuple] = []
    to_delete: list = []
    for row in cursor:
        old = row.get("date")
        if old is None:
            continue
        new = _swap_dayfirst_iso_misparse(old)
        if _is_future_date(new, today=today) or _as_utc(new).date() == _as_utc(old).date():
            to_delete.append(row["_id"])
            continue
        exists = price_coll.find_one(
            {
                "market_commodity_id": row["market_commodity_id"],
                "date": new,
            },
            {"_id": 1},
        )
        if exists:
            to_delete.append(row["_id"])
        else:
            moves.append((row["_id"], row["market_commodity_id"], old, new))

    print(
        f"[INFO] {len(moves)} AP future price(s) to swap, "
        f"{len(to_delete)} to delete."
    )
    moved = 0
    deleted = 0
    if not dry_run and (moves or to_delete):
        park_ops = []
        final_ops = []
        for pid, mc_id, old, new in moves:
            parked = _as_utc(old).replace(year=_as_utc(old).year + 100)
            park_ops.append(UpdateOne({"_id": pid}, {"$set": {"date": parked}}))
            final_ops.append(UpdateOne(
                {"_id": pid},
                {"$set": {"date": new, "market_commodity_id": mc_id}},
            ))
        if park_ops:
            price_coll.bulk_write(park_ops, ordered=False)
        if final_ops:
            res = price_coll.bulk_write(final_ops, ordered=False)
            moved = res.modified_count

        leftover = list(price_coll.find(future_q, {"_id": 1}))
        drop_ids = list(to_delete) + [x["_id"] for x in leftover]
        if drop_ids:
            deleted = price_coll.delete_many(
                {"_id": {"$in": drop_ids}}
            ).deleted_count
            print(f"[INFO] Deleted {deleted} AP future duplicate(s).")

    elif dry_run:
        print("[DRY-RUN] No writes. Re-run with --execute to apply.")

    client.close()
    return {
        "ap_masters": len(ap_ids),
        "future_prices": future_n,
        "would_move": len(moves),
        "moved": moved,
        "deleted_still_future": deleted,
        "dry_run": dry_run,
        "samples": [
            {"old": o, "new": n}
            for _, _, o, n in moves[:8]
        ],
    }


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Normalise/upload mandi data, or repair AP ISO dates"
    )
    parser.add_argument(
        "--repair-ap-dates",
        action="store_true",
        help="Fix agriculture.ap.gov.in dates swapped by dateutil dayfirst",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="With --repair-ap-dates, actually write (default is dry-run)",
    )
    parser.add_argument(
        "--input",
        default="final_data.json",
        help="JSON payload for a normal upload run",
    )
    args = parser.parse_args()

    if args.repair_ap_dates:
        report = repair_ap_iso_dayfirst_dates(dry_run=not args.execute)
        print(json.dumps(report, indent=2, default=str))
    else:
        with open(args.input, "r") as f:
            RAW_DATA = json.load(f)
        docs = normalise_all(RAW_DATA)
        upload_to_mongo(docs)
