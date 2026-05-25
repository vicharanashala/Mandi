"""
mandi_mcp_server.py

FastMCP server exposing tools to query APMC mandi price data from MongoDB.

Schema (per document):
    state, district, market, date, commodity, commodity_group,
    variety, grade, unit, arrival_qty,
    min_price, max_price, modal_price, wholesale_rate, retail_price,
    as_on_price, msp_price, trend,
    one_day_ago_price, two_day_ago_price,
    one_day_ago_arrival, two_day_ago_arrival,
    source_url, source_name, method, source_state, ingested_at

Tools:
    1. get_mandi_prices   — query by any combination of the schema fields
    2. get_unique_markets — list unique markets (optionally filtered by state)
"""

import asyncio
import json
import logging
import os

from dotenv import load_dotenv
from fastmcp import FastMCP
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dateutil import parser as date_parser

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

MONGO_URI       = os.getenv("MONGO_URI")
DB_NAME         = os.getenv("MANDI_DB_NAME", "mandi_db")
COLLECTION_NAME = os.getenv("MANDI_COLLECTION", "market_prices")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mandi-mcp")

mcp = FastMCP(
    name="mandi-price-server",
    instructions=(
        "Query APMC mandi commodity prices from MongoDB. "
        "Supports any combination of filters: commodity, market, state, "
        "district, date, commodity_group, variety, grade, and source_name."
    ),
)

# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------

def _get_collection():
    if not MONGO_URI:
        raise ValueError("MONGO_URI is not set in environment.")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6_000)
    client.admin.command("ping")
    return client[DB_NAME][COLLECTION_NAME], client


# ---------------------------------------------------------------------------
# Date helper
# ---------------------------------------------------------------------------

def _parse_query_date(raw: str) -> str | None:
    """
    Try to normalise an arbitrary date string to YYYY-MM-DD for exact
    matching against the stored `date` field.
    Returns None if parsing fails (caller falls back to regex).
    """
    if not raw:
        return None
    try:
        dt = date_parser.parse(raw.strip(), dayfirst=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Core query helpers
# ---------------------------------------------------------------------------

def _build_regex(value: str) -> dict:
    """Case-insensitive regex filter for a single string value."""
    return {"$regex": value.strip(), "$options": "i"}


def _query_prices(
    commodity: str | None,
    market: str | None,
    state: str | None,
    district: str | None,
    date: str | None,
    commodity_group: str | None,
    variety: str | None,
    grade: str | None,
    source_name: str | None,
    limit: int,
) -> list[dict]:
    col, client = _get_collection()
    try:
        and_clauses: list[dict] = []

        # --- commodity (required by the tool, but kept optional here for reuse) ---
        if commodity:
            and_clauses.append({"commodity": _build_regex(commodity)})

        # --- optional filters ---
        if market:
            and_clauses.append({"market": _build_regex(market)})

        if state:
            and_clauses.append({"state": _build_regex(state)})

        if district:
            and_clauses.append({"district": _build_regex(district)})

        if commodity_group:
            and_clauses.append({"commodity_group": _build_regex(commodity_group)})

        if variety:
            and_clauses.append({"variety": _build_regex(variety)})

        if grade:
            and_clauses.append({"grade": _build_regex(grade)})

        if source_name:
            and_clauses.append({"source_name": _build_regex(source_name)})

        # --- date: prefer exact YYYY-MM-DD, fall back to regex ---
        if date:
            parsed = _parse_query_date(date)
            if parsed:
                and_clauses.append({"date": parsed})
            else:
                and_clauses.append({"date": _build_regex(date)})

        query = {"$and": and_clauses} if and_clauses else {}

        # Exclude internal Mongo / ingest metadata from the response
        projection = {"_id": 0, "ingested_at": 0}

        return list(col.find(query, projection).limit(limit))
    finally:
        client.close()


def _get_unique_markets_from_db(state: str | None = None) -> list[str]:
    col, client = _get_collection()
    try:
        pipeline: list[dict] = []

        if state:
            pipeline.append(
                {"$match": {"state": _build_regex(state)}}
            )

        pipeline += [
            {"$group": {"_id": None, "markets": {"$addToSet": "$market"}}},
            {"$project": {"_id": 0, "markets": 1}},
        ]

        result = list(col.aggregate(pipeline))
        if not result:
            return []
        return sorted([m for m in result[0].get("markets", []) if m])
    finally:
        client.close()


# ---------------------------------------------------------------------------
# FastMCP Tools
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_mandi_prices(
    commodity: str = "",
    market: str = "",
    state: str = "",
    district: str = "",
    date: str = "",
    commodity_group: str = "",
    variety: str = "",
    grade: str = "",
    source_name: str = "",
    limit: int = 50,
) -> str:
    """
    Fetch mandi (APMC) commodity prices from the database.

    All parameters are optional — supply any combination to narrow the search.
    At least one filter should be provided to get meaningful results.

    Args:
        commodity:       Commodity name, e.g. "onion", "tomato", "wheat".
        market:          Market / mandi name, e.g. "azadpur apmc", "yeotmal".
        state:           State name, e.g. "nct of delhi", "karnataka".
        district:        District name, e.g. "north west delhi".
        date:            Date filter. Accepts most formats:
                         "2026-05-23", "23/05/2026", "23 May 2026", etc.
        commodity_group: Commodity group, e.g. "vegetables", "cereals".
        variety:         Variety of the commodity, e.g. "local", "hybrid".
        grade:           Grade, e.g. "FAQ", "A".
        source_name:     Data source, e.g. "agmarknet", "nafed".
        limit:           Maximum records to return (default 50).
    """
    # Normalise empties to None
    def _v(s: str) -> str | None:
        return s.strip() or None

    commodity_val    = _v(commodity)
    market_val       = _v(market)
    state_val        = _v(state)
    district_val     = _v(district)
    date_val         = _v(date)
    comm_group_val   = _v(commodity_group)
    variety_val      = _v(variety)
    grade_val        = _v(grade)
    source_name_val  = _v(source_name)

    # Guard: require at least one filter
    if not any([
        commodity_val, market_val, state_val, district_val,
        date_val, comm_group_val, variety_val, grade_val, source_name_val,
    ]):
        return (
            "Error: Please provide at least one search parameter "
            "(commodity, market, state, district, date, commodity_group, "
            "variety, grade, or source_name)."
        )

    try:
        records = await asyncio.to_thread(
            _query_prices,
            commodity_val,
            market_val,
            state_val,
            district_val,
            date_val,
            comm_group_val,
            variety_val,
            grade_val,
            source_name_val,
            limit,
        )
    except ConnectionFailure as e:
        return f"MongoDB connection error: {e}"
    except Exception as e:
        return f"Query error: {e}"

    if not records:
        # Build a human-readable "no results" message listing all active filters
        active = []
        if commodity_val:    active.append(f"commodity='{commodity_val}'")
        if market_val:       active.append(f"market='{market_val}'")
        if state_val:        active.append(f"state='{state_val}'")
        if district_val:     active.append(f"district='{district_val}'")
        if date_val:         active.append(f"date='{date_val}'")
        if comm_group_val:   active.append(f"commodity_group='{comm_group_val}'")
        if variety_val:      active.append(f"variety='{variety_val}'")
        if grade_val:        active.append(f"grade='{grade_val}'")
        if source_name_val:  active.append(f"source_name='{source_name_val}'")
        return "No records found for " + ", ".join(active) + "."

    # Build header
    parts = []
    if commodity_val:   parts.append(f"'{commodity_val}'")
    if market_val:      parts.append(f"in '{market_val}'")
    if state_val:       parts.append(f"({state_val})")
    if district_val:    parts.append(f"district '{district_val}'")
    if date_val:        parts.append(f"on {date_val}")
    if comm_group_val:  parts.append(f"group '{comm_group_val}'")
    if variety_val:     parts.append(f"variety '{variety_val}'")
    if grade_val:       parts.append(f"grade '{grade_val}'")
    if source_name_val: parts.append(f"source '{source_name_val}'")

    header = f"Found {len(records)} record(s)" + (" for " if parts else "") + " ".join(parts)

    output = json.dumps(records, ensure_ascii=False, indent=2, default=str)
    return f"{header}:\n\n{output}"


@mcp.tool()
async def get_unique_markets(
    state: str = "",
) -> str:
    """
    List all unique market names stored in the mandi database.

    Args:
        state: Optionally filter by state name, e.g. "karnataka". Leave blank
               to return markets from all states.
    """
    state_val = state.strip() or None

    try:
        markets = await asyncio.to_thread(_get_unique_markets_from_db, state_val)
    except ConnectionFailure as e:
        return f"MongoDB connection error: {e}"
    except Exception as e:
        return f"Error fetching markets: {e}"

    if not markets:
        return "No markets found" + (f" for state '{state_val}'." if state_val else ".")

    body   = "\n".join(f"  {m}" for m in markets)
    header = f"Unique markets ({len(markets)} total)"
    if state_val:
        header += f" in state '{state_val}'"

    return f"{header}:\n{body}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # mcp.run(transport="sse", host="127.0.0.1", port=8000)
    mcp.run(transport="sse", host="0.0.0.0", port=8000)