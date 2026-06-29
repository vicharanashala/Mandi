"""
add_market.py
-------------
Reads  googleapi_geolocation_results_final.xlsx  and converts every row
into a structured document ready for inserting into MongoDB.

Excel columns expected
----------------------
  [0] State
  [1] Market Names (APMC/PMY/SMY)
  [2] District
  [3] Postal Code
  [4] Latitude
  [5] Longitude

Document schema (GeoJSON 2dsphere)
-----------------------------------
  location.coordinates : [longitude, latitude]  ← GeoJSON order

Name resolution logic
---------------------
  - Split the "Market Names" cell by "/" and strip whitespace.
  - Normalise every token: Title-Case each word, preserve known acronyms
    (APMC, PMY, SMY) in uppercase.
  - If any token contains "APMC", the first APMC token becomes `name`;
    everything else (in original order) becomes `aliases`.
  - If no token contains "APMC", the first token is `name`;
    the rest become `aliases`.
  - Duplicate / empty aliases are removed.

Usage
-----
  pip install openpyxl pymongo
  python add_market.py
"""

import json
import re
import sys
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()
try:
    import openpyxl
except ImportError:
    sys.exit("openpyxl is not installed.  Run: pip install openpyxl")

try:
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import BulkWriteError
except ImportError:
    sys.exit("pymongo is not installed.  Run: pip install pymongo")

# ── paths ──────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
XLSX_PATH    = BASE_DIR / "googleapi_geolocation_results_final.xlsx"
JSON_PATH    = BASE_DIR / "markets_geo.json"   # optional preview output

# ── MongoDB config — FILL IN BEFORE RUNNING ───────────────────────────────────
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME     = os.getenv("MANDI_DB_NAME").strip()
COLLECTION  = os.getenv("ALL_MANDI_COLLECTION")


# ── helpers ────────────────────────────────────────────────────────────────────

def title_case(text: str) -> str:
    """Title-case a token while keeping APMC / PMY / SMY in uppercase."""
    ACRONYMS = {"APMC", "PMY", "SMY"}
    words  = text.strip().split()
    result = []
    for word in words:
        core   = word.rstrip(".,;:()/")
        suffix = word[len(core):]
        if core.upper() in ACRONYMS:
            result.append(core.upper() + suffix)
        else:
            result.append(core.capitalize() + suffix)
    return " ".join(result)


def contains_apmc(token: str) -> bool:
    return bool(re.search(r'\bAPMC\b', token, re.IGNORECASE))


def parse_market_names(raw: str):
    """
    Split, normalise, and return (name: str, aliases: list[str]).
    """
    raw    = raw.replace("\n", " ").replace("\r", " ")
    parts  = [p.strip() for p in raw.split("/") if p.strip()]
    tokens = [title_case(p) for p in parts]

    apmc_tokens     = [t for t in tokens if contains_apmc(t)]
    non_apmc_tokens = [t for t in tokens if not contains_apmc(t)]

    if apmc_tokens:
        name    = apmc_tokens[0]
        aliases = non_apmc_tokens + apmc_tokens[1:]
    else:
        name    = tokens[0] if tokens else ""
        aliases = tokens[1:]

    # de-duplicate aliases, drop blank entries, keep original order
    seen          = {name}
    clean_aliases = []
    for a in aliases:
        if a and a not in seen:
            clean_aliases.append(a)
            seen.add(a)

    return name, clean_aliases


def _to_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def _clean_str(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


# ── read xlsx ──────────────────────────────────────────────────────────────────

def read_xlsx(xlsx_path: Path) -> list[dict]:
    if not xlsx_path.exists():
        sys.exit(f"Excel file not found: {xlsx_path}")

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    rows       = ws.iter_rows(values_only=True)
    header_row = next(rows)

    # locate columns dynamically from the header
    header_lower = [
        str(h).lower().replace("\n", " ").strip() if h else ""
        for h in header_row
    ]

    def find_col(keyword: str, fallback: int) -> int:
        return next(
            (i for i, h in enumerate(header_lower) if keyword in h),
            fallback,
        )

    col_state    = find_col("state",        0)
    col_market   = find_col("market names", 1)
    col_district = find_col("district",     2)
    col_postcode = find_col("postal code",  3)
    col_lat      = find_col("latitude",     4)
    col_lon      = find_col("longitude",    5)

    markets = []
    skipped = 0

    for row in rows:
        state_raw    = row[col_state]    if col_state    < len(row) else None
        market_raw   = row[col_market]   if col_market   < len(row) else None
        district_raw = row[col_district] if col_district < len(row) else None
        postcode_raw = row[col_postcode] if col_postcode < len(row) else None
        lat_raw      = row[col_lat]      if col_lat      < len(row) else None
        lon_raw      = row[col_lon]      if col_lon      < len(row) else None

        # skip completely empty rows
        if not state_raw and not market_raw:
            skipped += 1
            continue

        if not market_raw:
            skipped += 1
            continue

        state         = str(state_raw).replace("\n", " ").strip().title() if state_raw else ""
        name, aliases = parse_market_names(str(market_raw))

        lat = _to_float(lat_raw)
        lon = _to_float(lon_raw)
        location = (
            {"type": "Point", "coordinates": [lon, lat]}
            if lon is not None and lat is not None
            else None
        )

        markets.append({
            "state":    state,
            "name":     name,
            "district": _clean_str(district_raw),
            "postcode": _clean_str(postcode_raw),
            "aliases":  aliases,
            "location": location,
        })

    wb.close()
    print(f"✓ {len(markets)} markets parsed  (skipped {skipped} empty rows)")
    return markets


# ── MongoDB upload ─────────────────────────────────────────────────────────────

def upload_to_mongo(markets: list[dict]) -> None:
    """
    Bulk-upsert markets into MongoDB.
    Upsert key  : { state, name }          ← never overwritten on match
    $set        : district, lat, lon, postcode, aliases  ← refreshed every run
    $setOnInsert: all other fields         ← written only on first insert
    """
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    try:
        client.admin.command("ping")
        print(f"✓ Connected to MongoDB  →  db={DB_NAME!r}, coll={COLLECTION!r}")
    except Exception as exc:
        sys.exit(f"[ERROR] Cannot connect to MongoDB: {exc}")

    col = client[DB_NAME][COLLECTION]

    # useful indexes
    try:
        col.create_index(
            [("state", 1), ("name", 1)],
            unique=True,
            name="unique_state_name",
        )
        col.create_index([("district",  1)],        name="idx_district")
        col.create_index([("state",     1)],        name="idx_state")
        col.create_index([("location",  "2dsphere")], name="idx_location_2dsphere")
    except Exception:
        pass  # indexes already exist

    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc)
    ops = []

    for doc in markets:
        filter_key = {"state": doc["state"], "name": doc["name"]}

        ops.append(UpdateOne(
            filter_key,
            {
                "$set": {
                    "district":  doc["district"],
                    "postcode":  doc["postcode"],
                    "aliases":   doc["aliases"],
                    "location":  doc["location"],
                    "updatedAt": now,
                },
                "$setOnInsert": {
                    "state":     doc["state"],
                    "name":      doc["name"],
                    "createdAt": now,
                },
            },
            upsert=True,
        ))

    # bulk write in batches of 1 000
    total_inserted = total_matched = 0
    BATCH = 1_000
    for i in range(0, len(ops), BATCH):
        batch = ops[i : i + BATCH]
        try:
            r = col.bulk_write(batch, ordered=False)
            total_inserted += r.upserted_count
            total_matched  += r.matched_count
        except BulkWriteError as bwe:
            n = len(bwe.details.get("writeErrors", []))
            print(f"[WARN] Batch {i // BATCH + 1}: {n} write error(s)")

    print(f"✓ Upload done — inserted: {total_inserted}, updated: {total_matched}")
    client.close()


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    markets = read_xlsx(XLSX_PATH)

    if not markets:
        print("[WARN] No records found — nothing to do.")
        return

    # save a JSON preview (optional)
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(markets, f, ensure_ascii=False, indent=2)
    print(f"✓ JSON preview saved to {JSON_PATH}")

    # ── preview first 3 entries ────────────────────────────────────────
    print("\n── Sample (first 3) ─────────────────────────────────────────")
    for m in markets[:3]:
        print(json.dumps(m, ensure_ascii=False, indent=2))

    # ── upload to MongoDB ──────────────────────────────────────────────
    upload_to_mongo(markets)


if __name__ == "__main__":
    main()
