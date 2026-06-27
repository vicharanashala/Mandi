"""
add_market.py
-------------
Converts the market.xlsx Excel file into a structured JSON file
ready for uploading into MongoDB.

Name resolution logic:
  - Split the "Market Names (APMC/PMY/SMY)" cell by "/" and strip whitespace.
  - Normalise every token: Title-Case each word.
  - If any token contains "APMC" (case-insensitive), the *first* such token
    becomes `name`; the rest (including non-APMC tokens) become `aliases`.
  - If no token contains "APMC", the first token becomes `name` and the
    rest become `aliases`.
  - Duplicate/empty aliases are removed.

Fields populated later (via OpenStreetMap API):
  district, lat, lon, postcode, bb_south, bb_north, bb_west, bb_east
"""

import json
import re
import sys
from pathlib import Path

try:
    import openpyxl
except ImportError:
    sys.exit("openpyxl is not installed. Run: pip install openpyxl")

# ── paths ──────────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).parent
XLSX_PATH = BASE_DIR / "market.xlsx"
JSON_PATH = BASE_DIR / "markets.json"

# ── helpers ────────────────────────────────────────────────────────────────────

def title_case(text: str) -> str:
    """
    Title-case a market name token while preserving known acronyms
    (APMC, PMY, SMY) in uppercase.
    """
    ACRONYMS = {"APMC", "PMY", "SMY"}
    words = text.strip().split()
    result = []
    for word in words:
        # strip trailing punctuation for comparison, but keep it
        core = word.rstrip(".,;:()/")
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
    Given the raw cell value (may contain "/" and newlines), return
    (name: str, aliases: list[str]).

    Rules:
      1. Split on "/" (and strip internal newlines).
      2. Title-case every token.
      3. If any token contains "APMC", the first APMC token is `name`;
         everything else (in original order) goes to `aliases`.
      4. If no token contains "APMC", the first token is `name`;
         the rest go to `aliases`.
    """
    # normalise newlines inside the cell
    raw = raw.replace("\n", " ").replace("\r", " ")
    parts = [p.strip() for p in raw.split("/") if p.strip()]
    tokens = [title_case(p) for p in parts]

    apmc_tokens     = [t for t in tokens if contains_apmc(t)]
    non_apmc_tokens = [t for t in tokens if not contains_apmc(t)]

    if apmc_tokens:
        name    = apmc_tokens[0]
        aliases = non_apmc_tokens + apmc_tokens[1:]
    else:
        name    = tokens[0]
        aliases = tokens[1:]

    # de-duplicate aliases, remove blank, keep order
    seen = {name}
    clean_aliases = []
    for a in aliases:
        if a and a not in seen:
            clean_aliases.append(a)
            seen.add(a)

    return name, clean_aliases


def parse_price_availability(raw) -> str:
    """Return 'yes' or 'no' from the 'Daily Market data available' column."""
    if raw is None:
        return "no"
    val = str(raw).strip().lower()
    return "yes" if val == "yes" else "no"


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    if not XLSX_PATH.exists():
        sys.exit(f"Excel file not found: {XLSX_PATH}")

    wb = openpyxl.load_workbook(XLSX_PATH, read_only=True, data_only=True)
    ws = wb.active

    rows = ws.iter_rows(values_only=True)
    header = next(rows)  # skip the header row

    # Identify column indices (0-based)
    header_lower = [str(h).lower().strip() if h else "" for h in header]
    try:
        col_state = header_lower.index("state")
    except ValueError:
        col_state = 0

    # The market-name column header contains a newline in the xlsx
    col_market = next(
        (i for i, h in enumerate(header_lower) if "market names" in h),
        1,
    )
    col_avail = next(
        (i for i, h in enumerate(header_lower) if "daily market data" in h),
        2,
    )

    markets = []
    skipped = 0

    for row_num, row in enumerate(rows, start=2):
        state_raw  = row[col_state]
        market_raw = row[col_market]
        avail_raw  = row[col_avail]

        # skip completely empty rows
        if not state_raw and not market_raw:
            skipped += 1
            continue

        state = str(state_raw).strip().title() if state_raw else ""
        if not market_raw:
            skipped += 1
            continue

        name, aliases = parse_market_names(str(market_raw))
        price_availability = parse_price_availability(avail_raw)

        doc = {
            "state":              state,
            "name":               name,
            "aliases":            aliases,
            "price_availability": price_availability,
            # ── to be filled via OpenStreetMap API ──
            "district":  None,
            "lat":       None,
            "lon":       None,
            "postcode":  None,
            "bb_south":  None,
            "bb_north":  None,
            "bb_west":   None,
            "bb_east":   None,
        }
        markets.append(doc)

    wb.close()

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(markets, f, ensure_ascii=False, indent=2)

    print(f"✓ {len(markets)} markets written to {JSON_PATH}")
    print(f"  (skipped {skipped} empty rows)")

    # ── preview a few entries ──────────────────────────────────────────────────
    print("\n── Sample output (first 5 entries) ──────────────────────────────")
    for m in markets[:5]:
        print(json.dumps(m, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
