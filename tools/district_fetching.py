"""
district_fetching.py
--------------------
Given an APMC/market name, normalize it and look up the district name
from the agmarknet_filters.json file.

JSON structure (relevant parts):
  data.market_data  -> list of { id, mkt_name, state_id, district_id }
  data.district_data -> list of { id, state_id, district_name }

Flow:
  1. Normalize the input APMC name (basic normalization: unicode, lowercase,
     punctuation removal).
  2. Strip common noise words ("apmc", "market", "mandi", etc.) from BOTH
     the input and the JSON market names so that e.g. "APMC Anand" and
     "Anand Market" both reduce to "anand" and match correctly.
  3. Find the best-matching market in market_data.
  4. Use its district_id to look up the district_name in district_data.
"""

import json
import os
import re
import sys
import unicodedata

# Add the parent directory to sys.path to allow importing from utils when run directly

from utils.maharashtra import mandi_code
# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_FILTERS_PATH = os.path.join(_THIS_DIR, "..", "agmarknet_filters.json")


# ---------------------------------------------------------------------------
# Load & index filter data (done once at import time)
# ---------------------------------------------------------------------------
def _load_filters() -> dict:
    with open(_FILTERS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


_FILTERS = _load_filters()

# Pre-build lookup dicts for fast access
_MARKET_LIST: list[dict] = _FILTERS["data"]["market_data"]
_DISTRICT_LIST: list[dict] = _FILTERS["data"]["district_data"]

# district_id  -> district_name
_DISTRICT_BY_ID: dict[int, str] = {
    d["id"]: d["district_name"] for d in _DISTRICT_LIST
}


# ---------------------------------------------------------------------------
# Noise words that appear in market names but carry no geographic meaning.
# Both the input and the JSON mkt_name are stripped of these before matching.
# ---------------------------------------------------------------------------
_NOISE_WORDS: frozenset[str] = frozenset({
    "apmc",      # Agricultural Produce Market Committee
    "apmcs",
    "market",
    "markets",
    "mandi",
    "mandis",
    "yard",
    "yards",
    "sabzi",     # vegetable
    "krishi",    # agriculture
    "upaj",      # produce
    "wholesale",
    "retail",
    "regulated",
    "sub",
    "main",
    "new",
    "old",
    "rural",
    "urban",
    "local",
    "sandhai",   # Tamil for market
    "uzhavar",   # Tamil for farmer
    "rythu",     # Telugu for farmer
    "bazaar",
    "bazar",
    "lot",
})


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------
def normalize_name(name: str) -> str:
    """
    Basic normalization (applied to ALL names):
      - Unicode NFKD decomposition -> ASCII (strips accents)
      - Lower-case
      - Strip leading/trailing whitespace
      - Collapse internal whitespace to a single space
      - Remove punctuation except hyphens and parentheses
    """
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    name = re.sub(r"\s+", " ", name)
    # Remove punctuation except ( ) - .
    name = re.sub(r"[^\w\s\-\(\)\.]", "", name)
    return name


def strip_noise(normalized: str) -> str:
    """
    Remove noise words (whole-word matches only) from an already-normalized
    string, then collapse whitespace again.

    Example:
        'apmc anand market' -> 'anand'
        'uzhavar sandhai trichy' -> 'trichy'
    """
    tokens = normalized.split()
    tokens = [t for t in tokens if t not in _NOISE_WORDS]
    return " ".join(tokens).strip()


# Pre-compute both normalized and noise-stripped market names once
_NORMALIZED_MARKETS: list[tuple[str, str, dict]] = [
    (normalize_name(m["mkt_name"]), strip_noise(normalize_name(m["mkt_name"])), m)
    for m in _MARKET_LIST
]


# ---------------------------------------------------------------------------
# Core lookup
# ---------------------------------------------------------------------------
def get_district_for_apmc(apmc_name: str) -> dict | None:
    """
    Given an APMC name (raw string), normalize + strip noise words, then find
    the corresponding district name from the filters JSON.

    Matching strategy (in order):
      1. Exact match on the full normalized name (noise words kept).
      2. Exact match on the noise-stripped name (both sides stripped).
      3. Substring / partial match on the noise-stripped name.

    Returns a dict:
        {
          "input_name":        <original input>,
          "normalized_name":   <normalized input (with noise words)>,
          "stripped_name":     <noise-stripped input>,
          "matched_market":    <mkt_name from JSON>,
          "market_id":         <market id>,
          "district_id":       <district id>,
          "district_name":     <district name>,
        }

    Returns None if no market match is found, or the district_id is missing.
    """
    normalized_input = normalize_name(apmc_name)
    stripped_input   = strip_noise(normalized_input)

    matched_market = None

    # --- 1. Exact match on full normalized name ---
    for norm, _stripped, market in _NORMALIZED_MARKETS:
        if norm == normalized_input:
            matched_market = market
            break

    # --- 2. Exact match on noise-stripped name ---
    if matched_market is None and stripped_input:
        for _norm, stripped, market in _NORMALIZED_MARKETS:
            if stripped == stripped_input:
                matched_market = market
                break

    # --- 3. Substring / partial match on noise-stripped name ---
    if matched_market is None and stripped_input and len(stripped_input) >= 3:
        candidates = [
            (stripped, market)
            for _norm, stripped, market in _NORMALIZED_MARKETS
            if stripped and len(stripped) >= 3 and (
                stripped_input in stripped or stripped in stripped_input
            )
        ]
        if len(candidates) == 1:
            matched_market = candidates[0][1]
        elif len(candidates) > 1:
            # Pick the closest length match
            candidates.sort(key=lambda x: abs(len(x[0]) - len(stripped_input)))
            matched_market = candidates[0][1]

    if matched_market is None:
        return None

    district_id = matched_market.get("district_id")
    if district_id is None:
        return None

    district_name = _DISTRICT_BY_ID.get(district_id)

    return {
        "input_name":      apmc_name,
        "normalized_name": normalized_input,
        "stripped_name":   stripped_input,
        "matched_market":  matched_market["mkt_name"],
        "market_id":       matched_market["id"],
        "district_id":     district_id,
        "district_name":   district_name,
    }


# ---------------------------------------------------------------------------
# Convenience wrapper
# ---------------------------------------------------------------------------
def get_district_name(apmc_name: str) -> str | None:
    """
    Thin wrapper — returns just the district name string, or None.
    """
    result = get_district_for_apmc(apmc_name)
    return result["district_name"] if result else None


# ---------------------------------------------------------------------------
# Quick demo / manual test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    test_names = [
        # Exact matches (with noise words)
        "APMC ANAND",
        "apmc anand",
        "APMC HALVAD",
        # Input with noise stripped should still match JSON entry that has noise
        "ANAND",                       # → should match 'APMC ANAND'
        "Halvad",                      # → should match 'APMC HALVAD'
        "Anand Market",                # → should match 'APMC ANAND'
        # Noise on both sides
        "A lot APMC",
        # Unrecognized name
        "SomeRandomMarket",            # should return None
    ]
    district_json = {}
    for name in mandi_code:
        result = get_district_for_apmc(name["text"])
        if result:

            district_json[name["text"]] = result['district_name']
        else:
            print(f"[--] '{name}' → No match found")
    
    with open("district.json", "w") as f:
        json.dump(district_json, f, indent=4)
