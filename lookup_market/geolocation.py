"""
geolocation.py
--------------
Enriches markets.json with geolocation data from OpenStreetMap-based APIs.

Primary  : Photon (photon.komoot.io) — OSM-powered, no key, permissive rate limit
Fallback : Nominatim (nominatim.openstreetmap.org) — if Photon returns nothing

For each market the script tries candidate queries in order:
  1. cleaned_name + state + India
  2. cleaned_name + India
  3. alias1_cleaned + state + India  …  (same pattern per alias)

Where "cleaned" means APMC/PMY/SMY/Mandi/etc. are stripped so the geocoder
can match the actual place name.

Fills: district, lat, lon, postcode, bb_south, bb_north, bb_west, bb_east

Resume support:
  - Entries where lat is not None are skipped automatically.
  - Progress is saved every SAVE_EVERY records so a crash loses little work.

Usage:
    python3 geolocation.py               # full run
    python3 geolocation.py --dry-run     # preview queries, no API calls
    python3 geolocation.py --limit 100   # process only the first N pending
"""

import argparse
import json
import re
import time
from pathlib import Path

import requests
from tqdm import tqdm

# ── paths ──────────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).parent
JSON_PATH = BASE_DIR / "markets.json"

# ── API config ─────────────────────────────────────────────────────────────────
PHOTON_URL    = "https://photon.komoot.io/api/"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Browser-like headers help avoid 403 on Nominatim
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; MarketGeocoder/1.0; "
        "+https://github.com/vicharanashala/Mandi)"
    ),
    "Accept":          "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://nominatim.openstreetmap.org/",
}

DELAY_SECONDS = 1.1    # stay under 1 req/s for Nominatim; Photon is more relaxed
SAVE_EVERY    = 50     # save progress every N processed records
TIMEOUT       = 12     # HTTP timeout in seconds
MAX_RETRIES   = 2      # retries on transient errors (429, 5xx)

# ── market-type label stripping ────────────────────────────────────────────────
_MARKET_SUFFIXES = re.compile(
    r'\b(APMC|PMY|SMY|Market|Mandi|Kisan\s+Mandi|Jute|Pine\s+Apple'
    r'|Grain|Vegetable|Cotton|Timber|Fish|Wholesale|Rythu\s+Bazar)\b',
    re.IGNORECASE,
)


def clean_for_query(text: str) -> str:
    """Strip market-type labels and normalise whitespace."""
    cleaned = _MARKET_SUFFIXES.sub('', text)
    cleaned = re.sub(r'[\s,/()]+', ' ', cleaned).strip(' ,/()')
    return cleaned or text  # fallback: never return empty string


# ── Photon (primary) ───────────────────────────────────────────────────────────

def query_photon(place: str, country_code: str = "IN") -> dict | None:
    """
    Query photon.komoot.io.  Returns extracted fields dict or None.

    Response GeoJSON shape:
      features[0].geometry.coordinates = [lon, lat]
      features[0].properties: name, country, state, county, postcode,
                               extent=[west, south, east, north]
    """
    params = {
        "q":    place,
        "limit": 1,
        "lang": "en",
    }
    try:
        resp = requests.get(PHOTON_URL, params=params,
                            headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        if not features:
            return None

        feat  = features[0]
        props = feat.get("properties", {})
        geom  = feat.get("geometry", {})
        coord = geom.get("coordinates", [None, None])  # [lon, lat]

        # Filter: only accept results from India
        if props.get("country_code", "").upper() not in ("IN", "IND", ""):
            country = props.get("country", "")
            if country and "india" not in country.lower():
                return None

        extent = props.get("extent")  # [west, south, east, north]

        district = (
            props.get("county")
            or props.get("state_district")
            or props.get("city")
            or props.get("district")
            or None
        )

        def _f(v):
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        return {
            "district": district,
            "lat":      _f(coord[1]),
            "lon":      _f(coord[0]),
            "postcode": props.get("postcode") or None,
            "bb_south": _f(extent[1]) if extent else None,
            "bb_north": _f(extent[3]) if extent else None,
            "bb_west":  _f(extent[0]) if extent else None,
            "bb_east":  _f(extent[2]) if extent else None,
        }
    except requests.RequestException as exc:
        tqdm.write(f"    [Photon error] {place!r}: {exc}")
        return None


# ── Nominatim (fallback) ───────────────────────────────────────────────────────

def query_nominatim(place: str) -> dict | None:
    """
    Fallback to Nominatim.  Returns extracted fields dict or None.
    Respects rate limits and retries on 429/5xx.
    """
    params = {
        "q":              place,
        "format":         "json",
        "addressdetails": 1,
        "limit":          1,
        "countrycodes":   "in",
    }
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(NOMINATIM_URL, params=params,
                                headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code in (429, 503):
                wait = 5 * (attempt + 1)
                tqdm.write(f"    [Nominatim {resp.status_code}] backing off {wait}s …")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                tqdm.write(f"    [Nominatim 403] skipping Nominatim for: {place!r}")
                return None
            resp.raise_for_status()
            results = resp.json()
            if not results:
                return None

            r    = results[0]
            addr = r.get("address", {})
            bb   = r.get("boundingbox", [None, None, None, None])  # S, N, W, E

            district = (
                addr.get("county")
                or addr.get("state_district")
                or addr.get("city_district")
                or addr.get("city")
                or addr.get("town")
                or addr.get("village")
                or None
            )

            def _f(v):
                try:
                    return float(v) if v is not None else None
                except (TypeError, ValueError):
                    return None

            return {
                "district": district,
                "lat":      _f(r.get("lat")),
                "lon":      _f(r.get("lon")),
                "postcode": addr.get("postcode") or None,
                "bb_south": _f(bb[0]),
                "bb_north": _f(bb[1]),
                "bb_west":  _f(bb[2]),
                "bb_east":  _f(bb[3]),
            }
        except requests.RequestException as exc:
            tqdm.write(f"    [Nominatim error] {place!r}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(3)
    return None


# ── combined lookup ────────────────────────────────────────────────────────────

def lookup(query: str) -> dict | None:
    """Try Photon first, fall back to Nominatim."""
    result = query_photon(query)
    time.sleep(DELAY_SECONDS)
    if result and result.get("lat") is not None:
        return result
    # Nominatim fallback
    result = query_nominatim(query)
    time.sleep(DELAY_SECONDS)
    return result


# ── query builder ──────────────────────────────────────────────────────────────

def build_queries(market: dict) -> list[str]:
    """
    Build an ordered list of search strings for a market.
    Tries name first, then each alias.
    Each candidate is tried as 'place, State, India' then 'place, India'.
    """
    name      = market["name"]
    state     = market.get("state", "")
    aliases   = market.get("aliases", [])
    candidates = [name] + list(aliases)

    queries = []
    for candidate in candidates:
        clean = clean_for_query(candidate)
        if state:
            queries.append(f"{clean}, {state}, India")
        queries.append(f"{clean}, India")

    # de-duplicate, preserve order
    seen, unique = set(), []
    for q in queries:
        if q not in seen:
            seen.add(q)
            unique.append(q)
    return unique


# ── persistence ────────────────────────────────────────────────────────────────

def save(markets: list[dict]) -> None:
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(markets, f, ensure_ascii=False, indent=2)


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich markets.json with geolocation via Photon / Nominatim.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show queries without making API calls.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Process at most N pending markets (0 = unlimited).")
    args = parser.parse_args()

    if not JSON_PATH.exists():
        raise SystemExit(f"markets.json not found at {JSON_PATH}")

    with open(JSON_PATH, encoding="utf-8") as f:
        markets: list[dict] = json.load(f)

    total        = len(markets)
    already_done = sum(1 for m in markets if m.get("lat") is not None)
    pending      = [m for m in markets if m.get("lat") is None]
    to_process   = min(len(pending), args.limit) if args.limit else len(pending)

    print(f"Total markets : {total}")
    print(f"Already done  : {already_done}")
    print(f"Pending       : {len(pending)}")
    print(f"This run      : {to_process}")
    if args.dry_run:
        print("\n── DRY RUN (no API calls) ──\n")

    found = not_found = save_counter = 0
    pbar = tqdm(total=to_process, unit="market", dynamic_ncols=True)

    for market in pending[:to_process]:
        queries = build_queries(market)

        if args.dry_run:
            pbar.write(f"[DRY] {market['name']!r}")
            for q in queries:
                pbar.write(f"        → {q!r}")
            pbar.update(1)
            continue

        result     = None
        used_query = None

        for q in queries:
            result = lookup(q)
            if result and result.get("lat") is not None:
                used_query = q
                break

        if result and result.get("lat") is not None:
            market.update(result)
            found += 1
            pbar.set_postfix(found=found, miss=not_found, refresh=False)
            pbar.write(f"  ✓ {market['name']!r}  ← \"{used_query}\"")
        else:
            not_found += 1
            pbar.set_postfix(found=found, miss=not_found, refresh=False)
            pbar.write(f"  ✗ NOT FOUND: {market['name']!r}  [{market.get('state')}]")

        pbar.update(1)
        save_counter += 1
        if save_counter % SAVE_EVERY == 0:
            save(markets)
            pbar.write(f"  💾 Saved ({save_counter} this run, {found} found)")

    pbar.close()

    if not args.dry_run:
        save(markets)
        total_done = sum(1 for m in markets if m.get("lat") is not None)
        print(f"\n✓  This run  — found: {found}  |  not found: {not_found}")
        print(f"   Total done : {total_done} / {total}")
        print(f"   Saved to   : {JSON_PATH}")


if __name__ == "__main__":
    main()
