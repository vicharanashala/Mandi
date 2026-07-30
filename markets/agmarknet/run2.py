"""
run2.py
=======
Agmarknet price-arrival scraper using the data.gov.in open API.

Resource
--------
ID   : 9ef84268-d588-465a-a308-a864a43d0070
Title: Current Daily Price of Various Commodities from Various Markets (Mandi)
URL  : https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070

API hard limit (Elasticsearch max_result_window)
-------------------------------------------------
The backend enforces:  offset + limit  <=  10 000

Fetching everything
-------------------
A single unfiltered query is limited to 10 000 records.  To retrieve the
full dataset (e.g. ~15 000+ records on busy days) we split by state:

    1. Probe the API (no filter) to get the global total and the list of
       states present in the response.
    2. For each state, paginate independently (each state has < 10 000
       records) and collect all its records.
    3. Merge the per-state lists into a single flat result.

Record schema (new API – lowercase field names)
-----------------------------------------------
{
    "state":        "Andhra Pradesh",
    "district":     "Prakasam",
    "market":       "Santhamaguluru APMC",
    "commodity":    "Maize",
    "variety":      "Hybrid",
    "grade":        "FAQ",
    "arrival_date": "30/07/2026",
    "min_price":    2400,
    "max_price":    2400,
    "modal_price":  2400
}
"""

import json
import logging
import os
import subprocess
import time
from datetime import date as date_type
from typing import Any

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RESOURCE_ID = os.getenv(
    "AGMARKNET_RESOURCE_ID",
    "9ef84268-d588-465a-a308-a864a43d0070",   # updated resource ID
)
API_KEY = os.getenv(
    "DATA_GOV_IN_API_KEY",
    "579b464db66ec23bdd0000019caa65074d924b6d6b8473dc337b0bca",
)

# Elasticsearch hard ceiling: offset + limit must NOT exceed ES_MAX_WINDOW.
# Requests violating this return a query_phase_execution_exception error.
ES_MAX_WINDOW: int = 10_000
PAGE_SIZE: int = min(int(os.getenv("AGMARKNET_PAGE_SIZE", "10000")), ES_MAX_WINDOW)

# Seconds to sleep between consecutive state fetches to avoid API rate-limiting.
# Increase this if you observe silent empty responses after heavy traffic.
STATE_DELAY_S: float = float(os.getenv("AGMARKNET_STATE_DELAY", "0.5"))

# Complete list of every Indian state / UT that can appear in this API.
# We iterate all of them unconditionally so no state is ever silently missed,
# even if it falls beyond the ES_MAX_WINDOW in an unfiltered sort order.
# States absent for a given date are detected via a cheap limit=1 probe
# (returns total=0) and skipped without downloading any records.
KNOWN_STATES: list[str] = [
    "Andaman and Nicobar",
    "Andhra Pradesh",
    "Arunachal Pradesh",
    "Assam",
    "Bihar",
    "Chandigarh",
    "Chattisgarh",
    "Dadra and Nagar Haveli",
    "Daman and Diu",
    "Goa",
    "Gujarat",
    "Haryana",
    "Himachal Pradesh",
    "Jammu and Kashmir",
    "Jharkhand",
    "Karnataka",
    "Keralam",
    "Lakshadweep",
    "Madhya Pradesh",
    "Maharashtra",
    "Manipur",
    "Meghalaya",
    "Mizoram",
    "NCT of Delhi",
    "Nagaland",
    "Odisha",
    "Pondicherry",
    "Punjab",
    "Rajasthan",
    "Sikkim",
    "Tamil Nadu",
    "Telangana",
    "Tripura",
    "Uttar Pradesh",
    "Uttarakhand",
    "West Bengal",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_via_curl(url: str) -> dict[str, Any]:
    """Run a curl GET request with a 30 s timeout and return parsed JSON."""
    cmd = [
        "curl",
        "-s",
        "--max-time", "30",
        "-X", "GET",
        url,
        "-H", "accept: application/json",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    if not result.stdout.strip():
        raise ValueError("curl returned an empty response body")
    data = json.loads(result.stdout)

    # Surface Elasticsearch window-exceeded errors embedded in the JSON message
    msg = data.get("message", "")
    if isinstance(msg, str) and "Result window is too large" in msg:
        raise ValueError(
            f"Elasticsearch max_result_window exceeded (offset + limit > {ES_MAX_WINDOW}). "
            "Reduce PAGE_SIZE or add a state/commodity filter."
        )
    return data


def _to_dd_mm_yyyy(date_str: str | None) -> str:
    """
    Accept YYYY-MM-DD or DD/MM/YYYY and always return DD/MM/YYYY.
    Falls back to today if date_str is None or empty.
    """
    if not date_str:
        return date_type.today().strftime("%d/%m/%Y")

    date_str = date_str.strip()

    if "/" in date_str:       # already DD/MM/YYYY
        return date_str

    if "-" in date_str:       # YYYY-MM-DD  ->  DD/MM/YYYY
        parts = date_str.split("-")
        if len(parts) == 3:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"

    return date_str           # fallback: return as-is


# ---------------------------------------------------------------------------
# Per-state pagination helper
# ---------------------------------------------------------------------------

def _fetch_for_state(
    arrival_date: str,
    encoded_date: str,
    state: str,
) -> list[dict[str, Any]]:
    """
    Fetch all records for a single state on a given date.

    Each state has far fewer than 10 000 records, so a normal offset loop
    works without hitting the Elasticsearch window ceiling.
    """
    from urllib.parse import quote

    encoded_state = quote(state, safe="")
    state_records: list[dict[str, Any]] = []
    offset = 0

    # Probe: get the total for this state
    probe_url = (
        f"https://api.data.gov.in/resource/{RESOURCE_ID}"
        f"?api-key={API_KEY}"
        f"&format=json"
        f"&limit=1"
        f"&offset=0"
        f"&filters%5Barrival_date%5D={encoded_date}"
        f"&filters%5Bstate.keyword%5D={encoded_state}"  # .keyword = exact term match
    )
    probe = _fetch_via_curl(probe_url)
    state_total: int = int(probe.get("total", 0))

    if state_total == 0:
        return []

    logger.info("  State %-30s  total=%d", f'"{state}"', state_total)

    while True:
        remaining_window = ES_MAX_WINDOW - offset
        if remaining_window <= 0:
            # Extremely unlikely for a single state, but guard anyway
            logger.warning(
                "  State %s: ES window reached at offset=%d (%d/%d records collected).",
                state, offset, len(state_records), state_total,
            )
            break

        batch_size = min(PAGE_SIZE, remaining_window)

        url = (
            f"https://api.data.gov.in/resource/{RESOURCE_ID}"
            f"?api-key={API_KEY}"
            f"&format=json"
            f"&limit={batch_size}"
            f"&offset={offset}"
            f"&filters%5Barrival_date%5D={encoded_date}"
            f"&filters%5Bstate.keyword%5D={encoded_state}"  # .keyword = exact term match
        )

        response: dict[str, Any] | None = None
        last_error: Exception | None = None

        for attempt in range(1, 4):
            try:
                response = _fetch_via_curl(url)
                break
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "  State %s – attempt %d failed (offset=%d): %s. Retrying in 2 s…",
                    state, attempt, offset, exc,
                )
                time.sleep(2)

        if response is None:
            raise RuntimeError(
                f"Failed to fetch state '{state}' for {arrival_date} "
                f"at offset={offset} after 3 attempts: {last_error}"
            )

        batch: list[dict[str, Any]] = response.get("records", [])

        # If the API silently returns 0 records despite state_total > 0, treat
        # it as a transient error so the outer retry loop can back off and retry.
        if not batch and len(state_records) < state_total:
            raise ValueError(
                f"API returned 0 records for state '{state}' at offset={offset} "
                f"but total={state_total} (likely rate-limited — will retry)"
            )

        state_records.extend(batch)

        if not batch or len(state_records) >= state_total:
            break

        offset += len(batch)

    return state_records


# ---------------------------------------------------------------------------
# Public scraper function
# ---------------------------------------------------------------------------

def agmarknet(date: str | None = None) -> list[dict[str, Any]]:
    """
    Fetch ALL price-arrival records from data.gov.in for the given date.

    Calling flow
    ------------
    1. **Count probe** — fetch ``limit=1`` to get the global ``total``
       cheaply without downloading any records.
    2. If ``total <= ES_MAX_WINDOW`` (10 000), do a single full-page fetch
       and return immediately.
    3. Otherwise **fetch per-state** using ``KNOWN_STATES`` — iterate every
       known Indian state/UT unconditionally.  A cheap ``limit=1`` probe
       inside each state's fetch returns ``total=0`` for absent states and
       skips them instantly.  This is safe regardless of ES sort order.
    4. Merge all per-state results into a single flat list and return.

    Parameters
    ----------
    date : str, optional
        Date in DD/MM/YYYY or YYYY-MM-DD format.  Defaults to today.

    Returns
    -------
    list[dict]
        Raw records as returned by the API (lowercase field names).
    """
    arrival_date = _to_dd_mm_yyyy(date)
    encoded_date = arrival_date.replace("/", "%2F")
    logger.info("Fetching Agmarknet data for date: %s", arrival_date)

    # ── Step 1: count probe (limit=1) — get total cheaply ────────────────
    count_url = (
        f"https://api.data.gov.in/resource/{RESOURCE_ID}"
        f"?api-key={API_KEY}"
        f"&format=json"
        f"&limit=1"
        f"&offset=0"
        f"&filters%5Barrival_date%5D={encoded_date}"
    )
    try:
        count_resp = _fetch_via_curl(count_url)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to probe Agmarknet for {arrival_date}: {exc}"
        ) from exc

    total: int = int(count_resp.get("total", 0))
    logger.info("Total records available for %s: %d", arrival_date, total)

    if total == 0:
        logger.warning("No records found for %s", arrival_date)
        return []

    # ── Step 2: if everything fits in one window, fetch and return ─────────
    if total <= ES_MAX_WINDOW:
        full_url = (
            f"https://api.data.gov.in/resource/{RESOURCE_ID}"
            f"?api-key={API_KEY}"
            f"&format=json"
            f"&limit={total}"
            f"&offset=0"
            f"&filters%5Barrival_date%5D={encoded_date}"
        )
        full_resp = _fetch_via_curl(full_url)
        records = full_resp.get("records", [])
        logger.info(
            "Agmarknet fetch complete for %s — %d record(s) returned (single page)",
            arrival_date, len(records),
        )
        return records

    # ── Step 3: total > 10 000 → per-state fetch using known state list ──────
    logger.info(
        "Total (%d) exceeds ES window (%d). Switching to per-state fetching.",
        total, ES_MAX_WINDOW,
    )

    # Iterate the full KNOWN_STATES list instead of discovering states
    # dynamically from the first 10k rows.  A dynamic query can miss states
    # whose records happen to fall beyond position 10 000 in the sort order.
    # The probe inside _fetch_for_state() returns total=0 for states absent
    # on the requested date, so they are skipped with a single cheap API call.

    all_records: list[dict[str, Any]] = []

    for i, state in enumerate(KNOWN_STATES):
        if i > 0:
            time.sleep(STATE_DELAY_S)   # avoid API rate-limiting between states
        try:
            state_records = _fetch_for_state(arrival_date, encoded_date, state)
            all_records.extend(state_records)
            logger.info(
                "  State %-30s  fetched=%d  running_total=%d",
                f'"{state}"', len(state_records), len(all_records),
            )
        except Exception as exc:
            logger.error("Failed to fetch state '%s': %s — skipping.", state, exc)

    logger.info(
        "Agmarknet fetch complete for %s — %d record(s) returned (per-state, expected ~%d)",
        arrival_date, len(all_records), total,
    )
    return all_records


# ---------------------------------------------------------------------------
# Entry point (standalone test)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data = agmarknet()
    print(json.dumps(data[:5], indent=2))
    print(f"\n Total records fetched: {len(data)}")

