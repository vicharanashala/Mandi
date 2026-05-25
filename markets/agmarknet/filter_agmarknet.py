"""
filter_agmarknet.py
~~~~~~~~~~~~~~~~~~~
Fetches the Agmarknet daily-price-arrival filter metadata
(states, commodities, varieties, markets …) from the public API and
saves the raw JSON to a file.

Endpoint : GET https://api.agmarknet.gov.in/v1/daily-price-arrival/filters
Retry    : exponential back-off via the `backoff` library
           - retries on 429, 5xx, and transient network errors
           - hard-fails on other 4xx immediately
Output   : JSON written as a Python object (not a string) via json.dump
"""

import json
import logging
import os
from pathlib import Path
from typing import Any

import backoff
import httpx
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration  (all tuneable via environment variables)
# ---------------------------------------------------------------------------
BASE_URL        = os.getenv("AGMARKNET_BASE_URL", "https://api.agmarknet.gov.in/v1").rstrip("/")
FILTERS_PATH    = "/daily-price-arrival/filters"
TIMEOUT_SECONDS = float(os.getenv("AGMARKNET_TIMEOUT_SECONDS", "30"))
MAX_TRIES       = int(os.getenv("AGMARKNET_MAX_RETRIES", "5"))
MAX_TIME        = float(os.getenv("AGMARKNET_MAX_BACKOFF_TIME", "120"))   # seconds overall budget
OUTPUT_FILE     = Path(os.getenv("AGMARKNET_FILTER_OUTPUT", "agmarknet_filters.json"))

# Browser-like headers to avoid nginx-level blocks
HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://agmarknet.gov.in/",
    "Connection":      "keep-alive",
}


# ---------------------------------------------------------------------------
# Back-off predicates
# ---------------------------------------------------------------------------

def _is_retryable(exc: Exception) -> bool:
    """Return True when the exception warrants a retry."""
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code if exc.response is not None else 0
        # Retry on rate-limit and server errors only
        return status == 429 or 500 <= status < 600
    # Retry on transient network / timeout errors
    return isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.TimeoutException,
            httpx.RemoteProtocolError,
            httpx.RequestError,
        ),
    )


def _log_backoff(details: dict[str, Any]) -> None:
    logger.warning(
        "Backing off %.2fs after %s tries | exception: %s",
        details["wait"],
        details["tries"],
        details["exception"],
    )


def _log_giveup(details: dict[str, Any]) -> None:
    logger.error(
        "Giving up after %s tries | exception: %s",
        details["tries"],
        details["exception"],
    )


# ---------------------------------------------------------------------------
# Core HTTP call  (decorated with back-off)
# ---------------------------------------------------------------------------

@backoff.on_exception(
    backoff.expo,
    Exception,                 # broad catch; filtered by `giveup`
    max_tries=MAX_TRIES,
    max_time=MAX_TIME,
    giveup=lambda exc: not _is_retryable(exc),
    on_backoff=_log_backoff,
    on_giveup=_log_giveup,
    jitter=backoff.full_jitter,
    logger=None,               # suppress backoff's own logger; we log ourselves
)
def _fetch_filters() -> httpx.Response:
    """
    Synchronous GET request (httpx sync client).

    Uses a fresh client per call so that each retry gets a clean connection.
    """
    url = f"{BASE_URL}{FILTERS_PATH}"
    logger.info("GET %s", url)

    with httpx.Client(
        timeout=httpx.Timeout(TIMEOUT_SECONDS),
        headers=HEADERS,
        follow_redirects=True,
    ) as client:
        response = client.get(url)
        response.raise_for_status()   # raises HTTPStatusError for 4xx / 5xx
        return response


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------

def fetch_agmarknet_filters() -> dict[str, Any]:
    """
    Fetch filter metadata from the Agmarknet daily-price-arrival endpoint.

    Returns the parsed JSON payload as a Python dict.
    Raises on unrecoverable errors (non-retryable HTTP errors, JSON parse
    failures, etc.).
    """
    response = _fetch_filters()
    logger.info("HTTP %s received (%d bytes)", response.status_code, len(response.content))

    try:
        payload: dict[str, Any] = response.json()
    except Exception as exc:
        logger.error("Failed to parse JSON response: %s", exc)
        raise

    return payload


def save_filters(payload: dict[str, Any], output_path: Path = OUTPUT_FILE) -> None:
    """
    Persist *payload* as a formatted JSON file.

    The payload is written as a native Python object — not a JSON string —
    so the file is always valid, human-readable JSON.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

    logger.info("Saved filters to %s", output_path.resolve())


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Fetching Agmarknet filter metadata …")

    filters = fetch_agmarknet_filters()

    # Pretty-print a summary of top-level keys
    top_keys = list(filters.keys()) if isinstance(filters, dict) else type(filters).__name__
    logger.info("Top-level keys in response: %s", top_keys)

    save_filters(filters)

    total_keys = len(filters) if isinstance(filters, dict) else "N/A"
    print(f"\n✅ Filters fetched successfully ({total_keys} top-level keys)")
    print(f"📄 Saved to: {OUTPUT_FILE.resolve()}")
