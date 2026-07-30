"""
run2.py
=======
Agmarknet price-arrival scraper using the data.gov.in open API.

Fetches all records for today's date (or a supplied date) via paginated curl calls
to prevent API payload truncation issues, and returns them as a flat list[dict].

Each record from the API looks like:
    {
        "Arrival_Date":    "28/07/2026",
        "Commodity":       "Mango(Raw-Ripe)",
        "Commodity_Code":  172,
        "District":        "Idukki",
        "Grade":           "Medium",
        "Market":          "Munnar Market",
        "Max_Price":       6500,
        "Min_Price":       6500,
        "Modal_Price":     6500,
        "State":           "Keralam",
        "Variety":         "Mango - Raw-Ripe"
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
    "35985678-0d79-46b4-9ed6-6f13308a1d24",
)
API_KEY = os.getenv(
    "DATA_GOV_IN_API_KEY",
    "579b464db66ec23bdd0000019caa65074d924b6d6b8473dc337b0bca",
)
PAGE_SIZE = int(os.getenv("AGMARKNET_PAGE_SIZE", "10000"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_via_curl(url: str) -> dict[str, Any]:
    """Run a curl GET request with a 30s timeout and return parsed JSON response."""
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
        raise ValueError("curl returned empty stdout response")
    return json.loads(result.stdout)


def _to_dd_mm_yyyy(date_str: str | None) -> str:
    """
    Accept YYYY-MM-DD or DD/MM/YYYY and always return DD/MM/YYYY.
    Falls back to today if date_str is None or empty.
    """
    if not date_str:
        today = date_type.today()
        return today.strftime("%d/%m/%Y")

    date_str = date_str.strip()

    # Already DD/MM/YYYY
    if "/" in date_str:
        return date_str

    # YYYY-MM-DD -> DD/MM/YYYY
    if "-" in date_str:
        parts = date_str.split("-")
        if len(parts) == 3:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"

    # Fallback: return as-is
    return date_str


# ---------------------------------------------------------------------------
# Public scraper function
# ---------------------------------------------------------------------------

def agmarknet(date: str | None = None) -> list[dict[str, Any]]:
    """
    Fetch all price-arrival records from data.gov.in for the given date.
    Uses pagination (PAGE_SIZE=10000) and retry logic to prevent JSON truncation
    errors on large response payloads.

    Parameters
    ----------
    date : str, optional
        Date in DD/MM/YYYY or YYYY-MM-DD format.
        Defaults to today's date.

    Returns
    -------
    list[dict]
        Raw records as returned by the API.
    """
    arrival_date = _to_dd_mm_yyyy(date)
    encoded_date = arrival_date.replace("/", "%2F")
    logger.info("Fetching Agmarknet data for date: %s", arrival_date)

    all_records: list[dict[str, Any]] = []
    offset = 0

    while True:
        url = (
            f"https://api.data.gov.in/resource/{RESOURCE_ID}"
            f"?api-key={API_KEY}"
            f"&format=json"
            f"&limit={PAGE_SIZE}"
            f"&offset={offset}"
            f"&filters%5BArrival_Date%5D={encoded_date}"
        )

        response = None
        last_error = None

        for attempt in range(1, 4):
            try:
                response = _fetch_via_curl(url)
                break
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Attempt %d failed for %s (offset=%d): %s. Retrying...",
                    attempt,
                    arrival_date,
                    offset,
                    exc,
                )
                time.sleep(2)

        if response is None:
            raise RuntimeError(
                f"Failed to fetch Agmarknet data for {arrival_date} at offset {offset} after 3 attempts: {last_error}"
            )

        records: list[dict[str, Any]] = response.get("records", [])
        total = int(response.get("total", len(records)))
        all_records.extend(records)

        logger.info(
            "Page fetched for %s (offset=%d) -- %d record(s) [accumulated=%d / total=%d]",
            arrival_date,
            offset,
            len(records),
            len(all_records),
            total,
        )

        if not records or len(all_records) >= total:
            break

        offset += PAGE_SIZE

    logger.info(
        "Agmarknet fetch complete for %s -- %d record(s) returned",
        arrival_date,
        len(all_records),
    )
    return all_records


# ---------------------------------------------------------------------------
# Entry point (standalone test)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data = agmarknet()
    print(json.dumps(data[:5], indent=2))
    print(f"\n Total records fetched: {len(data)}")
