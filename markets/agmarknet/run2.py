"""
run2.py
=======
Agmarknet price-arrival scraper using the data.gov.in open API.

Fetches all records for today's date (or a supplied date) via a single
curl call and returns them as a flat list[dict].

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
RECORD_LIMIT = int(os.getenv("AGMARKNET_LIMIT", "500000"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_url(arrival_date: str) -> str:
    """
    Build the data.gov.in API URL for a given Arrival_Date.

    Parameters
    ----------
    arrival_date : str
        Date in DD/MM/YYYY format, e.g. "28/07/2026".
    """
    # URL-encode the date for the filter parameter (/ -> %2F)
    encoded_date = arrival_date.replace("/", "%2F")
    return (
        f"https://api.data.gov.in/resource/{RESOURCE_ID}"
        f"?api-key={API_KEY}"
        f"&format=json"
        f"&limit={RECORD_LIMIT}"
        f"&filters%5BArrival_Date%5D={encoded_date}"
    )


def _fetch_via_curl(url: str) -> dict[str, Any]:
    """Run a curl GET request and return the parsed JSON response."""
    cmd = [
        "curl",
        "-X", "GET",
        url,
        "-H", "accept: application/json",
    ]
    logger.info("Running curl: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
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

    Parameters
    ----------
    date : str, optional
        Date in DD/MM/YYYY or YYYY-MM-DD format.
        Defaults to today's date.

    Returns
    -------
    list[dict]
        Raw records as returned by the API. Each record has keys:
        Arrival_Date, Commodity, Commodity_Code, District, Grade,
        Market, Max_Price, Min_Price, Modal_Price, State, Variety.
    """
    arrival_date = _to_dd_mm_yyyy(date)
    logger.info("Fetching Agmarknet data for date: %s", arrival_date)

    url = _build_url(arrival_date)
    response = _fetch_via_curl(url)

    records: list[dict[str, Any]] = response.get("records", [])
    total = response.get("total", len(records))

    logger.info(
        "Agmarknet fetch complete -- %d record(s) returned (API reports total=%s)",
        len(records),
        total,
    )
    return records


# ---------------------------------------------------------------------------
# Entry point (standalone test)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data = agmarknet()
    print(json.dumps(data, indent=2))
    print(f"\n Total records fetched: {len(data)}")
