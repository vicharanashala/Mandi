"""
main.py
=======
Master orchestrator for the India APMC Mandi Data Scraper.

Pipeline (sequential steps)
---------------------------
Step 1 – Agmarknet Filter Refresh
    Calls filter_agmarknet.fetch_agmarknet_filters() + save_filters()
    → saves agmarknet_filters.json (used by agmarknet/run.py as market list)
    → no data returned to pipeline; purely a file-update step

Step 2 – Agmarknet Scrape  (async)
    Calls agmarknet.agmarknet() → returns list[dict] of price-arrival records
    for ALL markets in the freshly-updated agmarknet_filters.json

Step 3 – Other-markets Scrape  (async orchestrator)
    Calls othermarkets.run_all_scrapers() → returns dict:
        {
            "Karnataka":     {"success": True, "data": [...]},
            "Maharashtra":   {"success": True, "data": [...]},
            "Meghalaya":     {"success": True, "data": [...]},
            "Nagaland":      {"success": True, "data": [...]},
            "Punjab":        {"success": True, "data": [...]},
            "Uttar Pradesh": {"success": True, "data": [...]},
        }

Step 4 – Merge
    Combines agmarknet records + othermarkets dict into one unified payload:
        {
            "agmarknet":     {"success": True, "data": [...]},
            "Karnataka":     {"success": True, "data": [...]},
            ...
        }

Step 5 – Save to JSON
    Writes final_data.json (useful as a checkpoint / debug artifact)

Step 6 – Normalise + Upload to MongoDB
    Calls database.normalise_all(merged) → list[unified_doc]
    Calls database.upload_to_mongo(docs)  → bulk upsert into MongoDB
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# ── Agmarknet pipeline ────────────────────────────────────────────────────────
from markets.agmarknet.run2 import agmarknet

# ── Other-state scrapers ──────────────────────────────────────────────────────
from markets.othermarkets.run import run_all_scrapers

# ── Database ──────────────────────────────────────────────────────────────────
from database import normalise_all, upload_to_mongo

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            f"scraper_run_{datetime.now().strftime('%Y%m%d')}.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)


def _record_count(data: object) -> int:
    if isinstance(data, (list, tuple, dict, set)):
        return len(data)
    return 0


def _summary_error(payload: dict) -> str:
    error = str(payload.get("error") or "")
    details = str(payload.get("details") or "")
    if error and details and details != error:
        message = f"{error}: {details}"
    else:
        message = error or details
    return message[:220]


def log_scraping_summary(merged: dict, elapsed: float) -> None:
    logger.info("")
    logger.info("SCRAPING STATUS SUMMARY")
    logger.info("%-18s %-8s %10s  %s", "Source", "Status", "Records", "Error")
    logger.info("%-18s %-8s %10s  %s", "-" * 18, "-" * 8, "-" * 10, "-" * 20)

    for source, payload in merged.items():
        if not isinstance(payload, dict):
            logger.info("%-18s %-8s %10s  %s", source, "FAILED", 0, "invalid payload")
            continue

        success = bool(payload.get("success"))
        data = payload.get("data") or []
        error = _summary_error(payload)
        status = "OK" if success else "FAILED"
        logger.info("%-18s %-8s %10d  %s", source, status, _record_count(data), error)

    logger.info("Pipeline finished in %.1f seconds.", elapsed)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 is now the first active pipeline step; Step 1 (filter refresh) has
# been removed because the new data.gov.in API does not require a local
# market-list file.
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Agmarknet scrape
# ─────────────────────────────────────────────────────────────────────────────

async def step2_scrape_agmarknet(date_str: str = "") -> dict:
    """
    Fetch all price-arrival records from data.gov.in for today (or date_str)
    via a single curl request. The underlying ``agmarknet()`` call is
    synchronous (uses subprocess curl) and is run in a thread executor so it
    does not block the event loop when used with asyncio.gather.

    Returns
    -------
    dict
        ``{"success": bool, "data": list, "error": str|None}``.
    """
    logger.info("Starting Agmarknet scrape.")
    try:
        # agmarknet may be async (old httpx-based run.py) or sync (new
        # curl-based run2.py).  Handle both transparently.
        if asyncio.iscoroutinefunction(agmarknet):
            records = await agmarknet(date_str or None)
        else:
            records = await asyncio.to_thread(agmarknet, date_str or None)
        logger.info("Agmarknet scrape complete: %d records.", len(records))
        return {"success": True, "data": records}
    except Exception as exc:
        logger.error("Agmarknet scrape failed: %s", exc)
        return {
            "success": False,
            "error": "Agmarknet scrape failed",
            "details": str(exc),
            "data": [],
        }


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Other-state scrapers
# ─────────────────────────────────────────────────────────────────────────────

async def step3_scrape_other_markets(date_str: str = "") -> dict:
    """
    Run all non-agmarknet state scrapers concurrently:
    Karnataka, Maharashtra, Meghalaya, Nagaland, Punjab, Uttar Pradesh.

    Parameters
    ----------
    date_str : str, optional
        DD/MM/YYYY date forwarded to scrapers that accept it.
        Defaults to each scraper's own default (today / yesterday).

    Returns
    -------
    dict
        ``{ "Karnataka": {"success": bool, "data": [...]}, … }``
    """
    logger.info("Starting other-market scrapers.")
    try:
        results = await run_all_scrapers(date_str=date_str)
        logger.info("Other-market scrapers complete.")
        return results
    except Exception as exc:
        logger.error("Other-market scrape failed: %s", exc)
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Merge agmarknet + other-markets into one unified payload
# ─────────────────────────────────────────────────────────────────────────────

def step4_merge(
    agmarknet_result: list[dict] | dict,
    other_markets: dict,
) -> dict:
    """
    Combine the agmarknet flat list and the per-state other-markets dict
    into a single payload that ``database.normalise_all`` can consume.

    The final shape mirrors what ``final_data.json`` stores:

    .. code-block:: json

        {
            "agmarknet":     {"success": true, "data": [...]},
            "Karnataka":     {"success": true, "data": [...]},
            "Maharashtra":   {"success": true, "data": [...]},
            ...
        }

    Parameters
    ----------
    agmarknet_result : list[dict] | dict
        Step 2 result. New shape is ``{"success": bool, "data": [...]}``;
        lists are still accepted for backwards compatibility.
    other_markets : dict
        Per-state dicts from Step 3 (empty dict if step failed).

    Returns
    -------
    dict
        Unified payload ready for normalisation and upload.
    """
    merged: dict = {}

    # Agmarknet comes first (national feed)
    if isinstance(agmarknet_result, dict):
        merged["agmarknet"] = agmarknet_result
    else:
        merged["agmarknet"] = {
            "success": bool(agmarknet_result),
            "data": agmarknet_result,
        }

    # All other state scrapers
    merged.update(other_markets)

    total_records = sum(
        len(v.get("data") or [])
        for v in merged.values()
        if isinstance(v, dict)
    )
    logger.info("Merged %d sources with %d total records.", len(merged), total_records)
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Save merged payload to JSON (checkpoint)
# ─────────────────────────────────────────────────────────────────────────────

def step5_save_json(merged: dict, path: str = "final_data.json") -> None:
    """
    Save the merged payload to a JSON file as a debug checkpoint.
    This file can be inspected manually or re-fed into the database
    uploader without re-running the scrapers.
    """
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=4)
        size_mb = Path(path).stat().st_size / 1_048_576
        logger.info("Saved checkpoint %.2f MB -> %s.", size_mb, path)
    except Exception as exc:
        logger.error("Failed to save JSON: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Normalise + upload to MongoDB
# ─────────────────────────────────────────────────────────────────────────────

def step6_upload(merged: dict) -> None:
    """
    Normalise every state's raw records into the unified schema and
    bulk-upsert them into MongoDB.

    Calls:
      - ``database.normalise_all(merged)``  →  list[unified_doc]
      - ``database.upload_to_mongo(docs)``  →  bulk upsert
    """
    try:
        docs = normalise_all(merged)
        logger.info("Normalised %d documents. Uploading to MongoDB.", len(docs))
        upload_to_mongo(docs)
        logger.info("MongoDB upload complete.")
    except Exception as exc:
        logger.error("Upload failed: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN async pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def run_pipeline(date_str: str = "", skip_agmarknet: bool = False) -> None:
    """
    Execute the full end-to-end pipeline:
    1. (Removed) Agmarknet filter refresh — no longer needed with data.gov.in API
    2. Scrape Agmarknet via data.gov.in curl (skipped if skip_agmarknet is True)
    3. Scrape other state markets (async)
    4. Merge all data
    5. Save to final_data.json
    6. Normalise + upload to MongoDB
    """
    start = datetime.now()
    logger.info("APMC scraper started at %s.", start.strftime("%Y-%m-%d %H:%M:%S"))

    if skip_agmarknet:
        logger.info("Skipping Agmarknet scrape (skip_agmarknet=True) …")
        agmarknet_result = {
            "success": False,
            "error": "Agmarknet skipped",
            "data": [],
        }
        other_markets = await step3_scrape_other_markets(date_str=date_str)
    else:
        logger.info("Running Agmarknet and other-state scrapers in parallel.")
        agmarknet_result, other_markets = await asyncio.gather(
            step2_scrape_agmarknet(date_str=date_str),
            step3_scrape_other_markets(date_str=date_str),
            return_exceptions=False,   # let exceptions propagate to top-level handler
        )

    # If a step returned an exception object (shouldn't happen here), normalise
    if isinstance(agmarknet_result, BaseException):
        logger.error("Agmarknet step raised: %s", agmarknet_result)
        agmarknet_result = {
            "success": False,
            "error": "Agmarknet step raised",
            "details": str(agmarknet_result),
            "data": [],
        }
    if isinstance(other_markets, BaseException):
        logger.error("Other-markets step raised: %s", other_markets)
        other_markets = {}

    # Step 4 — merge
    merged = step4_merge(agmarknet_result, other_markets)

    # Step 5 — save checkpoint JSON
    step5_save_json(merged)

    # Step 6 — upload to MongoDB
    step6_upload(merged)

    elapsed = (datetime.now() - start).total_seconds()
    log_scraping_summary(merged, elapsed)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="APMC Mandi Scraper Pipeline")
    parser.add_argument("--date", type=str, default="", help="Date string (DD/MM/YYYY or YYYY-MM-DD)")
    parser.add_argument(
        "--skip-agmarknet",
        action="store_true",
        help="Skip Agmarknet filter refresh and scrape for testing",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run_pipeline(date_str=args.date, skip_agmarknet=args.skip_agmarknet))
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user (KeyboardInterrupt).")
    except Exception as exc:
        logger.critical("Pipeline crashed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    import time
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    logger.debug("Runtime: %.2f seconds", end - start)
