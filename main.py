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
from markets.agmarknet.filter_agmarknet import fetch_agmarknet_filters, save_filters
from markets.agmarknet.run import agmarknet

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


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Agmarknet filter refresh
# ─────────────────────────────────────────────────────────────────────────────

def step1_refresh_agmarknet_filters() -> None:
    """
    Fetch fresh filter metadata from the Agmarknet API and save it to
    ``agmarknet_filters.json``.

    This file is read by Step 2 (agmarknet scraper) as the market list,
    so it must be refreshed BEFORE the agmarknet scrape runs.

    No data is returned — this step only writes a file.
    """
    logger.info("=" * 60)
    logger.info("STEP 1 — Refreshing Agmarknet filter metadata …")
    logger.info("=" * 60)
    try:
        filters = fetch_agmarknet_filters()
        save_filters(filters)           # saves to agmarknet_filters.json
        logger.info("STEP 1 ✓ — Filter metadata saved successfully.")
    except Exception as exc:
        # Non-fatal: if the file already exists from a previous run the
        # agmarknet scraper will use the old version and still work.
        logger.warning(
            "STEP 1 ⚠ — Could not refresh filters (using existing file if present): %s",
            exc,
        )


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Agmarknet scrape
# ─────────────────────────────────────────────────────────────────────────────

async def step2_scrape_agmarknet() -> list[dict]:
    """
    Scrape marketwise price-arrival records for every market in
    ``agmarknet_filters.json`` concurrently.

    Returns
    -------
    list[dict]
        Raw agmarknet records (each dict has fields like ``cmdt_name``,
        ``reported_date``, ``market``, ``state``, etc.)
    """
    logger.info("=" * 60)
    logger.info("STEP 2 — Scraping Agmarknet …")
    logger.info("=" * 60)
    try:
        records = await agmarknet()
        logger.info("STEP 2 ✓ — Agmarknet returned %d records.", len(records))
        return records
    except Exception as exc:
        logger.error("STEP 2 ✗ — Agmarknet scrape failed: %s", exc, exc_info=True)
        return []


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
    logger.info("=" * 60)
    logger.info("STEP 3 — Scraping other state markets …")
    logger.info("=" * 60)
    try:
        results = await run_all_scrapers(date_str=date_str)
        for state, result in results.items():
            status = "✓" if result.get("success") else "✗"
            count  = len(result.get("data") or []) if result.get("success") else "–"
            logger.info("  %s %s — %s records", status, state, count)
        logger.info("STEP 3 ✓ — Other markets scrape complete.")
        return results
    except Exception as exc:
        logger.error("STEP 3 ✗ — Other markets scrape failed: %s", exc, exc_info=True)
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Merge agmarknet + other-markets into one unified payload
# ─────────────────────────────────────────────────────────────────────────────

def step4_merge(
    agmarknet_records: list[dict],
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
    agmarknet_records : list[dict]
        Raw records from Step 2 (empty list if step failed).
    other_markets : dict
        Per-state dicts from Step 3 (empty dict if step failed).

    Returns
    -------
    dict
        Unified payload ready for normalisation and upload.
    """
    logger.info("=" * 60)
    logger.info("STEP 4 — Merging data sources …")
    logger.info("=" * 60)

    merged: dict = {}

    # Agmarknet comes first (national feed)
    merged["agmarknet"] = {
        "success": bool(agmarknet_records),
        "data":    agmarknet_records,
    }

    # All other state scrapers
    merged.update(other_markets)

    total_records = sum(
        len(v.get("data") or [])
        for v in merged.values()
        if isinstance(v, dict)
    )
    logger.info(
        "STEP 4 ✓ — Merged %d sources, %d total records.",
        len(merged),
        total_records,
    )
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
    logger.info("=" * 60)
    logger.info("STEP 5 — Saving merged data to %s …", path)
    logger.info("=" * 60)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=4)
        size_mb = Path(path).stat().st_size / 1_048_576
        logger.info("STEP 5 ✓ — Saved %.2f MB → %s", size_mb, path)
    except Exception as exc:
        logger.error("STEP 5 ✗ — Failed to save JSON: %s", exc, exc_info=True)


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
    logger.info("=" * 60)
    logger.info("STEP 6 — Normalising & uploading to MongoDB …")
    logger.info("=" * 60)
    try:
        docs = normalise_all(merged)
        logger.info("Normalised %d documents — beginning upload …", len(docs))
        upload_to_mongo(docs)
        logger.info("STEP 6 ✓ — Upload complete.")
    except Exception as exc:
        logger.error("STEP 6 ✗ — Upload failed: %s", exc, exc_info=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN async pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def run_pipeline(date_str: str = "") -> None:
    """
    Execute the full end-to-end pipeline:
    1. Refresh Agmarknet filters
    2. Scrape Agmarknet (async)
    3. Scrape other state markets (async)
    4. Merge all data
    5. Save to final_data.json
    6. Normalise + upload to MongoDB
    """
    start = datetime.now()
    logger.info("━" * 60)
    logger.info("  APMC Mandi Scraper Pipeline — started %s", start.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("━" * 60)

    # Step 1 — sync; run before anything async
    step1_refresh_agmarknet_filters()

    # Steps 2 & 3 run CONCURRENTLY (both are async)
    logger.info("Running Agmarknet and other-state scrapers in parallel …")
    agmarknet_records, other_markets = await asyncio.gather(
        step2_scrape_agmarknet(),
        step3_scrape_other_markets(date_str=date_str),
        return_exceptions=False,   # let exceptions propagate to top-level handler
    )

    # If a step returned an exception object (shouldn't happen here), normalise
    if isinstance(agmarknet_records, BaseException):
        logger.error("Agmarknet step raised: %s", agmarknet_records)
        agmarknet_records = []
    if isinstance(other_markets, BaseException):
        logger.error("Other-markets step raised: %s", other_markets)
        other_markets = {}

    # Step 4 — merge
    merged = step4_merge(agmarknet_records, other_markets)

    # Step 5 — save checkpoint JSON
    step5_save_json(merged)

    # Step 6 — upload to MongoDB
    step6_upload(merged)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("━" * 60)
    logger.info("  Pipeline finished in %.1f seconds.", elapsed)
    logger.info("━" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user (KeyboardInterrupt).")
    except Exception as exc:
        logger.critical("Pipeline crashed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    import time
    start = time.perf_counter()
    # Your code here
    main()
    end = time.perf_counter()
    print(f"Runtime: {end - start} seconds")
