"""
agmarknetupdate.py
==================
Batch fetcher and exporter for Agmarknet price-arrival data across a date range.

Fetches data from data.gov.in day-by-day for each date from start_date to end_date
(inclusive) and saves it to a JSON file (default: agmarknet_12jul_27jul.json).

Optionally normalises and uploads to MongoDB if --upload is specified.
Includes a --delete flag to clean up records from MongoDB if needed.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

from markets.agmarknet.run2 import agmarknet
from database import (
    normalise_all,
    upload_to_mongo,
    MONGO_URI,
    DB_NAME,
    MASTER_COLLECTION,
    PRICE_COLLECTION,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[sys.stdout],
)
logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> datetime:
    """Parse date string in DD/MM/YYYY or YYYY-MM-DD format to UTC datetime."""
    date_str = date_str.strip()
    if "/" in date_str:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
    elif "-" in date_str:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    else:
        raise ValueError(f"Unrecognized date format: '{date_str}'. Expected DD/MM/YYYY or YYYY-MM-DD.")
    return dt.replace(tzinfo=timezone.utc)


def generate_date_range(start_date_str: str, end_date_str: str) -> list[str]:
    """Generate a list of date strings in DD/MM/YYYY format between start and end (inclusive)."""
    start_dt = parse_date(start_date_str)
    end_dt = parse_date(end_date_str)

    if start_dt > end_dt:
        raise ValueError(f"Start date ({start_date_str}) must be <= End date ({end_date_str}).")

    dates = []
    curr = start_dt
    while curr <= end_dt:
        dates.append(curr.strftime("%d/%m/%Y"))
        curr += timedelta(days=1)

    return dates


def update_agmarknet_range(
    start_date: str = "12/07/2026",
    end_date: str = "27/07/2026",
    output_file: str = "agmarknet_12jul_27jul.json",
    source_system: str = "agmarknet2",
    do_upload: bool = False,
) -> None:
    """
    Fetch Agmarknet records for each date in [start_date, end_date],
    save to JSON file (output_file), and optionally upload to MongoDB.
    """
    date_list = generate_date_range(start_date, end_date)
    logger.info(
        "Starting Agmarknet batch fetch for %d date(s): %s to %s",
        len(date_list),
        start_date,
        end_date,
    )

    all_records: list[dict] = []

    for idx, date_str in enumerate(date_list, 1):
        logger.info("-" * 60)
        logger.info("[%d/%d] Fetching Agmarknet records for date: %s", idx, len(date_list), date_str)

        try:
            records = agmarknet(date=date_str)
            logger.info("Fetched %d raw record(s) for %s", len(records), date_str)
            all_records.extend(records)
        except Exception as exc:
            logger.error("Failed to fetch data for date %s: %s", date_str, exc, exc_info=True)

    # Package payload in standard format: {"agmarknet": {"success": True, "data": all_records}}
    payload = {
        "agmarknet": {
            "success": bool(all_records),
            "data": all_records,
        }
    }

    # Save to JSON file
    logger.info("=" * 60)
    logger.info("Saving %d total records to %s ...", len(all_records), output_file)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    logger.info("✓ JSON file saved successfully: %s (%.2f MB, %d records)", output_file, file_size_mb, len(all_records))

    # Optional Upload to MongoDB
    if do_upload and all_records:
        logger.info("=" * 60)
        logger.info("Uploading to MongoDB (source_system='%s') ...", source_system)
        docs = normalise_all(payload)

        # Tag source_system as specified
        for doc in docs:
            doc["source_system"] = source_system

        upload_to_mongo(docs)
        logger.info("✓ Upload to MongoDB complete (%d documents).", len(docs))

    logger.info("=" * 60)
    logger.info("Task finished! Total records: %d | Output file: %s", len(all_records), output_file)
    logger.info("=" * 60)


def delete_agmarknet_data(
    source_system: str = "agmarknet2",
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """
    Delete all records with source_system (default: 'agmarknet2') from MongoDB
    collections ('price_records' and 'markets_commodities'). Optionally filter by date range.
    """
    from pymongo import MongoClient

    logger.info("=" * 60)
    logger.info("CLEANUP MODE: Deleting records for source_system='%s' from MongoDB...", source_system)
    logger.info("=" * 60)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    master_coll = db[MASTER_COLLECTION]
    price_coll = db[PRICE_COLLECTION]

    # Find master IDs matching source_system
    master_docs = list(master_coll.find({"source_system": source_system}, projection={"_id": 1}))
    master_ids = [d["_id"] for d in master_docs]
    logger.info("Found %d master record(s) in '%s' matching source_system='%s'.",
                len(master_ids), MASTER_COLLECTION, source_system)

    if not master_ids:
        logger.info("No matching records found to delete.")
        return

    price_query: dict = {"market_commodity_id": {"$in": master_ids}}

    if start_date and end_date:
        start_dt = parse_date(start_date)
        end_dt = parse_date(end_date) + timedelta(days=1) - timedelta(microseconds=1)
        price_query["date"] = {"$gte": start_dt, "$lte": end_dt}
        logger.info("Filtering price deletions between %s and %s", start_dt, end_dt)

    # 1. Delete price records
    res_price = price_coll.delete_many(price_query)
    logger.info("Deleted %d price record(s) from '%s'.", res_price.deleted_count, PRICE_COLLECTION)

    # 2. Delete master dimension records
    if not (start_date and end_date):
        res_master = master_coll.delete_many({"source_system": source_system})
        logger.info("Deleted %d master record(s) from '%s'.", res_master.deleted_count, MASTER_COLLECTION)
    else:
        active_master_ids = set(price_coll.distinct("market_commodity_id", {"market_commodity_id": {"$in": master_ids}}))
        orphan_ids = [m_id for m_id in master_ids if m_id not in active_master_ids]
        if orphan_ids:
            res_master = master_coll.delete_many({"_id": {"$in": orphan_ids}})
            logger.info("Deleted %d orphaned master record(s) from '%s'.", res_master.deleted_count, MASTER_COLLECTION)

    logger.info("✓ Cleanup complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Agmarknet Date Range Scraper & JSON Saver")
    parser.add_argument("--start-date", type=str, default="29/07/2026", help="Start date (DD/MM/YYYY or YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="30/07/2026", help="End date (DD/MM/YYYY or YYYY-MM-DD)")
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="agmarknet_12jul_27jul.json",
        help="JSON file path to save data (default: agmarknet_12jul_27jul.json)",
    )
    parser.add_argument("--source-system", type=str, default="agmarknet2", help="Source system tag for DB upload (default: agmarknet2)")
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Also upload normalised data to MongoDB",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete records with specified source-system from MongoDB instead of fetching/saving",
    )
    args = parser.parse_args()

    if args.delete:
        delete_agmarknet_data(
            source_system=args.source_system,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    else:
        update_agmarknet_range(
            start_date=args.start_date,
            end_date=args.end_date,
            output_file=args.output,
            source_system=args.source_system,
            do_upload=args.upload,
        )


if __name__ == "__main__":
    main()
