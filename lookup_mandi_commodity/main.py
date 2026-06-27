"""
upload_xlsx_to_mongodb.py

Reads your xlsx file and uploads all rows to MongoDB commodity_alias_lookup collection.

Usage:
    pip install pymongo openpyxl

    MONGODB_URI="mongodb+srv://user:pass@cluster/agri_db" python upload_xlsx_to_mongodb.py --file your_file.xlsx
    
    # Or with all options:
    python upload_xlsx_to_mongodb.py --file your_file.xlsx --db agri_db --sheet Sheet1
"""

import os
import sys
import argparse
from datetime import datetime, timezone
from collections import defaultdict

import openpyxl
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, TEXT
from pymongo.errors import OperationFailure

load_dotenv()

COLLECTION = "commodity_alias_lookup"

# ── Column name mapping (matches your xlsx headers) ──────────────────────────
# Update these if your column headers are different
COL_MANDI_NAME    = "Commodity name -comes from mandi dataset"   # Column A
COL_CROP_NAME     = "crop name from crop master"                  # Column B
COL_SIMILAR_NAME  = "Similar Name"                                # Column C
# COL_CORRECT_WRONG = "Correct/wrong"                               # Column D
COL_CROP_ID       = "crop id -from crop master"                   # Column E

# ── MongoDB Schema ────────────────────────────────────────────────────────────
SCHEMA = {
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["aliases", "canonical_name", "active", "createdAt", "updatedAt"],
            "properties": {
                "crop_master_id":   {"bsonType": ["string", "null"]},
                "canonical_name":   {"bsonType": "string"},
                "aliases":          {"bsonType": "array", "minItems": 1, "items": {"bsonType": "string"}},
                "language_variants":{"bsonType": "array", "items": {"bsonType": "string"}},
                "confidence":       {"bsonType": "double", "minimum": 0, "maximum": 1},
                "notes":            {"bsonType": ["string", "null"]},
                "active":           {"bsonType": "bool"},
                "source_tags":      {"bsonType": "array", "items": {"bsonType": "string"}},
                "createdAt":        {"bsonType": "date"},
                "updatedAt":        {"bsonType": "date"},
            }
        }
    },
    "validationLevel":  "moderate",
    "validationAction": "warn",
}


# ── Read xlsx ─────────────────────────────────────────────────────────────────
def read_xlsx(filepath: str, sheet_name: str = None) -> list[dict]:
    """
    Reads all rows from the xlsx file.
    Returns list of dicts with column headers as keys.
    """
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)

    if sheet_name:
        ws = wb[sheet_name]
    else:
        ws = wb.active
        print(f"📄 Reading sheet: '{ws.title}'")

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        raise ValueError("Excel file is empty")

    headers = [str(h).strip() if h is not None else f"col_{i}" for i, h in enumerate(rows[0])]
    print(f"📋 Columns found: {headers}")

    data = []
    for row in rows[1:]:
        if all(v is None for v in row):
            continue  # skip empty rows
        data.append(dict(zip(headers, row)))

    print(f"✅ Read {len(data)} rows from xlsx\n")
    wb.close()
    return data


# ── Build MongoDB documents from xlsx rows ────────────────────────────────────
def build_documents(rows: list[dict]) -> list[dict]:
    """
    Groups rows by crop_master_id and builds one MongoDB document per crop.
    Multiple mandi names for the same crop_id get merged into one aliases[] array.
    """
    # Group by crop_master_id (or canonical_name if no id)
    groups = defaultdict(lambda: {
        "crop_master_id": None,
        "canonical_name": None,
        "aliases": set(),
        "language_variants": set(),
        "confidence": 1.0,
        "notes": [],
        "source_tags": {"kishar"},
    })

    for i, row in enumerate(rows, start=2):  # start=2 because row 1 is header
        mandi_name    = row.get(COL_MANDI_NAME)
        crop_name     = row.get(COL_CROP_NAME)
        similar_name  = row.get(COL_SIMILAR_NAME)
        # correct_wrong = row.get(COL_CORRECT_WRONG)
        crop_id       = row.get(COL_CROP_ID)

        # Skip rows with no mandi name
        if not mandi_name:
            continue

        mandi_name = str(mandi_name).strip()
        crop_id    = str(crop_id).strip() if crop_id else None
        crop_name  = str(crop_name).strip() if crop_name else None

        # Use crop_id as group key; fall back to crop_name if no id
        group_key = crop_id or crop_name or mandi_name

        g = groups[group_key]
        g["crop_master_id"] = crop_id
        g["canonical_name"] = crop_name or mandi_name  # prefer crop_master name

        # Add mandi name as alias
        g["aliases"].add(mandi_name.lower())

        # Add crop_name as alias too
        if crop_name:
            g["aliases"].add(crop_name.lower())

        # Add similar name if present
        if similar_name:
            similar_name = str(similar_name).strip()
            g["aliases"].add(similar_name.lower())

        # Detect language variants (non-ASCII = likely Hindi/regional script)
        if any(ord(c) > 127 for c in mandi_name):
            g["language_variants"].add(mandi_name)

        # Handle correct/wrong column
        # if correct_wrong:
        #     val = str(correct_wrong).strip().lower()
        #     if val == "wrong":
        #         g["confidence"] = min(g["confidence"], 0.5)
        #         g["notes"].append(f"Row {i}: marked as wrong match")
        #     elif val == "correct":
        #         pass  # keep confidence as is
        #     else:
        #         g["notes"].append(f"Row {i}: correct/wrong = '{correct_wrong}'")

        # No crop_id = unresolved
        if not crop_id:
            g["confidence"] = 0.0
            g["notes"].append(f"Row {i}: no crop_master_id found")

    # Convert to final document list
    now = datetime.now(timezone.utc)
    docs = []
    for group_key, g in groups.items():
        doc = {
            "crop_master_id":    g["crop_master_id"],
            "canonical_name":    g["canonical_name"] or group_key,
            "aliases":           sorted(g["aliases"]),
            "language_variants": sorted(g["language_variants"]),
            "confidence":        g["confidence"],
            "notes":             "; ".join(g["notes"]) if g["notes"] else None,
            "active":            True,
            "source_tags":       sorted(g["source_tags"]),
            "createdAt":         now,
            "updatedAt":         now,
        }
        docs.append(doc)

    return docs


# ── MongoDB setup ─────────────────────────────────────────────────────────────
def setup_collection(db):
    existing = db.list_collection_names()
    if COLLECTION not in existing:
        db.create_collection(COLLECTION, **SCHEMA)
        print(f"✅ Created collection: {COLLECTION}")
    else:
        try:
            db.command("collMod", COLLECTION, **SCHEMA)
            print(f"✅ Updated schema on: {COLLECTION}")
        except OperationFailure as e:
            print(f"⚠️  Schema update skipped: {e.details.get('errmsg')}")

    col = db[COLLECTION]

    col.create_index([("aliases", ASCENDING)], name="idx_aliases")
    col.create_index([("crop_master_id", ASCENDING)], name="idx_crop_master_id")
    col.create_index(
        [("confidence", ASCENDING), ("active", ASCENDING)],
        name="idx_confidence_active"
    )
    col.create_index(
        [("canonical_name", TEXT), ("aliases", TEXT), ("language_variants", TEXT)],
        name="idx_text_search",
        weights={"canonical_name": 10, "aliases": 5, "language_variants": 3},
        default_language="none",
    )
    print("✅ Indexes ready")
    return col


# ── Upload ────────────────────────────────────────────────────────────────────
def upload(col, docs: list[dict]):
    inserted = updated = 0
    now = datetime.now(timezone.utc)

    for doc in docs:
        result = col.update_one(
            {"canonical_name": doc["canonical_name"]},  # upsert key
            {
                "$set": {k: v for k, v in doc.items() if k != "createdAt"},
                "$setOnInsert": {"createdAt": now},
            },
            upsert=True,
        )
        if result.upserted_id:
            inserted += 1
        elif result.modified_count:
            updated += 1

    return inserted, updated


# ── Stats ─────────────────────────────────────────────────────────────────────
def print_stats(col):
    total    = col.count_documents({})
    unmapped = col.count_documents({"crop_master_id": None})
    low_conf = col.count_documents({"confidence": {"$lt": 0.8}, "active": True})

    print(f"\n📊 Collection stats:")
    print(f"   Total records   : {total}")
    print(f"   Unmapped (null) : {unmapped}  ← needs manual crop_master_id")
    print(f"   Low confidence  : {low_conf}   ← needs human review")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Upload commodity xlsx to MongoDB")
    parser.add_argument("--file",  default='./file.xlsx', help="Path to xlsx file")
    parser.add_argument("--db",    default=os.getenv("MANDI_DB_NAME", "agri_db"), help="MongoDB database name")
    # parser.add_argument("--sheet", default=None, help="Sheet name (default: first sheet)")
    args = parser.parse_args()

    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")

    # 1. Read xlsx
    print(f"📂 Reading: {args.file}")
    rows = read_xlsx(args.file, sheet_name="Master commodity")

    # 2. Build documents
    docs = build_documents(rows)
    print(f"🔨 Built {len(docs)} unique crop documents\n")

    # 3. Upload
    client = MongoClient(uri)
    db = client[args.db]

    col = setup_collection(db)
    inserted, updated = upload(col, docs)
    print(f"✅ Upload done: {inserted} inserted, {updated} updated")

    print_stats(col)
    client.close()


if __name__ == "__main__":
    main()