"""
v2 name links: scraper upload + migration, against a throwaway MongoDB.

    TEST_MONGO_URI=mongodb://127.0.0.1:27017 pytest tests/test_catalog_v2.py

Skipped when TEST_MONGO_URI is not set. Never point it at a real database:
each test creates and drops its own databases.
"""
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from bson import ObjectId
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "maintenance"))
os.environ.setdefault("MASTER_COLLECTION", "listings")
os.environ.setdefault("PRICE_COLLECTION", "price_records")
os.environ.setdefault("ALL_MANDI_COLLECTION", "available_mandi")

from name_checks import looks_wrong  # noqa: E402

URI = os.getenv("TEST_MONGO_URI")
needs_db = pytest.mark.skipif(not URI, reason="TEST_MONGO_URI not set")


def test_looks_wrong_matches_review_site_rules():
    assert looks_wrong("hen", "groundnut", ["groundnut", "hen"])
    assert looks_wrong("tur", "brinjal", ["brinjal", "tur"])
    assert looks_wrong("other green and fresh vegetables", "paddy", ["paddy"])
    assert not looks_wrong("paddy(common)", "paddy", ["paddy"])
    assert not looks_wrong("soyabean", "soybean", [])
    assert not looks_wrong("मूंगफली (दाना)", "groundnut", [])


@pytest.fixture
def v2db():
    client = MongoClient(URI)
    name = f"test_v2_{uuid.uuid4().hex[:8]}"
    db = client[name]
    yield db, name
    client.drop_database(name)
    client.close()


def _doc(**kw):
    base = {"source_system": "agmarknet", "state": "andhra pradesh", "market_name": "pendurthi apmc",
            "commodity_name": "paddy(common)", "variety": "paddy", "grade": "local",
            "date": datetime(2026, 10, 28, tzinfo=timezone.utc), "min_price": 2400.0, "max_price": 2500.0,
            "modal_price": 2441.0, "source_url": "u", "source_name": "agmarknet", "method": "api"}
    base.update(kw)
    return base


@needs_db
def test_upload_uses_only_approved_links(v2db):
    import database
    db, name = v2db
    product, mandi = ObjectId(), ObjectId()
    db.states.insert_one({"_id": "andhra-pradesh", "name": "andhra pradesh", "spellings": ["andhra pradesh"]})
    db.crop_names.insert_many([
        {"source": "agmarknet", "raw_name": "paddy(common)", "raw_variety": "*", "product_id": product, "status": "approved"},
        {"source": "agmarknet", "raw_name": "hen", "raw_variety": "*", "product_id": None, "status": "rejected"},
    ])
    db.mandi_names.insert_one({"source": "agmarknet", "state_code": "andhra-pradesh", "raw_name": "pendurthi apmc",
                               "mandi_id": mandi, "status": "approved"})

    docs = [_doc(), _doc(commodity_name="paddy-sona"), _doc(commodity_name="hen"), _doc(market_name="new mandi")]
    stats = database.upload_to_mongo(docs, uri=URI, db_name=name, run_id="run-1")

    by = {(l["commodity_name"], l["market_name"]): l for l in db.listings.find()}
    assert by[("paddy(common)", "pendurthi apmc")]["product_id"] == product
    assert by[("paddy(common)", "pendurthi apmc")]["mandi_id"] == mandi
    assert by[("paddy(common)", "pendurthi apmc")]["state_code"] == "andhra-pradesh"
    assert by[("paddy-sona", "pendurthi apmc")]["product_id"] is None
    assert by[("hen", "pendurthi apmc")]["product_id"] is None
    assert by[("paddy(common)", "new mandi")]["mandi_id"] is None

    pending = {d["raw_name"] for d in db.crop_names.find({"status": "pending"})}
    assert pending == {"paddy-sona"}
    assert {d["raw_name"] for d in db.mandi_names.find({"status": "pending"})} == {"new mandi"}
    assert stats["new_crop_names"] == 1 and stats["new_mandi_names"] == 1

    price = db.price_records.find_one()
    assert price["run_id"] == "run-1"
    assert "commodity_alias_lookup_id" not in price and "market_id" not in price

    # Approving the pending name takes effect on the next run, and stays.
    db.crop_names.update_one({"raw_name": "paddy-sona"}, {"$set": {"status": "approved", "product_id": product}})
    database.upload_to_mongo([_doc(commodity_name="paddy-sona")], uri=URI, db_name=name, run_id="run-2")
    assert db.listings.find_one({"commodity_name": "paddy-sona"})["product_id"] == product
    assert db.crop_names.count_documents({"raw_name": "paddy-sona"}) == 1

    # Rejecting a link clears it on the next run.
    db.crop_names.update_one({"raw_name": "paddy(common)"}, {"$set": {"status": "rejected"}})
    database.upload_to_mongo([_doc()], uri=URI, db_name=name, run_id="run-3")
    assert db.listings.find_one({"commodity_name": "paddy(common)", "market_name": "pendurthi apmc"})["product_id"] is None


@needs_db
def test_unknown_state_spelling_is_reported(v2db):
    import database
    db, name = v2db
    stats = database.upload_to_mongo([_doc(state="atlantis")], uri=URI, db_name=name, run_id="r")
    assert stats["unknown_states"] == ["atlantis"]
    assert db.listings.find_one()["state_code"] is None


@needs_db
def test_migration_reads_v1_and_builds_v2():
    client = MongoClient(URI)
    src, dst = f"test_v1_{uuid.uuid4().hex[:8]}", f"test_v2_{uuid.uuid4().hex[:8]}"
    v1 = client[src]
    try:
        paddy, groundnut, cattle = ObjectId(), ObjectId(), ObjectId()
        pendurthi = ObjectId()
        v1.commodity_alias_lookup.insert_many([
            {"_id": paddy, "canonical_name": "paddy", "aliases": ["paddy", "paddy(common)", "rice"]},
            {"_id": groundnut, "canonical_name": "groundnut", "aliases": ["groundnut", "hen"]},
            {"_id": cattle, "canonical_name": "cattle", "aliases": ["cattle", "rice"]},
        ])
        v1.available_mandi.insert_one({"_id": pendurthi, "name": "pendurthi apmc", "state": "andhra pradesh", "aliases": []})
        mc = [
            {"_id": ObjectId(), "source_system": "agmarknet", "state": "andhra pradesh", "market_name": "pendurthi apmc",
             "commodity_name": "paddy(common)", "variety": "paddy", "commodity_alias_lookup_id": paddy, "market_id": pendurthi},
            {"_id": ObjectId(), "source_system": "agmarknet", "state": "andhra pradesh", "market_name": "pendurthi rbz",
             "commodity_name": "hen", "variety": None, "commodity_alias_lookup_id": groundnut, "market_id": pendurthi},
            {"_id": ObjectId(), "source_system": "agmarknet", "state": "keralam", "market_name": "x",
             "commodity_name": "rice", "variety": None, "commodity_alias_lookup_id": paddy, "market_id": None},
        ]
        v1.markets_commodities.insert_many(mc)
        v1.price_records.insert_one({"market_commodity_id": mc[0]["_id"], "date": datetime(2026, 10, 28),
                                     "modal_price": 2441.0, "commodity_alias_lookup_id": paddy, "market_id": pendurthi})
        before = {c: list(v1[c].find()) for c in v1.list_collection_names()}

        out = subprocess.run(
            [sys.executable, str(ROOT / "maintenance" / "migrate_v2.py"), "--uri", URI, "--source", src, "--target", dst],
            capture_output=True, text=True,
        )
        assert out.returncode == 0, out.stdout + out.stderr

        v2 = client[dst]
        # v1 untouched
        assert {c: list(v1[c].find()) for c in v1.list_collection_names()} == before
        # same listing ids, prices copied without the copied ids
        assert {d["_id"] for d in v2.listings.find()} == {d["_id"] for d in mc}
        price = v2.price_records.find_one()
        assert price["market_commodity_id"] == mc[0]["_id"] and "commodity_alias_lookup_id" not in price
        names = {d["raw_name"]: d for d in v2.crop_names.find()}
        assert names["paddy(common)"]["status"] == "approved"
        assert names["hen"]["status"] == "pending"          # looks wrong under groundnut
        assert names["rice"]["status"] == "pending"         # in two groups
        mandis = {d["raw_name"]: d for d in v2.mandi_names.find()}
        assert mandis["pendurthi apmc"]["status"] == "approved"
        assert mandis["pendurthi rbz"]["status"] == "pending"   # matched only by a guess in v1
        listing = v2.listings.find_one({"_id": mc[0]["_id"]})
        assert listing["state_code"] == "andhra-pradesh" and listing["mandi_id"] == pendurthi
        assert listing["product_id"] is not None and listing["legacy"]["commodity_alias_lookup_id"] == paddy
        assert v2.listings.find_one({"_id": mc[2]["_id"]})["state_code"] == "keralam"
        assert v2.states.find_one({"_id": "keralam"})["spellings"] == ["kerala", "keralam"]
    finally:
        client.drop_database(src)
        client.drop_database(dst)
        client.close()
