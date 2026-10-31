"""Run log and review list built from a scrape run (no database needed)."""
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from review_log import (
    build_review_ops,
    build_run_documents,
    price_problems,
    summarise_documents,
)

NOW = datetime(2026, 10, 31, 6, 0, tzinfo=timezone.utc)


def _doc(**kw) -> dict:
    base = {
        "source_state": "agmarknet", "source_system": "agmarknet", "state": "andhra pradesh",
        "market_name": "pendurthi apmc", "market_id": "m1", "commodity_name": "paddy(common)",
        "commodity_alias_lookup_id": "a1", "variety": "paddy", "date": NOW,
        "min_price": 2400.0, "max_price": 2500.0, "modal_price": 2441.0,
    }
    base.update(kw)
    return base


def test_price_problems_flags_bad_prices_only() -> None:
    assert price_problems(_doc()) == []
    assert price_problems(_doc(min_price=None, max_price=None)) == []
    assert price_problems(_doc(min_price=2600.0)) == ["min price 2600 is above max price 2500"]
    assert price_problems(_doc(modal_price=3000.0)) == ["modal price 3000 is outside 2400–2500"]
    assert price_problems(_doc(min_price=0.0)) == ["min price is 0"]


def test_summary_collects_unmapped_and_unknown() -> None:
    docs = [
        _doc(),
        _doc(commodity_name="paddy-sona", commodity_alias_lookup_id=None),
        _doc(market_name="new mandi", market_id=None),
    ]
    s = summarise_documents(docs)
    assert s["by_source"]["agmarknet"]["records"] == 3
    state = s["by_source"]["agmarknet"]["states"]["andhra pradesh"]
    assert state["markets"] == {"pendurthi apmc", "new mandi"}
    assert set(s["unmapped"]) == {"paddy-sona"}
    assert set(s["unknown_markets"]) == {("andhra pradesh", "new mandi")}


def test_run_documents_per_source_plus_pipeline() -> None:
    merged = {
        "agmarknet": {"success": True, "data": [{}, {}]},
        "Punjab": {"success": False, "error": "Punjab scrape failed", "details": "timeout", "data": []},
    }
    s = summarise_documents([_doc(), _doc()])
    docs = build_run_documents(merged, s, {"price_upserted": 2}, "run1", NOW, NOW, "")
    by_source = {d["source"]: d for d in docs}
    assert by_source["agmarknet"]["records_fetched"] == 2
    assert by_source["agmarknet"]["states"][0] == {
        "state": "andhra pradesh", "records": 2, "markets": 1,
        "unmapped_commodities": 0, "unknown_markets": 0,
    }
    assert by_source["Punjab"]["error"] == "Punjab scrape failed: timeout"
    assert by_source["_pipeline"]["success"] is False
    assert by_source["_pipeline"]["sources_failed"] == ["Punjab"]


def test_review_ops_are_keyed_upserts() -> None:
    merged = {"agmarknet": {"success": True, "data": []}, "Punjab": {"success": False, "error": "x"}}
    s = summarise_documents([_doc(commodity_name="hen", commodity_alias_lookup_id=None, max_price=100.0)])
    ops = build_review_ops(merged, s, "run1", NOW)
    keys = sorted(op._filter["key"] for op in ops)
    assert keys == [
        "price_invalid|agmarknet|andhra pradesh|pendurthi apmc|hen|paddy|2026-10-31",
        "run_failed|Punjab|2026-10-31",
        "unmapped_commodity|hen",
    ]
    assert all(op._upsert for op in ops)
