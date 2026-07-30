"""
Tests for the agmarknet run2 scraper helpers.

The old rate-limiting tests (_get_retry_delay) are removed because the
scraper now uses a simple curl subprocess call via data.gov.in and no
longer has custom HTTP retry logic.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from markets.agmarknet.run2 import _to_dd_mm_yyyy


def test_iso_date_converts_to_dd_mm_yyyy() -> None:
    assert _to_dd_mm_yyyy("2026-07-28") == "28/07/2026"


def test_dd_mm_yyyy_passthrough() -> None:
    assert _to_dd_mm_yyyy("28/07/2026") == "28/07/2026"


def test_none_returns_today_format() -> None:
    result = _to_dd_mm_yyyy(None)
    # Should be DD/MM/YYYY format
    parts = result.split("/")
    assert len(parts) == 3
    assert len(parts[0]) == 2  # DD
    assert len(parts[1]) == 2  # MM
    assert len(parts[2]) == 4  # YYYY
