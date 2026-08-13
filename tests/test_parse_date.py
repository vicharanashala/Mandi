"""ISO dates must not be parsed as YYYY-DD-MM via dateutil dayfirst."""
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database import _parse_date, _swap_dayfirst_iso_misparse, _is_future_date


def test_iso_yyyy_mm_dd_not_swapped() -> None:
    dt = _parse_date("2026-07-09")
    assert dt == datetime(2026, 7, 9, tzinfo=timezone.utc)


def test_iso_with_time_prefix() -> None:
    dt = _parse_date("2026-07-09T18:30:00Z")
    assert dt.date() == datetime(2026, 7, 9).date()


def test_compact_yyyymmdd() -> None:
    dt = _parse_date("20260709")
    assert dt == datetime(2026, 7, 9, tzinfo=timezone.utc)


def test_indian_slash_date_still_dayfirst() -> None:
    dt = _parse_date("09/07/2026")
    assert dt.date() == datetime(2026, 7, 9).date()


def test_swap_undoes_dayfirst_iso_misparse() -> None:
    wrong = datetime(2026, 9, 7, tzinfo=timezone.utc)
    assert _swap_dayfirst_iso_misparse(wrong).date() == datetime(2026, 7, 9).date()


def test_swap_leaves_unambiguous_day_alone() -> None:
    ok = datetime(2026, 7, 22, tzinfo=timezone.utc)
    assert _swap_dayfirst_iso_misparse(ok).date() == ok.date()


def test_future_date_guard() -> None:
    today = datetime(2026, 8, 13, tzinfo=timezone.utc)
    assert _is_future_date(datetime(2026, 9, 7, tzinfo=timezone.utc), today=today)
    assert not _is_future_date(datetime(2026, 8, 11, tzinfo=timezone.utc), today=today)
