"""Tests for the APMC Mandi Scraper email notification service."""

import os
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from email_service import (
    IST,
    _get_notification_emails,
    _format_duration,
    _build_cron_report_html,
    _build_crash_html,
    send_cron_notification,
    send_error_notification,
)


def test_get_notification_emails_empty(monkeypatch):
    monkeypatch.setenv("NOTIFICATION_EMAIL", "")
    assert _get_notification_emails() == []


def test_get_notification_emails_parsed(monkeypatch):
    monkeypatch.setenv(
        "NOTIFICATION_EMAIL",
        "dev1@example.com, dev2@example.com , , dev3@example.com",
    )
    assert _get_notification_emails() == [
        "dev1@example.com",
        "dev2@example.com",
        "dev3@example.com",
    ]


def test_format_duration():
    assert _format_duration(None) == "N/A"
    assert _format_duration(42.34) == "42.3s"
    assert _format_duration(125.5) == "2m 5.5s"


def test_build_cron_report_html_structure():
    merged_sample = {
        "agmarknet": {
            "success": True,
            "data": [{"id": 1}, {"id": 2}],
        },
        "Karnataka": {
            "success": True,
            "data": [{"id": 3}],
        },
        "Maharashtra": {
            "success": False,
            "error": "Connection timeout",
            "details": "Gateway 504",
            "data": [],
        },
    }
    upload_stats_sample = {
        "master_upserted": 10,
        "master_matched": 50,
        "master_errors": 0,
        "price_upserted": 25,
        "price_matched": 100,
        "price_errors": 0,
        "unresolved_combos": 0,
        "skipped_degenerate": 0,
    }

    html = _build_cron_report_html(
        merged=merged_sample,
        normalised_count=3,
        upload_stats=upload_stats_sample,
        run_time=datetime(2026, 8, 31, 16, 30, tzinfo=IST),
        elapsed_seconds=75.4,
        status_tag="PARTIAL",
        status_color="#F59E0B",
        badge_text="⚠ PARTIAL",
        total_scraped=3,
        total_sources=3,
        successful_sources=2,
        error=None,
        date_str="31/08/2026",
    )

    assert "APMC Mandi Scraper Run" in html
    assert "PARTIAL" in html
    assert "Agmarknet (National)" in html
    assert "Karnataka" in html
    assert "Maharashtra" in html
    assert "Gateway 504" in html
    assert "1m 15.4s" in html
    assert "31/08/2026" in html
    assert "markets_commodities" in html
    assert "price_records" in html


def test_send_cron_notification_success(monkeypatch):
    monkeypatch.setenv("NOTIFICATION_EMAIL", "alerts@annam.ai")
    monkeypatch.setenv("EMAIL_USER", "zohosyncsage@annam.ai")
    monkeypatch.setenv("EMAIL_PASS", "mock_pass")
    monkeypatch.setenv("EMAIL_NOTIFICATIONS_ENABLED", "true")

    merged = {
        "agmarknet": {"success": True, "data": [{"id": 1}]},
        "Punjab": {"success": True, "data": [{"id": 2}]},
    }
    upload_stats = {
        "master_upserted": 2,
        "master_matched": 0,
        "master_errors": 0,
        "price_upserted": 2,
        "price_matched": 0,
        "price_errors": 0,
        "unresolved_combos": 0,
        "skipped_degenerate": 0,
    }

    with patch("email_service._send_email", return_value=True) as mock_send:
        res = send_cron_notification(
            merged=merged,
            normalised_count=2,
            upload_stats=upload_stats,
            elapsed_seconds=12.5,
        )
        assert res is True
        mock_send.assert_called_once()
        subject, content, recipients = mock_send.call_args[0]
        assert "[SUCCESS] APMC Mandi Cron Run" in subject
        assert recipients == ["alerts@annam.ai"]


def test_send_cron_notification_disabled(monkeypatch):
    monkeypatch.setenv("NOTIFICATION_EMAIL", "alerts@annam.ai")
    monkeypatch.setenv("EMAIL_NOTIFICATIONS_ENABLED", "false")

    with patch("email_service._send_email") as mock_send:
        res = send_cron_notification(merged={})
        assert res is False
        mock_send.assert_not_called()


def test_send_error_notification(monkeypatch):
    monkeypatch.setenv("NOTIFICATION_EMAIL", "alerts@annam.ai")
    monkeypatch.setenv("EMAIL_USER", "zohosyncsage@annam.ai")
    monkeypatch.setenv("EMAIL_PASS", "mock_pass")
    monkeypatch.setenv("EMAIL_NOTIFICATIONS_ENABLED", "true")

    with patch("email_service._send_email", return_value=True) as mock_send:
        res = send_error_notification(
            error="MongoDB authentication failed",
            traceback_str="Traceback (most recent call last):\n  ...",
        )
        assert res is True
        mock_send.assert_called_once()
        subject, content, recipients = mock_send.call_args[0]
        assert "[ALERT] APMC Mandi Pipeline Crashed" in subject
        assert "MongoDB authentication failed" in content
