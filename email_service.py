"""Email service for sending APMC Mandi Scraper cron job notifications."""

from __future__ import annotations

import html
import logging
import os
import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from dotenv import load_dotenv

load_dotenv()

IST = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s IST %(levelname)s [email] %(message)s",
)
log = logging.getLogger(__name__)

# Email Configuration
EMAIL_USER = os.getenv("EMAIL_USER", "zohosyncsage@annam.ai")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtppro.zoho.in")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))


def _is_notifications_enabled() -> bool:
    """Check if email notifications are enabled via environment variables."""
    return os.getenv("EMAIL_NOTIFICATIONS_ENABLED", "true").strip().lower() in (
        "true",
        "1",
        "yes",
    )


def _get_notification_emails() -> list[str]:
    """Parse NOTIFICATION_EMAIL env var into a list of emails.

    Returns:
        List of email addresses (empty if not configured)
    """
    notif_email = os.getenv("NOTIFICATION_EMAIL", "")
    if not notif_email:
        return []
    return [email.strip() for email in notif_email.split(",") if email.strip()]


def _send_email(subject: str, html_content: str, recipients: list[str]) -> bool:
    """Send an HTML email via yagmail (if available) or standard smtplib.

    Args:
        subject: Email subject line
        html_content: Complete HTML string
        recipients: List of recipient email addresses

    Returns:
        True if sent successfully, False otherwise
    """
    if not recipients:
        log.warning("No recipient emails specified.")
        return False

    if not EMAIL_USER or not EMAIL_PASS:
        log.warning("Email configuration missing. Set EMAIL_USER and EMAIL_PASS in .env")
        return False

    # 1. Try yagmail if available
    try:
        import yagmail  # type: ignore

        with yagmail.SMTP(
            EMAIL_USER,
            EMAIL_PASS,
            host=SMTP_SERVER,
            port=SMTP_PORT,
        ) as yag:
            yag.send(
                to=recipients,
                subject=subject,
                contents=html_content,
            )
        log.info("Email notification sent successfully via yagmail to %s", recipients)
        return True
    except ImportError:
        log.debug("yagmail not installed; falling back to built-in smtplib.")
    except Exception as e:
        log.warning("yagmail sending encountered an issue: %s. Trying smtplib fallback...", e)

    # 2. Fallback to standard library smtplib
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(recipients)

        part = MIMEText(html_content, "html", "utf-8")
        msg.attach(part)

        if SMTP_PORT == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context, timeout=30) as server:
                server.login(EMAIL_USER, EMAIL_PASS)
                server.sendmail(EMAIL_USER, recipients, msg.as_string())
        else:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
                server.starttls()
                server.login(EMAIL_USER, EMAIL_PASS)
                server.sendmail(EMAIL_USER, recipients, msg.as_string())

        log.info("Email notification sent successfully via smtplib to %s", recipients)
        return True
    except Exception as e:
        log.error("Failed to send email notification: %s", str(e))
        return False


def send_cron_notification(
    merged: dict[str, Any] | None = None,
    normalised_count: int = 0,
    upload_stats: dict[str, Any] | None = None,
    start_time: datetime | None = None,
    elapsed_seconds: float | None = None,
    error: str | None = None,
    date_str: str = "",
) -> bool:
    """Send a comprehensive email notification after the APMC Mandi cron job runs.

    Args:
        merged: Dictionary mapping source names to their scraping result payload
        normalised_count: Number of documents normalised for database insertion
        upload_stats: Statistics returned from database.upload_to_mongo
        start_time: Execution start time (defaults to now IST)
        elapsed_seconds: Pipeline runtime in seconds
        error: Top-level error message if the pipeline failed
        date_str: Date parameter passed to the scraper (if any)

    Returns:
        True if sent successfully, False otherwise
    """
    if not _is_notifications_enabled():
        log.info("Email notifications disabled via EMAIL_NOTIFICATIONS_ENABLED env var.")
        return False

    recipients = _get_notification_emails()
    if not recipients:
        log.warning("No notification emails configured. Set NOTIFICATION_EMAIL in .env")
        return False

    run_time = start_time or datetime.now(IST)
    if run_time.tzinfo is None:
        run_time = run_time.replace(tzinfo=IST)

    merged = merged or {}
    upload_stats = upload_stats or {}

    # Calculate metrics
    total_sources = len(merged)
    successful_sources = sum(
        1 for s, p in merged.items() if isinstance(p, dict) and bool(p.get("success"))
    )
    failed_sources = max(0, total_sources - successful_sources)
    total_scraped = sum(
        len(p.get("data") or []) for p in merged.values() if isinstance(p, dict)
    )

    # Determine overall status
    if error:
        status_tag = "FAILED"
        status_color = "#EF4444"
        badge_text = "✗ FAILED"
    elif total_sources > 0 and failed_sources == 0:
        status_tag = "SUCCESS"
        status_color = "#10B981"
        badge_text = "✓ SUCCESS"
    elif successful_sources > 0:
        status_tag = "PARTIAL"
        status_color = "#F59E0B"
        badge_text = "⚠ PARTIAL"
    else:
        status_tag = "FAILED"
        status_color = "#EF4444"
        badge_text = "✗ FAILED"

    # Build subject
    date_suffix = f" for {date_str}" if date_str else ""
    subject = (
        f"[{status_tag}] APMC Mandi Cron Run{date_suffix} - "
        f"{total_scraped:,} Records ({run_time.strftime('%Y-%m-%d %H:%M')} IST)"
    )

    html_content = _build_cron_report_html(
        merged=merged,
        normalised_count=normalised_count,
        upload_stats=upload_stats,
        run_time=run_time,
        elapsed_seconds=elapsed_seconds,
        status_tag=status_tag,
        status_color=status_color,
        badge_text=badge_text,
        total_scraped=total_scraped,
        total_sources=total_sources,
        successful_sources=successful_sources,
        error=error,
        date_str=date_str,
    )

    return _send_email(subject, html_content, recipients)


def send_error_notification(
    error: str,
    traceback_str: str | None = None,
    run_time: datetime | None = None,
) -> bool:
    """Send an immediate alert email when the cron pipeline crashes unexpectedly."""
    if not _is_notifications_enabled():
        return False

    recipients = _get_notification_emails()
    if not recipients:
        return False

    run_time = run_time or datetime.now(IST)
    if run_time.tzinfo is None:
        run_time = run_time.replace(tzinfo=IST)

    subject = f"[ALERT] APMC Mandi Pipeline Crashed - {run_time.strftime('%Y-%m-%d %H:%M')} IST"
    html_content = _build_crash_html(error, traceback_str, run_time)

    return _send_email(subject, html_content, recipients)


def _format_duration(seconds: float | None) -> str:
    """Format elapsed seconds into a human-friendly string."""
    if seconds is None:
        return "N/A"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    rem_seconds = seconds % 60
    return f"{minutes}m {rem_seconds:.1f}s"


def _build_cron_report_html(
    merged: dict[str, Any],
    normalised_count: int,
    upload_stats: dict[str, Any],
    run_time: datetime,
    elapsed_seconds: float | None,
    status_tag: str,
    status_color: str,
    badge_text: str,
    total_scraped: int,
    total_sources: int,
    successful_sources: int,
    error: str | None,
    date_str: str = "",
) -> str:
    """Build a comprehensive, modern HTML email body for cron scraper runs."""
    formatted_time = run_time.strftime("%B %d, %Y at %I:%M %p")
    duration_str = _format_duration(elapsed_seconds)
    failed_sources = max(0, total_sources - successful_sources)

    # Database metrics
    master_upserted = upload_stats.get("master_upserted", 0)
    master_matched = upload_stats.get("master_matched", 0)
    master_errors = upload_stats.get("master_errors", 0)
    price_upserted = upload_stats.get("price_upserted", 0)
    price_matched = upload_stats.get("price_matched", 0)
    price_errors = upload_stats.get("price_errors", 0)
    unresolved = upload_stats.get("unresolved_combos", 0)
    skipped_deg = upload_stats.get("skipped_degenerate", 0)

    # Build per-source rows
    source_rows_html = []
    # Sort sources alphabetically with Agmarknet first
    sorted_sources = sorted(
        merged.keys(),
        key=lambda k: ("0_" if k.lower() == "agmarknet" else "1_" + k.lower()),
    )

    for source in sorted_sources:
        payload = merged[source]
        if not isinstance(payload, dict):
            source_rows_html.append(
                f"""<tr>
<td style="padding:10px 12px;border-bottom:1px solid #E5E7EB;font-weight:600;color:#1F2937;">{html.escape(source)}</td>
<td style="padding:10px 12px;border-bottom:1px solid #E5E7EB;text-align:center;">
<span style="background:#FEE2E2;color:#DC2626;font-size:10px;font-weight:bold;padding:3px 8px;border-radius:12px;">FAILED</span>
</td>
<td style="padding:10px 12px;border-bottom:1px solid #E5E7EB;text-align:right;color:#4B5563;">0</td>
<td style="padding:10px 12px;border-bottom:1px solid #E5E7EB;font-size:11px;color:#DC2626;">Invalid payload structure</td>
</tr>"""
            )
            continue

        success = bool(payload.get("success"))
        data_list = payload.get("data") or []
        records_count = len(data_list)
        err_msg = str(payload.get("error") or "")
        details_msg = str(payload.get("details") or "")

        if err_msg and details_msg and details_msg != err_msg:
            full_err = f"{err_msg}: {details_msg}"
        else:
            full_err = err_msg or details_msg

        if success:
            badge_html = '<span style="background:#D1FAE5;color:#059669;font-size:10px;font-weight:bold;padding:3px 8px;border-radius:12px;">OK</span>'
            status_desc = '<span style="color:#059669;font-size:11px;">Scraped & Ingested</span>'
        elif records_count == 0 and "skipped" in full_err.lower():
            badge_html = '<span style="background:#F3F4F6;color:#6B7280;font-size:10px;font-weight:bold;padding:3px 8px;border-radius:12px;">SKIPPED</span>'
            status_desc = f'<span style="color:#6B7280;font-size:11px;">{html.escape(full_err[:120])}</span>'
        else:
            badge_html = '<span style="background:#FEE2E2;color:#DC2626;font-size:10px;font-weight:bold;padding:3px 8px;border-radius:12px;">FAILED</span>'
            status_desc = f'<span style="color:#DC2626;font-size:11px;">{html.escape(full_err[:120])}</span>' if full_err else '<span style="color:#DC2626;font-size:11px;">Failed</span>'

        source_display = "Agmarknet (National)" if source.lower() == "agmarknet" else source

        source_rows_html.append(
            f"""<tr>
<td style="padding:10px 12px;border-bottom:1px solid #E5E7EB;font-weight:600;color:#1F2937;">{html.escape(source_display)}</td>
<td style="padding:10px 12px;border-bottom:1px solid #E5E7EB;text-align:center;">{badge_html}</td>
<td style="padding:10px 12px;border-bottom:1px solid #E5E7EB;text-align:right;font-weight:600;color:#111827;">{records_count:,}</td>
<td style="padding:10px 12px;border-bottom:1px solid #E5E7EB;">{status_desc}</td>
</tr>"""
        )

    sources_table_content = "\n".join(source_rows_html)

    # Optional top-level error box
    error_box_html = ""
    if error:
        error_escaped = html.escape(error)
        error_box_html = f"""<div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:6px;padding:12px;margin:16px 0;font-family:Consolas,Monaco,monospace;font-size:11px;color:#991B1B;white-space:pre-wrap;">
<strong>Pipeline Alert:</strong> {error_escaped}
</div>"""

    # Sources ratio color
    sources_color = "#10B981" if failed_sources == 0 else ("#F59E0B" if successful_sources > 0 else "#EF4444")

    target_date_label = f" • Target Date: <strong>{html.escape(date_str)}</strong>" if date_str else ""

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>APMC Mandi Scraper Report</title>
</head>
<body style="margin:0;padding:0;background:#F3F4F6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;-webkit-font-smoothing:antialiased;">
<div style="max-width:620px;margin:20px auto;background:#FFFFFF;border-radius:8px;overflow:hidden;box-shadow:0 4px 12px rgba(0,0,0,0.06);border:1px solid #E5E7EB;">

<!-- Header Banner -->
<div style="background:linear-gradient(135deg, #1E3A8A 0%, #3B82F6 100%);padding:20px 24px;color:#FFFFFF;">
<table width="100%" cellpadding="0" cellspacing="0" border="0">
<tr>
<td>
  <div style="font-size:18px;font-weight:700;letter-spacing:-0.2px;line-height:1.2;">APMC Mandi Scraper Run</div>
  <div style="font-size:12px;opacity:0.9;margin-top:4px;">{formatted_time} IST{target_date_label}</div>
</td>
<td style="text-align:right;vertical-align:middle;">
  <span style="background:{status_color};color:#FFFFFF;font-size:11px;font-weight:bold;padding:5px 12px;border-radius:14px;letter-spacing:0.5px;box-shadow:0 2px 4px rgba(0,0,0,0.1);">{badge_text}</span>
  <div style="font-size:11px;opacity:0.85;margin-top:5px;">Duration: {duration_str}</div>
</td>
</tr>
</table>
</div>

<!-- Main Body -->
<div style="padding:20px 24px;">

{error_box_html}

<!-- Key Metric Cards -->
<div style="margin-bottom:24px;">
<table width="100%" cellpadding="0" cellspacing="0" border="0">
<tr>

<td width="24%" style="padding-right:6px;">
  <table width="100%" style="background:#F9FAFB;border:1px solid #E5E7EB;border-radius:6px;text-align:center;">
    <tr><td style="padding:12px 6px;">
      <div style="font-size:19px;font-weight:700;color:#1E3A8A;">{total_scraped:,}</div>
      <div style="font-size:10px;font-weight:600;color:#4B5563;text-transform:uppercase;letter-spacing:0.4px;margin-top:2px;">Total Data Scraped</div>
      <div style="font-size:9px;color:#9CA3AF;margin-top:1px;">Raw Mandi Records</div>
    </td></tr>
  </table>
</td>

<td width="24%" style="padding:0 3px;">
  <table width="100%" style="background:#F9FAFB;border:1px solid #E5E7EB;border-radius:6px;text-align:center;">
    <tr><td style="padding:12px 6px;">
      <div style="font-size:19px;font-weight:700;color:#4F46E5;">{normalised_count:,}</div>
      <div style="font-size:10px;font-weight:600;color:#4B5563;text-transform:uppercase;letter-spacing:0.4px;margin-top:2px;">Cleaned & Validated</div>
      <div style="font-size:9px;color:#9CA3AF;margin-top:1px;">Standardized Format</div>
    </td></tr>
  </table>
</td>

<td width="24%" style="padding:0 3px;">
  <table width="100%" style="background:#F9FAFB;border:1px solid #E5E7EB;border-radius:6px;text-align:center;">
    <tr><td style="padding:12px 6px;">
      <div style="font-size:19px;font-weight:700;color:{sources_color};">{successful_sources}/{total_sources}</div>
      <div style="font-size:10px;font-weight:600;color:#4B5563;text-transform:uppercase;letter-spacing:0.4px;margin-top:2px;">Scrapers Succeeded</div>
      <div style="font-size:9px;color:#9CA3AF;margin-top:1px;">Active Portals</div>
    </td></tr>
  </table>
</td>

<td width="28%" style="padding-left:6px;">
  <table width="100%" style="background:#F9FAFB;border:1px solid #E5E7EB;border-radius:6px;text-align:center;">
    <tr><td style="padding:12px 6px;">
      <div style="font-size:19px;font-weight:700;color:#059669;">+{price_upserted:,}</div>
      <div style="font-size:10px;font-weight:600;color:#4B5563;text-transform:uppercase;letter-spacing:0.4px;margin-top:2px;">New Prices Saved</div>
      <div style="font-size:9px;color:#9CA3AF;margin-top:1px;">Written to Database</div>
    </td></tr>
  </table>
</td>

</tr>
</table>
</div>

<!-- Section: Source Breakdown -->
<div style="margin-bottom:20px;">
<div style="font-size:13px;font-weight:700;color:#374151;margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;">
Source Portal Scraper Breakdown
</div>

<div style="border:1px solid #E5E7EB;border-radius:6px;overflow:hidden;">
<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-size:12px;background:#FFFFFF;">
<thead>
<tr style="background:#F9FAFB;color:#4B5563;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">
<th style="padding:8px 12px;text-align:left;border-bottom:1px solid #E5E7EB;">Source / State</th>
<th style="padding:8px 12px;text-align:center;border-bottom:1px solid #E5E7EB;">Scrape Status</th>
<th style="padding:8px 12px;text-align:right;border-bottom:1px solid #E5E7EB;">Total Data Scraped</th>
<th style="padding:8px 12px;text-align:left;border-bottom:1px solid #E5E7EB;">Details / Remarks</th>
</tr>
</thead>
<tbody>
{sources_table_content}
</tbody>
</table>
</div>
</div>

<!-- Section: Database Ingestion Breakdown -->
<div style="margin-bottom:16px;">
<div style="font-size:13px;font-weight:700;color:#374151;margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;">
MongoDB Database Ingestion Summary
</div>

<div style="background:#F9FAFB;border:1px solid #E5E7EB;border-radius:6px;padding:12px 16px;font-size:12px;color:#374151;">
<table width="100%" cellpadding="0" cellspacing="0" border="0">
<tr>
<td width="50%" style="vertical-align:top;padding-right:12px;border-right:1px solid #E5E7EB;">
  <div style="font-weight:600;color:#1F2937;margin-bottom:6px;">Mandi & Commodity Catalog (<code>markets_commodities</code>)</div>
  <div style="line-height:1.6;font-size:11px;color:#4B5563;">
    • New Combos Added: <strong style="color:#111827;">{master_upserted:,}</strong><br>
    • Existing Combos Matched: <strong style="color:#111827;">{master_matched:,}</strong><br>
    • Write Errors: <strong style="color:{'#DC2626' if master_errors else '#4B5563'};">{master_errors}</strong>
  </div>
</td>
<td width="50%" style="vertical-align:top;padding-left:12px;">
  <div style="font-weight:600;color:#1F2937;margin-bottom:6px;">Daily Price Records (<code>price_records</code>)</div>
  <div style="line-height:1.6;font-size:11px;color:#4B5563;">
    • New Price Records Saved: <strong style="color:#059669;">+{price_upserted:,}</strong><br>
    • Existing Records Updated: <strong style="color:#111827;">{price_matched:,}</strong><br>
    • Unresolved / Skipped: <strong style="color:{'#DC2626' if unresolved else '#4B5563'};">{unresolved} / {skipped_deg}</strong>
  </div>
</td>
</tr>
</table>
</div>
</div>

</div>

<!-- Footer -->
<div style="background:#F9FAFB;border-top:1px solid #E5E7EB;padding:12px 24px;text-align:center;">
<p style="margin:0;font-size:11px;color:#6B7280;font-weight:500;">Ajrasakha Agri Platform • APMC Mandi Ingestion Pipeline</p>
<p style="margin:4px 0 0;font-size:10px;color:#9CA3AF;">Automated cron notification delivered via Zoho SMTP</p>
</div>

</div>
</body>
</html>"""


def _build_crash_html(error: str, traceback_str: str | None, run_time: datetime) -> str:
    """Build HTML body for critical pipeline crashes."""
    formatted_time = run_time.strftime("%B %d, %Y at %I:%M %p")
    error_escaped = html.escape(error)
    tb_html = ""
    if traceback_str:
        tb_escaped = html.escape(traceback_str)
        tb_html = f"""<div style="margin-top:12px;background:#1F2937;color:#F9FAFB;padding:12px;border-radius:4px;font-family:Consolas,Monaco,monospace;font-size:10px;overflow-x:auto;white-space:pre-wrap;">{tb_escaped}</div>"""

    return f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#F4F5F7;font-family:Arial,sans-serif;">
<div style="max-width:560px;margin:20px auto;background:#FFFFFF;border-radius:8px;overflow:hidden;border:1px solid #FECACA;">
<div style="background:#DC2626;padding:14px 20px;color:#FFFFFF;">
  <span style="font-size:15px;font-weight:700;">⚠ APMC Mandi Pipeline Critical Failure</span>
  <p style="margin:4px 0 0;font-size:11px;opacity:0.9;">{formatted_time} IST</p>
</div>
<div style="padding:16px 20px;">
  <div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:4px;padding:12px;font-family:Consolas,Monaco,monospace;font-size:11px;color:#991B1B;white-space:pre-wrap;">
    <strong>Error:</strong> {error_escaped}
  </div>
  {tb_html}
</div>
<div style="border-top:1px solid #E5E7EB;padding:10px 20px;text-align:center;background:#F9FAFB;">
  <p style="margin:0;font-size:10px;color:#6B7280;">Ajrasakha Agri Platform • Emergency Notification</p>
</div>
</div>
</body>
</html>"""


if __name__ == "__main__":
    print("Testing APMC Mandi Cron Email Notification...")
    print(f"  EMAIL_USER: {EMAIL_USER}")
    print(f"  SMTP_SERVER: {SMTP_SERVER}:{SMTP_PORT}")
    recipients = _get_notification_emails()
    print(f"  NOTIFICATION_EMAIL: {recipients}")

    if not recipients:
        print("\n[!] Please set NOTIFICATION_EMAIL in your .env file to test sending to your email.")
    elif not EMAIL_PASS:
        print("\n[!] Please set EMAIL_PASS in your .env file with your SMTP app password.")
    else:
        sample_merged = {
            "agmarknet": {"success": True, "data": [{"id": i} for i in range(18420)]},
            "Karnataka": {"success": True, "data": [{"id": i} for i in range(2310)]},
            "Maharashtra": {"success": True, "data": [{"id": i} for i in range(4150)]},
            "Meghalaya": {"success": True, "data": [{"id": i} for i in range(85)]},
            "Nagaland": {"success": True, "data": [{"id": i} for i in range(120)]},
            "Punjab": {"success": True, "data": [{"id": i} for i in range(1890)]},
            "Uttar Pradesh": {"success": True, "data": [{"id": i} for i in range(1515)]},
        }
        sample_stats = {
            "master_upserted": 420,
            "master_matched": 3850,
            "master_errors": 0,
            "price_upserted": 22100,
            "price_matched": 6390,
            "price_errors": 0,
            "unresolved_combos": 0,
            "skipped_degenerate": 0,
        }

        print("\nSending sample test email...")
        ok = send_cron_notification(
            merged=sample_merged,
            normalised_count=28490,
            upload_stats=sample_stats,
            elapsed_seconds=48.6,
            date_str=datetime.now(IST).strftime("%d/%m/%Y"),
        )
        if ok:
            print(f"[SUCCESS] Test email successfully sent to {recipients}!")
        else:
            print("[FAILED] Could not send test email. Check logs above for details.")
