"""
othermarkets/run.py
===================
Single-file aggregator for ALL state-level APMC mandi scrapers,
EXCLUDING agmarknet.

All scraper logic is written INLINE here — no imports from other
market modules — so you have every function in one place.

States covered
--------------
1. Karnataka      – Playwright (async) → krama.karnataka.gov.in
2. Maharashtra    – requests (sync)    → msamb.com
3. Meghalaya      – requests + BS4     → megamb.gov.in  (paginated ASP.NET)
4. Nagaland       – Playwright (async) → commodityonline.com
5. Punjab         – requests (sync)    → api.emandikaran-pb.in
6. Uttar Pradesh  – requests (sync)    → upkrishivipran.in
7. Andhra Pradesh – requests (sync)    → agriculture.ap.gov.in

Usage
-----
    # Run as a script — saves othermarkets_data_YYYYMMDD.json
    python markets/othermarkets/run.py

    # Or call programmatically
    import asyncio
    from markets.othermarkets.run import run_all_scrapers
    results = asyncio.run(run_all_scrapers())
"""

# ── Standard library ───────────────────────────────────────────────────────────
import asyncio
import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import quote

# ── Third-party ────────────────────────────────────────────────────────────────
import httpx
import requests
import urllib3
from bs4 import BeautifulSoup

# ── Project helpers (parsers / data tables) ────────────────────────────────────
# These are utility modules shared across the project; they are NOT other
# market scrapers, so they are safe to import here.
from tools.html_parser import karnataka_mandi_parser       # Karnataka HTML → list[dict]
from tools.maha_html_parse import maharashtra_parse        # Maharashtra HTML → list[dict]
from tools.nagaland_html_parser import extract_apmc_prices # Nagaland HTML → JSON str
from utils.maharashtra import mandi_code                   # Maharashtra APMC code list
from utils.up_utils import up_product_category, up_mandi_code  # UP reference data

# Suppress InsecureRequestWarning from Meghalaya (uses verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Root logger ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# 1. KARNATAKA  (Async / Playwright)
# Source : https://krama.karnataka.gov.in/Reports/Main_rep
# Method : Playwright headless Chromium → fill date → select "State Level
#           Daily Report" radio → click View → click "All" → parse HTML.
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_karnataka_state_daily(
    date_str: str = "",
    commodity: str = "",
) -> dict[str, Any]:
    """
    Scrape Karnataka State Level Daily Report via Playwright.

    Parameters
    ----------
    date_str : str
        Date in dd/MM/yyyy format. Defaults to today.
    commodity : str
        Optional commodity filter (partial match, unused here but kept for
        forward compatibility with the HTML parser).

    Returns
    -------
    dict
        Parsed mandi data or an error dict on failure.
    """
    # Import Playwright inside the function to avoid import-time failures if
    # Playwright is not installed in the environment.
    from playwright.async_api import async_playwright

    # Default to today if no date supplied
    if not date_str:
        date_str = date.today().strftime("%d/%m/%Y")

    playwright_instance = None
    browser = None

    # Retry the whole Playwright session up to 3 times to handle transient
    # CI network timeouts that are common when hitting slow government sites.
    _KARNATAKA_MAX_RETRIES = 3
    _KARNATAKA_GOTO_TIMEOUT = 120_000  # 120 s — government sites are slow on CI

    last_exc: Exception | None = None

    async def goto_with_fallback(page, url: str, initial_timeout: int) -> None:
        """
        Navigate to URL with fallback strategy:
        1. Try domcontentloaded (strict)
        2. Fall back to load (window.onload)
        3. Fall back to commit + wait (minimal)
        """
        last_error = None
        
        # Attempt 1: domcontentloaded
        try:
            logger.debug(f"[Karnataka] Navigation attempt 1/3: domcontentloaded (timeout: {initial_timeout}ms)")
            await page.goto(url, timeout=initial_timeout, wait_until="domcontentloaded")
            logger.debug("[Karnataka] ✓ domcontentloaded succeeded")
            return
        except Exception as e:
            last_error = e
            logger.debug(f"[Karnataka] domcontentloaded failed: {type(e).__name__}")
        
        # Attempt 2: load (window.onload)
        try:
            logger.debug(f"[Karnataka] Navigation attempt 2/3: load (timeout: 60000ms)")
            await page.goto(url, timeout=60000, wait_until="load")
            logger.debug("[Karnataka] ✓ load succeeded")
            return
        except Exception as e:
            last_error = e
            logger.debug(f"[Karnataka] load failed: {type(e).__name__}")
        
        # Attempt 3: commit + manual wait
        try:
            logger.debug(f"[Karnataka] Navigation attempt 3/3: commit + manual wait")
            await page.goto(url, timeout=10000, wait_until="commit")
            # Give page extra time to stabilize
            await page.wait_for_timeout(3000)
            logger.debug("[Karnataka] ✓ commit + manual wait succeeded")
            return
        except Exception as e:
            last_error = e
            logger.debug(f"[Karnataka] commit failed: {type(e).__name__}")
        
        # All strategies failed
        raise last_error or Exception("Navigation failed with all strategies")

    async def expect_navigation_with_fallback(page, timeout: int, action_coro):
        """
        Execute action and expect navigation with fallback strategies.
        Falls back gracefully if domcontentloaded doesn't fire.
        """
        last_error = None
        
        # Attempt 1: domcontentloaded
        try:
            async with page.expect_navigation(timeout=timeout, wait_until="domcontentloaded"):
                await action_coro()
            logger.debug("[Karnataka] ✓ Navigation with domcontentloaded succeeded")
            return
        except Exception as e:
            last_error = e
            logger.debug(f"[Karnataka] Navigation domcontentloaded failed: {type(e).__name__}")
        
        # Attempt 2: load
        try:
            async with page.expect_navigation(timeout=60000, wait_until="load"):
                await action_coro()
            logger.debug("[Karnataka] ✓ Navigation with load succeeded")
            return
        except Exception as e:
            last_error = e
            logger.debug(f"[Karnataka] Navigation load failed: {type(e).__name__}")
        
        # Attempt 3: commit + wait
        try:
            async with page.expect_navigation(timeout=10000, wait_until="commit"):
                await action_coro()
            await page.wait_for_timeout(2000)
            logger.debug("[Karnataka] ✓ Navigation with commit succeeded")
            return
        except Exception as e:
            last_error = e
            logger.debug(f"[Karnataka] Navigation commit failed: {type(e).__name__}")
        
        # If all explicit navigation waits fail, just do the action and wait for stability
        try:
            logger.debug("[Karnataka] All navigation waits failed, trying action-only approach")
            await action_coro()
            await page.wait_for_timeout(3000)  # Simple wait for stability
            logger.debug("[Karnataka] ✓ Action completed, waited for stability")
            return
        except Exception as e:
            raise e

    for attempt in range(1, _KARNATAKA_MAX_RETRIES + 1):
        playwright_instance = None
        browser = None
        try:
            logger.info(f"[Karnataka] Attempt {attempt}/{_KARNATAKA_MAX_RETRIES}")
            playwright_instance = await async_playwright().start()

            # Launch headless Chromium with sandbox disabled (required in CI/Docker)
            browser = await playwright_instance.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-setuid-sandbox",
                    "--disable-extensions",
                    "--single-process",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=site-per-process",
                    "--window-size=1440,1200",
                ],
            )

            context = await browser.new_context(
                viewport={"width": 1440, "height": 1200},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                ignore_https_errors=True,
                java_script_enabled=True,
            )
            page = await context.new_page()
            page.set_default_timeout(90_000)
            await page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            # ============ INITIAL PAGE LOAD (with fallback) ============
            logger.debug(f"[Karnataka] Navigating to main report page for {date_str}...")
            await goto_with_fallback(
                page,
                "https://krama.karnataka.gov.in/Reports/Main_rep",
                _KARNATAKA_GOTO_TIMEOUT
            )
            await page.wait_for_load_state("networkidle", timeout=90_000)

            # ============ VERIFY THE PAGE IS READY ============
            date_field = page.locator("#_ctl0_MainContent_TxtDate")
            radio = page.locator("input[name='_ctl0:MainContent:RadBtnSel'][value='S']")
            view_button = page.locator("#_ctl0_MainContent_BtnRep")
            all_button = page.locator("#_ctl0_MainContent_lbtn_all")

            await date_field.wait_for(timeout=90_000)
            await radio.wait_for(timeout=90_000)
            await view_button.wait_for(timeout=90_000)

            # Some CI environments receive a challenge page or a slower-rendered DOM.
            # If the site blocks the browser, fail with a clear message instead of
            # silently timing out on the first click.
            page_text = (await page.content()).lower()
            if any(token in page_text for token in ["captcha", "verify you are human", "access denied"]):
                raise RuntimeError("Karnataka site challenged the browser session (captcha / anti-bot page)")

            # ============ FILL FORM FIELDS ============
            logger.debug("[Karnataka] Filling date field...")
            await date_field.fill(date_str)

            logger.debug("[Karnataka] Selecting 'State Level Daily Report'...")
            await radio.check()

            # ============ VIEW REPORT (with fallback navigation) ============
            logger.debug("[Karnataka] Clicking 'View Report'...")
            await expect_navigation_with_fallback(
                page,
                _KARNATAKA_GOTO_TIMEOUT,
                lambda: view_button.click(timeout=90_000)
            )
            await page.wait_for_load_state("networkidle", timeout=90_000)

            # ============ LOAD ALL RECORDS (with fallback navigation) ============
            logger.debug("[Karnataka] Clicking 'All' to load all records...")
            await all_button.wait_for(timeout=90_000)
            await expect_navigation_with_fallback(
                page,
                _KARNATAKA_GOTO_TIMEOUT,
                lambda: all_button.click(timeout=90_000)
            )
            await page.wait_for_load_state("networkidle", timeout=90_000)

            # ============ EXTRACT HTML ============
            html_content = await page.content()
            logger.debug(f"[Karnataka] Extracted HTML ({len(html_content)} bytes)")

            # ============ PARSE DATA ============
            # Parse using the Karnataka-specific HTML parser
            parsed_data = karnataka_mandi_parser(
                html_content, filter=False, report_date=date_str
            )
            logger.info(f"[Karnataka] ✓ SUCCESS: Parsed {len(parsed_data)} records for {date_str}")
            return parsed_data

        except Exception as e:
            last_exc = e
            logger.warning(
                f"[Karnataka] Attempt {attempt}/{_KARNATAKA_MAX_RETRIES} failed: {type(e).__name__}: {e}"
            )
            if attempt < _KARNATAKA_MAX_RETRIES:
                wait_time = 5 * attempt
                logger.info(f"[Karnataka] Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)  # back-off: 5 s, 10 s, 15 s
        finally:
            if browser:
                try:
                    await browser.close()
                except Exception as close_err:
                    logger.warning(f"[Karnataka] Failed to close browser: {close_err}")
            if playwright_instance:
                try:
                    await playwright_instance.stop()
                except Exception as stop_err:
                    logger.warning(f"[Karnataka] Failed to stop Playwright: {stop_err}")

    logger.error(
        f"[Karnataka] All {_KARNATAKA_MAX_RETRIES} attempts failed for date {date_str}: {type(last_exc).__name__}: {last_exc}",
        exc_info=last_exc,
    )
    return {
        "success": False,
        "error": "Failed to retrieve or parse Karnataka mandi data.",
        "details": str(last_exc),
        "data": [],
    }
# ══════════════════════════════════════════════════════════════════════════════
# 2. MAHARASHTRA  (Sync / requests)
# Source : https://www.msamb.com/ApmcDetail/DataGridBind
# Method : GET request per APMC code from mandi_code list → parse HTML table.
# ══════════════════════════════════════════════════════════════════════════════

def maharashtra() -> list[dict]:
    """
    Scrape Maharashtra APMC mandi data from msamb.com.

    Iterates over every APMC market in ``mandi_code`` (from utils.maharashtra)
    and fetches commodity-level price data for each market.

    Returns
    -------
    list[dict]
        Combined records from all APMC markets.
    """
    url = "https://www.msamb.com/ApmcDetail/DataGridBind"

    headers = {
        # Session cookie required by the site; update if expired
        "Cookie": (
            "ASP.NET_SessionId=siqhdsinqxjmgniw3ybuwey3; "
            "kcsremuser=Language=E&username=Administrator&password=&"
            "userid=0&fullname=&rememberme=&roleid=0"
        ),
        "Referer": "https://www.msamb.com/ApmcDetail",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json; charset=UTF-8",
    }

    all_data = []

    try:
        for index, item in enumerate(mandi_code):
            try:
                params = {"commodityCode": "null", "apmcCode": item["value"]}
                logger.info(f"[Maharashtra] Fetching APMC {item['text']} (code {item['value']})")

                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()

                # Parse HTML response into structured dicts
                parsed_data = maharashtra_parse(response.text, item["text"])
                all_data.extend(parsed_data)

            except requests.RequestException as e:
                logger.error(
                    f"[Maharashtra] Network error for {item['text']} "
                    f"(code {item['value']}): {e}",
                    exc_info=True,
                )
            except Exception as e:
                logger.error(
                    f"[Maharashtra] Unexpected error for {item['text']} "
                    f"(code {item['value']}): {e}",
                    exc_info=True,
                )

    except Exception as e:
        logger.error(f"[Maharashtra] Critical failure: {e}", exc_info=True)
        return []

    logger.info(f"[Maharashtra] Total records collected: {len(all_data)}")
    return all_data


# ══════════════════════════════════════════════════════════════════════════════
# 3. MEGHALAYA  (Sync / requests + BeautifulSoup)
# Source : https://megamb.gov.in/Public/MegambDailyReport.aspx
# Method : GET first page, then POST with ASP.NET __doPostBack to paginate
#          through all pages until there is no "Next" link.
# ══════════════════════════════════════════════════════════════════════════════

# Meghalaya portal constants
_MEGHALAYA_BASE_URL = "https://megamb.gov.in/Public/MegambDailyReport.aspx"
_MEGHALAYA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://megamb.gov.in/",
}
_MEGHALAYA_TABLE_ID  = "ContentPlaceHolder1_grdv_Statedaily"
_MEGHALAYA_TARGET_ID = "ctl00$ContentPlaceHolder1$grdv_Statedaily"  # ASP.NET postback target

# Column names for the Meghalaya grid (Group Name column is dropped)
_MEGHALAYA_KEEP_COLS = [
    "commodity_name",
    "variety",
    "market",
    "grade",
    "arrival_quintals",
    "unit",
    "min_price_rs_per_quintal",
    "max_price_rs_per_quintal",
    "modal_price_rs_per_quintal",
]
_MEGHALAYA_NUMERIC_COLS = {
    "arrival_quintals",
    "min_price_rs_per_quintal",
    "max_price_rs_per_quintal",
    "modal_price_rs_per_quintal",
}


def _meghalaya_extract_hidden(soup: BeautifulSoup) -> dict:
    """
    Pull all ASP.NET hidden form fields needed for a postback request.
    These fields carry the view-state and event validation tokens.
    """
    fields = {}
    for name in (
        "__VIEWSTATE",
        "__VIEWSTATEGENERATOR",
        "__EVENTVALIDATION",
        "__LASTFOCUS",
        "__EVENTTARGET",
        "__EVENTARGUMENT",
    ):
        tag = soup.find("input", {"name": name})
        if tag:
            fields[name] = tag.get("value", "")
    return fields


def _meghalaya_parse_page(soup: BeautifulSoup, report_date: str) -> list[dict]:
    """
    Extract all commodity rows from a single page of the Meghalaya grid.

    Table structure note:
    - Rows with a "Group Name" column → 10 cells → drop cell[0]
    - Continuation rows (rowspan)     →  9 cells → use as-is
    - Pagination / header rows        → skipped
    """
    table = soup.find("table", {"id": _MEGHALAYA_TABLE_ID})
    if not table:
        return []

    records = []
    for row in table.find_all("tr")[1:]:  # skip header row
        cells = row.find_all("td")
        if not cells:
            continue
        # Pagination rows contain <a> links (e.g. "Next", "Last")
        if cells[0].find("a"):
            continue

        texts = [c.get_text(strip=True) for c in cells]
        if len(texts) == 10:
            values = texts[1:]   # drop the Group Name cell
        elif len(texts) == 9:
            values = texts
        else:
            continue  # unexpected column count, skip row

        if len(values) != 9:
            continue

        entry = {"date": report_date}
        entry.update(dict(zip(_MEGHALAYA_KEEP_COLS, values)))

        # Cast numeric columns from string to int where possible
        for col in _MEGHALAYA_NUMERIC_COLS:
            try:
                entry[col] = int(entry[col])
            except (ValueError, KeyError):
                pass  # leave as string if conversion fails

        records.append(entry)

    return records


def _meghalaya_has_next(soup: BeautifulSoup) -> bool:
    """Return True if the grid has a 'Next' page navigation link."""
    table = soup.find("table", {"id": _MEGHALAYA_TABLE_ID})
    if not table:
        return False
    return any(
        "Page$Next" in (a.get("href") or "") for a in table.find_all("a")
    )


def meghalaya_scrape_daily_report(report_date: str | None = None) -> list[dict]:
    """
    Scrape every page of the Meghalaya State Level Daily Report.

    Parameters
    ----------
    report_date : str, optional
        DD/MM/YYYY format. Defaults to today.

    Returns
    -------
    list[dict]
        All commodity records across all paginated pages.
    """
    if report_date is None:
        report_date = datetime.today().strftime("%d/%m/%Y")

    url = f"{_MEGHALAYA_BASE_URL}?date={quote(report_date, safe='')}"
    session = requests.Session()
    session.headers.update(_MEGHALAYA_HEADERS)

    logger.info(f"[Meghalaya] Fetching page 1 → {url}")
    resp = session.get(url, timeout=30, verify=False)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    all_records = _meghalaya_parse_page(soup, report_date)
    logger.info(f"[Meghalaya] Page 1: {len(all_records)} records")

    page = 1
    while _meghalaya_has_next(soup):
        page += 1
        # Build the ASP.NET postback payload for "next page"
        payload = _meghalaya_extract_hidden(soup)
        payload["__EVENTTARGET"]   = _MEGHALAYA_TARGET_ID
        payload["__EVENTARGUMENT"] = "Page$Next"

        logger.info(f"[Meghalaya] Fetching page {page} …")
        resp = session.post(url, data=payload, timeout=30, verify=False)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        page_records = _meghalaya_parse_page(soup, report_date)
        logger.info(f"[Meghalaya] Page {page}: {len(page_records)} records")
        all_records.extend(page_records)

    logger.info(
        f"[Meghalaya] Done — {len(all_records)} total records for {report_date}"
    )
    return all_records


# ══════════════════════════════════════════════════════════════════════════════
# 4. NAGALAND  (Async / Playwright)
# Source : https://www.commodityonline.com/mandiprices/state/nagaland
# Method : Playwright headless Chromium → wait for page load → parse HTML table.
# ══════════════════════════════════════════════════════════════════════════════

_NAGALAND_URL = "https://www.commodityonline.com/mandiprices/state/nagaland"


def _nagaland_extract_apmc_prices(html_content: str) -> list[dict]:
    """
    Extract the APMC price table from Nagaland page HTML.

    Looks for the first <table> on the page, reads headers from <th> tags,
    and builds a list of row dicts — excluding the 'Telegram' column if present.

    Parameters
    ----------
    html_content : str
        Raw HTML string from the Nagaland commodityonline page.

    Returns
    -------
    list[dict]
        List of price records; empty list if no table found.
    """
    soup = BeautifulSoup(html_content, "html.parser")

    table = soup.find("table")
    if not table:
        logger.warning("[Nagaland] No table found in HTML.")
        return []

    headers = [th.text.strip() for th in table.find_all("th")]

    # Identify and skip the 'Telegram' column (social share link, not data)
    telegram_index = headers.index("Telegram") if "Telegram" in headers else -1

    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

    data = []
    for row in rows:
        cols = [td.text.strip() for td in row.find_all("td")]
        if len(cols) == len(headers):
            row_data = {
                headers[i]: col
                for i, col in enumerate(cols)
                if i != telegram_index
            }
            data.append(row_data)

    return data


async def fetch_nagaland_state_daily() -> list[dict] | dict:
    """
    Scrape Nagaland APMC mandi prices via Playwright.

    The page renders with JavaScript so a headless browser is required.

    Returns
    -------
    list[dict]
        Parsed price records, or an error dict on failure.
    """
    from playwright.async_api import async_playwright

    playwright_instance = None
    browser = None

    # Retry the whole Playwright session up to 3 times to handle transient
    # CI network timeouts that are common when hitting slow external sites.
    _NAGALAND_MAX_RETRIES = 3
    _NAGALAND_GOTO_TIMEOUT = 120_000  # 120 s

    last_exc: Exception | None = None

    for attempt in range(1, _NAGALAND_MAX_RETRIES + 1):
        playwright_instance = None
        browser = None
        try:
            logger.info(f"[Nagaland] Attempt {attempt}/{_NAGALAND_MAX_RETRIES}")
            playwright_instance = await async_playwright().start()

            browser = await playwright_instance.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-setuid-sandbox",
                    "--disable-extensions",
                    "--single-process",
                ],
            )

            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            # Navigate to the Nagaland commodity price page
            await page.goto(
                _NAGALAND_URL,
                timeout=_NAGALAND_GOTO_TIMEOUT,
                wait_until="domcontentloaded",
            )
            # Wait for full DOM content to settle
            await page.wait_for_load_state("domcontentloaded")

            html_content = await page.content()

            # Parse the price table inline (no external parser import needed)
            data = _nagaland_extract_apmc_prices(html_content)
            logger.info(f"[Nagaland] Parsed {len(data)} records")
            return data

        except Exception as e:
            last_exc = e
            logger.warning(
                f"[Nagaland] Attempt {attempt}/{_NAGALAND_MAX_RETRIES} failed: {e}"
            )
            if attempt < _NAGALAND_MAX_RETRIES:
                await asyncio.sleep(5 * attempt)  # back-off: 5 s, 10 s
        finally:
            if browser:
                try:
                    await browser.close()
                except Exception as close_err:
                    logger.warning(f"[Nagaland] Failed to close browser: {close_err}")
            if playwright_instance:
                try:
                    await playwright_instance.stop()
                except Exception as stop_err:
                    logger.warning(f"[Nagaland] Failed to stop Playwright: {stop_err}")

    logger.error(
        f"[Nagaland] All {_NAGALAND_MAX_RETRIES} attempts failed: {last_exc}",
        exc_info=last_exc,
    )
    return {
        "success": False,
        "error": "Failed to retrieve or parse Nagaland APMC data.",
        "details": str(last_exc),
        "data": [],
    }


# ══════════════════════════════════════════════════════════════════════════════
# 5. PUNJAB  (Sync / requests)
# Source : https://api.emandikaran-pb.in/CommonMasterDataAPI/getDailyArrival/...
# Method : Single GET request with date range path params → JSON response.
# ══════════════════════════════════════════════════════════════════════════════

def punjab_mandi(start_date: str = None, end_date: str = None) -> list[dict]:
    """
    Fetch Punjab mandi daily arrival data from the eMandikaran API.

    Parameters
    ----------
    start_date : str, optional
        DD-MM-YYYY format. Defaults to yesterday.
    end_date : str, optional
        DD-MM-YYYY format. Defaults to today.

    Returns
    -------
    list[dict]
        List of mandi records with 'Market' field mapped from 'BranchName'.
    """
    # Default date range: yesterday → today
    if start_date is None:
        start_date = (datetime.today() - timedelta(days=1)).strftime("%d-%m-%Y")
    if end_date is None:
        end_date = datetime.today().strftime("%d-%m-%Y")

    # URL pattern: /getDailyArrival/{commodityId}/{stateCode}/{mandiId}/{from}/{to}
    # 0 = all commodities, 34 = Punjab state code, 0 = all mandis
    url = f"https://api.emandikaran-pb.in/CommonMasterDataAPI/getDailyArrival/0/34/0/{start_date}/{end_date}"

    # The eMandikaran API can be very slow from cloud/CI environments;
    # use a long timeout with retries so transient latency doesn't drop data.
    _PUNJAB_TIMEOUT = 300   # seconds — government API is slow from datacenter IPs
    _PUNJAB_RETRIES = 3

    for attempt in range(1, _PUNJAB_RETRIES + 1):
        try:
            logger.info(
                f"[Punjab] Fetching {start_date}→{end_date} "
                f"(attempt {attempt}/{_PUNJAB_RETRIES}, timeout={_PUNJAB_TIMEOUT}s)"
            )
            response = requests.get(url, timeout=_PUNJAB_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            raw_records = data.get("responseData") or []
            logger.info(
                f"[Punjab] Raw responseData length: {len(raw_records)}  "
                f"| Top-level keys: {list(data.keys())}"
            )

            if not raw_records:
                # Log the full response so we can diagnose empty results in CI
                logger.warning(
                    f"[Punjab] API returned 0 records — full response: "
                    f"{str(data)[:500]}"
                )

            # Rename 'BranchName' → 'Market' for consistency across all states
            response_data = [
                {
                    **{k: v for k, v in item.items() if k != "BranchName"},
                    "Market": item["BranchName"],
                }
                for item in raw_records
            ]

            logger.info(f"[Punjab] Fetched {len(response_data)} records")
            return response_data

        except requests.Timeout as e:
            logger.warning(
                f"[Punjab] Attempt {attempt}/{_PUNJAB_RETRIES} timed out "
                f"after {_PUNJAB_TIMEOUT}s: {e}"
            )
            if attempt == _PUNJAB_RETRIES:
                logger.error("[Punjab] All retry attempts timed out — giving up.")
                return []
        except requests.RequestException as e:
            logger.error(f"[Punjab] Network error (attempt {attempt}): {e}", exc_info=True)
            if attempt == _PUNJAB_RETRIES:
                return []
        except json.JSONDecodeError as e:
            logger.error(f"[Punjab] Failed to parse JSON response: {e}", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"[Punjab] Unexpected error (attempt {attempt}): {e}", exc_info=True)
            if attempt == _PUNJAB_RETRIES:
                return []

    return []  # should not be reached


# ══════════════════════════════════════════════════════════════════════════════
# 6. UTTAR PRADESH  (Sync / requests)
# Source : https://www.upkrishivipran.in/Default.aspx/GetHomeCenterBhavDetails
# Method : POST request per (mandi_code × product_category) combination.
#          up_mandi_code  – dict  { mandi_name: centre_code }
#          up_product_category – list[{ "value": species_group_code, ... }]
# ══════════════════════════════════════════════════════════════════════════════

async def uttarpradesh_mandi() -> list[dict]:
    """
    Scrape Uttar Pradesh mandi prices from the UPKrishiVipran API asynchronously.

    Iterates over every (mandi, product_category) pair and POSTs a JSON
    request concurrently using a Semaphore. Only non-empty responses are
    appended to the result.

    Returns
    -------
    list[dict]
        Combined records from all mandi × category combinations.
    """
    url = "https://www.upkrishivipran.in/Default.aspx/GetHomeCenterBhavDetails"
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://www.upkrishivipran.in/",
    }

    # Use yesterday's date — the API typically has yesterday's data available
    yesterday_date = (datetime.today() - timedelta(days=1)).strftime("%d/%m/%Y")

    sem = asyncio.Semaphore(30)  # limit concurrency to 30 to not overwhelm the server
    all_data = []

    async def fetch_one(mandi_name: str, centre_code: str, category: dict) -> list[dict]:
        payload = {
            "BhavDate": yesterday_date,
            "CentreCode": centre_code,
            "SpeciesGroupCode": category["value"],
        }
        async with sem:
            for attempt in range(3):
                try:
                    async with httpx.AsyncClient(timeout=30) as client:
                        response = await client.post(url, headers=headers, json=payload)
                        response.raise_for_status()
                        resp_json = response.json()
                        data_list = resp_json.get("d") or []
                        records = []
                        for each_item in data_list:
                            record = {
                                "Market":         mandi_name,
                                "Date":           yesterday_date,
                                "arrival":        each_item.get("aavakRate"),
                                "Commodity":      each_item.get("ProductName"),
                                "Wholesale_rate": each_item.get("WholeSeleRate"),
                                "Retail_price":   each_item.get("PhutKarRate"),
                            }
                            records.append(record)
                        return records
                except Exception as e:
                    if attempt == 2:
                        logger.error(
                            f"[UP] Error for CentreCode={centre_code} "
                            f"SpeciesGroup={category['value']}: {e}"
                        )
                    await asyncio.sleep(0.5)
            return []

    tasks = []
    for mandi_name, centre_code in up_mandi_code.items():
        for category in up_product_category:
            tasks.append(fetch_one(mandi_name, centre_code, category))

    results = await asyncio.gather(*tasks)
    for r in results:
        all_data.extend(r)

    logger.info(f"[UP] Total records collected: {len(all_data)}")
    return all_data


# ══════════════════════════════════════════════════════════════════════════════
# 7. ANDHRA PRADESH  (Sync / requests)
# Source : https://agriculture.ap.gov.in/staging/api/emarket/getMarketPriceData
# Method : POST request with date range timestamps → JSON response.
# ══════════════════════════════════════════════════════════════════════════════

def andhra_pradesh_mandi(
    start_date: str | None = None,
    end_date: str | None = None,
    days_back: int = 30,
) -> list[dict]:
    """
    Fetch Andhra Pradesh mandi price data from the agriculture.ap.gov.in API.

    Parameters
    ----------
    start_date : str, optional
        Start date formatted or timestamp (in ms). If None, defaults to `days_back` prior.
    end_date : str, optional
        End date formatted or timestamp (in ms). If None, defaults to current time.
    days_back : int, optional
        Number of days to fetch backwards if start_date is not specified. Default 30.

    Returns
    -------
    list[dict]
        List of flattened commodity records across mandis in Andhra Pradesh.
    """
    url = "https://agriculture.ap.gov.in/staging/api/emarket/getMarketPriceData"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://agriculture.ap.gov.in",
        "Referer": "https://agriculture.ap.gov.in/home",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/150.0.0.0 Safari/537.36"
        ),
    }

    import time

    now_ms = int(time.time() * 1000)

    # Calculate end timestamp in ms
    if not end_date:
        end_ms = now_ms
    elif isinstance(end_date, (int, float)):
        end_ms = int(end_date)
    elif str(end_date).isdigit():
        end_ms = int(end_date)
    else:
        try:
            dt = datetime.strptime(str(end_date), "%Y-%m-%d")
            end_ms = int(dt.timestamp() * 1000)
        except ValueError:
            try:
                dt = datetime.strptime(str(end_date), "%d/%m/%Y")
                end_ms = int(dt.timestamp() * 1000)
            except ValueError:
                end_ms = now_ms

    # Calculate start timestamp in ms
    if not start_date:
        start_ms = end_ms - (days_back * 24 * 3600 * 1000)
    elif isinstance(start_date, (int, float)):
        start_ms = int(start_date)
    elif str(start_date).isdigit():
        start_ms = int(start_date)
    else:
        try:
            dt = datetime.strptime(str(start_date), "%Y-%m-%d")
            start_ms = int(dt.timestamp() * 1000)
        except ValueError:
            try:
                dt = datetime.strptime(str(start_date), "%d/%m/%Y")
                start_ms = int(dt.timestamp() * 1000)
            except ValueError:
                start_ms = end_ms - (days_back * 24 * 3600 * 1000)

    payload = {
        "apmcList": [],
        "commoditiesList": [],
        "startDate": start_ms,
        "endDate": end_ms,
        "source": "AGRIWATCH_MARKET_PRICE",
        "stateUUID": "1b62503c-0355-4222-8712-80e1e1d29445",
    }

    _AP_TIMEOUT = 60
    _AP_RETRIES = 3

    for attempt in range(1, _AP_RETRIES + 1):
        try:
            logger.info(
                f"[Andhra Pradesh] Fetching market price data (attempt {attempt}/{_AP_RETRIES})"
            )
            response = requests.post(
                url, headers=headers, json=payload, timeout=_AP_TIMEOUT, verify=False
            )
            response.raise_for_status()
            data = response.json()

            if not data.get("result"):
                logger.warning(
                    f"[Andhra Pradesh] API returned result=False or error: {data.get('message')}"
                )
                return []

            response_obj = data.get("response", {})
            extracted_records = []

            for loc_uuid, loc_data in response_obj.items():
                if not isinstance(loc_data, dict):
                    continue
                lat = loc_data.get("latitude")
                lon = loc_data.get("longitude")
                for date_key, date_data in loc_data.items():
                    if date_key in ("latitude", "longitude"):
                        continue
                    if isinstance(date_data, dict):
                        for comm_uuid, comm_list in date_data.items():
                            if isinstance(comm_list, list):
                                for item in comm_list:
                                    if isinstance(item, dict):
                                        record = dict(item)
                                        if lat is not None:
                                            record["latitude"] = lat
                                        if lon is not None:
                                            record["longitude"] = lon
                                        extracted_records.append(record)

            logger.info(
                f"[Andhra Pradesh] Successfully extracted {len(extracted_records)} records"
            )
            return extracted_records

        except requests.RequestException as e:
            logger.error(
                f"[Andhra Pradesh] Network error (attempt {attempt}): {e}", exc_info=True
            )
            if attempt == _AP_RETRIES:
                return []
        except json.JSONDecodeError as e:
            logger.error(
                f"[Andhra Pradesh] Failed to parse JSON response: {e}", exc_info=True
            )
            return []
        except Exception as e:
            logger.error(
                f"[Andhra Pradesh] Unexpected error (attempt {attempt}): {e}",
                exc_info=True,
            )
            if attempt == _AP_RETRIES:
                return []

    return []


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR — runs all scrapers concurrently
# ══════════════════════════════════════════════════════════════════════════════

async def run_all_scrapers(date_str: str = "") -> dict[str, Any]:
    """
    Execute all state scrapers concurrently and return aggregated results.

    - Async scrapers (Karnataka, Nagaland) run as native coroutines.
    - Sync scrapers (Karnataka, Maharashtra, Meghalaya, Nagaland, Punjab, Uttar Pradesh, Andhra Pradesh.) wrapped in
      ``asyncio.to_thread`` so they don't block the event loop.

    Parameters
    ----------
    date_str : str, optional
        Date in DD/MM/YYYY format forwarded to scrapers that accept it.
        Defaults to each scraper's own default (usually today / yesterday).

    Returns
    -------
    dict[str, Any]
        ``{ "StateName": { "success": bool, "data": list|dict } }``
    """
    logger.info("═" * 60)
    logger.info("  Starting all-market APMC mandi scraper run …")
    logger.info("═" * 60)

    # ── Schedule async (Playwright) scrapers with a hard timeout ────────────
    # Playwright browsers can hang indefinitely on slow/broken pages.
    # asyncio.wait_for cancels the coroutine and raises TimeoutError (which
    # asyncio.gather captures as an exception) so the whole run is not blocked.
    # Playwright scrapers can be very slow on CI (page load + interaction).
    # 300 s gives the browser time to load even on a congested government site.
    # 600 s outer guard — each scraper already retries 3× with 120 s page.goto
    PLAYWRIGHT_TIMEOUT = 600  # seconds per Playwright scraper

    karnataka_task = asyncio.create_task(
        asyncio.wait_for(
            fetch_karnataka_state_daily(date_str=date_str),
            timeout=PLAYWRIGHT_TIMEOUT,
        ),
        name="Karnataka",
    )
    nagaland_task = asyncio.create_task(
        asyncio.wait_for(
            fetch_nagaland_state_daily(),
            timeout=PLAYWRIGHT_TIMEOUT,
        ),
        name="Nagaland",
    )

    # ── Wrap sync scrapers in threads so they don't block the event loop ──────
    maharashtra_task = asyncio.to_thread(maharashtra)
    meghalaya_task   = asyncio.to_thread(
        meghalaya_scrape_daily_report,
        date_str if date_str else None,   # pass date or let it default
    )
    punjab_task = asyncio.to_thread(punjab_mandi)
    up_task     = uttarpradesh_mandi()
    ap_task     = asyncio.to_thread(
        andhra_pradesh_mandi,
        start_date=None,
        end_date=date_str if date_str else None,
    )

    # ── Gather all results; return_exceptions=True so one failure doesn't
    #    cancel the rest ─────────────────────────────────────────────────────
    logger.info("Waiting for all scrapers to complete …")
    gathered = await asyncio.gather(
        karnataka_task,
        nagaland_task,
        maharashtra_task,
        meghalaya_task,
        punjab_task,
        up_task,
        ap_task,
        return_exceptions=True,
    )

    state_names = [
        "Karnataka",
        "Nagaland",
        "Maharashtra",
        "Meghalaya",
        "Punjab",
        "Uttar Pradesh",
        "Andhra Pradesh",
    ]

    results: dict[str, Any] = {}
    for state, result in zip(state_names, gathered):
        if isinstance(result, BaseException):
            logger.error(f"Scraper for {state} failed: {result}", exc_info=result)
            results[state] = {
                "success": False,
                "error": str(result),
                "data": None,
            }
        elif isinstance(result, dict) and not result.get("success", True):
            # Scraper returned an explicit error dict (e.g. Playwright failure)
            # instead of raising an exception.  Treat it as a failure so that
            # normalise_all() never iterates over the error-dict's string keys.
            logger.error(
                f"Scraper for {state} returned an error: "
                f"{result.get('error', 'unknown')} — {result.get('details', '')}"
            )
            results[state] = {
                "success": False,
                "error": result.get("error", "Scraper returned failure dict"),
                "data": result.get("data", []),
            }
        else:
            logger.info(f"✓ {state} — completed successfully")
            results[state] = {"success": True, "data": result}

    logger.info("═" * 60)
    logger.info("  All scrapers finished.")
    logger.info("═" * 60)
    return results


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """
    Stand-alone entry point.
    Runs all scrapers and saves aggregated JSON to
    ``othermarkets_data_YYYYMMDD.json`` in the current directory.
    """
    try:
        aggregated = asyncio.run(run_all_scrapers())

        # Coerce any accidentally stringified JSON fields back to dicts/lists
        for state, result in aggregated.items():
            if result.get("success") and isinstance(result.get("data"), str):
                try:
                    result["data"] = json.loads(result["data"])
                except (json.JSONDecodeError, TypeError):
                    pass  # leave as-is if it cannot be parsed

        out_file = f"othermarkets_data_{datetime.now().strftime('%Y%m%d')}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(aggregated, f, ensure_ascii=False, indent=4)

        logger.info(f"Results saved → {out_file}")

    except KeyboardInterrupt:
        logger.warning("Scraping interrupted by user (KeyboardInterrupt).")
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}", exc_info=True)


if __name__ == "__main__":
    main()
