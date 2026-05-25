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

    try:
        playwright_instance = await async_playwright().start()

        # Launch headless Chromium with sandbox disabled (required in CI/Docker)
        browser = await playwright_instance.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
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

        # Navigate to the Karnataka mandi report page
        await page.goto(
            "https://krama.karnataka.gov.in/Reports/Main_rep",
            timeout=60000,
            wait_until="domcontentloaded",
        )

        # Fill the date input field
        await page.locator("#_ctl0_MainContent_TxtDate").fill(date_str)

        # Select "State Level Daily Report" radio button
        await page.locator(
            "input[name='_ctl0:MainContent:RadBtnSel'][value='S']"
        ).check()

        # Click "View Report" and wait for navigation
        async with page.expect_navigation(timeout=60000, wait_until="domcontentloaded"):
            await page.locator("#_ctl0_MainContent_BtnRep").click()

        # Click "All" to load all records on one page
        async with page.expect_navigation(timeout=60000, wait_until="domcontentloaded"):
            await page.locator("#_ctl0_MainContent_lbtn_all").click()

        html_content = await page.content()

        # Parse using the Karnataka-specific HTML parser
        parsed_data = karnataka_mandi_parser(
            html_content, filter=False, report_date=date_str
        )
        logger.info(f"[Karnataka] Parsed {len(parsed_data)} records for {date_str}")
        return parsed_data

    except Exception as e:
        logger.error(
            f"[Karnataka] Error fetching data for date {date_str}: {e}",
            exc_info=True,
        )
        return {
            "success": False,
            "error": "Failed to retrieve or parse Karnataka mandi data.",
            "details": str(e),
            "data": [],
        }
    finally:
        # Always clean up browser resources
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

    try:
        playwright_instance = await async_playwright().start()

        browser = await playwright_instance.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
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
            timeout=60000,
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
        logger.error(f"[Nagaland] Error fetching APMC prices: {e}", exc_info=True)
        return {
            "success": False,
            "error": "Failed to retrieve or parse Nagaland APMC data.",
            "details": str(e),
            "data": [],
        }
    finally:
        # Always clean up browser resources
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

    try:
        logger.info(f"[Punjab] Fetching data from {start_date} to {end_date}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Rename 'BranchName' → 'Market' for consistency across all states
        response_data = [
            {
                **{k: v for k, v in item.items() if k != "BranchName"},
                "Market": item["BranchName"],
            }
            for item in data.get("responseData", [])
        ]

        logger.info(f"[Punjab] Fetched {len(response_data)} records")
        return response_data

    except requests.RequestException as e:
        logger.error(f"[Punjab] Network error: {e}", exc_info=True)
        return []
    except json.JSONDecodeError as e:
        logger.error(f"[Punjab] Failed to parse JSON response: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"[Punjab] Unexpected error: {e}", exc_info=True)
        return []


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
# ORCHESTRATOR — runs all scrapers concurrently
# ══════════════════════════════════════════════════════════════════════════════

async def run_all_scrapers(date_str: str = "") -> dict[str, Any]:
    """
    Execute all state scrapers concurrently and return aggregated results.

    - Async scrapers (Karnataka, Nagaland) run as native coroutines.
    - Sync scrapers (Maharashtra, Meghalaya, Punjab, UP) are wrapped in
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
    PLAYWRIGHT_TIMEOUT = 120  # seconds per Playwright scraper

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
        return_exceptions=True,
    )

    state_names = [
        "Karnataka",
        "Nagaland",
        "Maharashtra",
        "Meghalaya",
        "Punjab",
        "Uttar Pradesh",
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
