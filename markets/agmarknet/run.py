import asyncio
import json
import logging
import os
import random
from datetime import date as date_type, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = os.getenv("AGMARKNET_BASE_URL", "https://api.agmarknet.gov.in/v1").rstrip("/")
TIMEOUT_SECONDS = float(os.getenv("AGMARKNET_TIMEOUT_SECONDS", "30"))
MAX_RETRIES = int(os.getenv("AGMARKNET_MAX_RETRIES", "3"))
INITIAL_BACKOFF = float(os.getenv("AGMARKNET_INITIAL_BACKOFF", "10.0"))

DASHBOARD = "marketwise_price_arrival"
PAGE_SIZE = 10

# How many market requests to run in parallel
CONCURRENCY = int(os.getenv("AGMARKNET_CONCURRENCY", "1"))

# Polite delay each worker waits after completing a request (seconds)
PAGE_DELAY = float(os.getenv("AGMARKNET_PAGE_DELAY", "5.0"))

# agmarknet_filters.json (market list and state names)
FILTERS_FILE = Path(__file__).resolve().parents[2] / "agmarknet_filters.json"


def _build_state_names() -> dict[int, str]:
    """Build a state_id → lowercase state_name lookup from agmarknet_filters.json."""
    try:
        with open(FILTERS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return {
            s["state_id"]: s["state_name"].lower()
            for s in data["data"]["state_data"]
            if "state_id" in s and "state_name" in s
        }
    except Exception as exc:
        logger.warning("Could not load state names from agmarknet_filters.json: %s", exc)
        return {}


def _build_district_names() -> dict[int, str]:
    """Build a district_id → lowercase district_name lookup from agmarknet_filters.json."""
    try:
        with open(FILTERS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return {
            d["id"]: d["district_name"].lower()
            for d in data["data"]["district_data"]
            if "id" in d and "district_name" in d and d["id"] is not None
        }
    except Exception as exc:
        logger.warning("Could not load district names from agmarknet_filters.json: %s", exc)
        return {}


STATE_NAMES: dict[int, str] = _build_state_names()
DISTRICT_NAMES: dict[int, str] = _build_district_names()

# Mimic a real browser to avoid nginx-level blocks
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Referer": "https://agmarknet.gov.in/",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_retry_delay(response: httpx.Response | None, attempt: int, initial_backoff: float, status: int | None = None) -> float:
    """Return a sleep delay for transient failures, honoring Retry-After and enforcing a safe floor for 429s."""
    if response is not None:
        retry_after = response.headers.get("retry-after")
        if retry_after:
            try:
                return max(float(retry_after), 20.0 if status == 429 else 0.0)
            except ValueError:
                pass
            try:
                parsed = parsedate_to_datetime(retry_after)
                if parsed is not None:
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    delta = (parsed - datetime.now(timezone.utc)).total_seconds()
                    return max(delta, 20.0 if status == 429 else 0.0)
            except (TypeError, ValueError, OverflowError):
                pass

    if status == 429:
        return max(initial_backoff * (2**attempt) + 10.0, 20.0)

    cap = initial_backoff * (2**attempt)
    return random.uniform(0, cap)


async def _retry_with_backoff(func, *args, max_retries: int = MAX_RETRIES, **kwargs):
    """Retry *func* with exponential back-off on transient HTTP / network errors.

    - 429 / 5xx  → retried with back-off (full jitter)
    - 403        → re-raised immediately
    - other 4xx  → re-raised immediately
    - network errors → retried with back-off
    """
    last_exception: Exception | None = None

    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 429 or (status is not None and 500 <= status < 600):
                last_exception = exc
                if attempt < max_retries - 1:
                    delay = _get_retry_delay(exc.response, attempt, INITIAL_BACKOFF, status)
                    logger.warning(
                        "HTTP %s – retry %s/%s after %.2fs",
                        status,
                        attempt + 1,
                        max_retries,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                break
            raise
        except (
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.TimeoutException,
            httpx.RemoteProtocolError,
            httpx.RequestError,
        ) as exc:
            last_exception = exc
            if attempt < max_retries - 1:
                cap = INITIAL_BACKOFF * (2 ** attempt)
                backoff = random.uniform(0, cap)
                logger.warning(
                    "Network error – retry %s/%s after %.2fs: %s", attempt + 1, max_retries, backoff, exc
                )
                await asyncio.sleep(backoff)
                continue
            break
        except Exception:
            raise

    if last_exception:
        raise last_exception
    raise RuntimeError("_retry_with_backoff exhausted without captured exception")


async def _post(client: httpx.AsyncClient, path: str, body: dict[str, Any]) -> httpx.Response:
    """Make a POST request using a shared AsyncClient and return the raw Response."""
    url = f"{BASE_URL}/{path.lstrip('/')}"

    async def make_request():
        response = await client.post(url, json=body)
        response.raise_for_status()
        return response

    return await _retry_with_backoff(make_request)


def _load_markets() -> list[dict[str, Any]]:
    """Load market_data from agmarknet_filters.json, skipping 'All Markets' entries
    (those with state_id=None or district_id=None).
    """
    with open(FILTERS_FILE, encoding="utf-8") as f:
        filters = json.load(f)

    all_markets: list[dict] = filters["data"]["market_data"]

    # Skip entries where state_id or district_id is None (i.e. "All Markets")
    valid = [
        m for m in all_markets
        if m.get("state_id") is not None and m.get("district_id") is not None
    ]
    logger.info(
        "Loaded %s markets from filters (%s skipped as 'All Markets')",
        len(valid), len(all_markets) - len(valid),
    )
    return valid


def _extract_records(payload: dict[str, Any], market_id: int) -> list[dict[str, Any]]:
    """Extract the record list from various Agmarknet API response shapes."""
    data = payload.get("data")
    if isinstance(data, dict):
        records = data.get("records") or data.get("results") or data.get("data") or []
        if isinstance(records, list):
            return records
    if isinstance(data, list):
        return data
    for key in ("records", "results", "items", "market_data"):
        v = payload.get(key)
        if isinstance(v, list):
            return v
    return []


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

async def fetch_market(
    market: dict[str, Any],
    date: str,
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    counter: list[int],
    total: int,
) -> list[dict[str, Any]]:
    """Fetch data for a single market under the shared semaphore."""
    market_id: int = market["id"]
    state_id: int = market["state_id"]
    district_id: int = market["district_id"]

    body = {
        "dashboard": DASHBOARD,
        "date": date,
        "group": [100000],
        "commodity": [100001],
        "variety": 100021,
        "state": state_id,
        "district": [district_id],
        "market": [market_id],
        "grades": [4],
        "limit": PAGE_SIZE,
        "format": "json",
    }

    async with sem:
        logger.debug(
            "POST market_id=%s state=%s district=%s (%s)",
            market_id, state_id, district_id, market.get("mkt_name", ""),
        )
        try:
            response = await _post(client, "dashboard-data/", body)
        except httpx.HTTPStatusError as e:
            status = e.response.status_code if e.response is not None else "?"
            logger.warning("HTTP %s for market_id=%s – skipping", status, market_id)
            return []
        except Exception as e:
            logger.error("Error fetching market_id=%s: %s", market_id, e)
            return []
        finally:
            # Polite delay inside the semaphore so each slot rests briefly
            await asyncio.sleep(PAGE_DELAY)

    try:
        payload: dict[str, Any] = response.json()
    except Exception as e:
        logger.error("JSON parse error for market_id=%s: %s", market_id, e)
        return []

    if payload.get("success") is False or payload.get("status") is False:
        logger.debug(
            "API no-data for market_id=%s: %s",
            market_id, payload.get("message") or payload.get("detail") or "",
        )
        return []

    records = _extract_records(payload, market_id)

    # Stamp each record with human-readable market, state, and district (lowercase)
    market_name_lc = market.get("mkt_name", "").lower()
    state_name_lc = STATE_NAMES.get(state_id, "")
    district_name_lc = DISTRICT_NAMES.get(district_id, "")
    for rec in records:
        rec["market"] = market_name_lc
        rec["state"] = state_name_lc
        rec["district"] = district_name_lc

    # Progress tracking
    counter[0] += 1
    done = counter[0]
    if done % 100 == 0 or done == total:
        logger.info("Progress: %s/%s markets done", done, total)
    if records:
        logger.info("market_id=%s → %s record(s)", market_id, len(records))

    return records


async def agmarknet(date: str | None = None, concurrency: int = CONCURRENCY) -> list[dict[str, Any]]:
    """
    Fetch marketwise price-arrival records for all markets in agmarknet_filters.json
    concurrently (bounded by *concurrency*) and return them as a flat list.
    """
    if date is None:
        date = date_type.today().isoformat()

    markets = _load_markets()
    total = len(markets)
    logger.info(
        "Starting Agmarknet scrape | date=%s  markets=%s  concurrency=%s",
        date, total, concurrency,
    )

    sem = asyncio.Semaphore(concurrency)
    counter: list[int] = [0]  # mutable container so coroutines can increment it

    async with httpx.AsyncClient(
        timeout=TIMEOUT_SECONDS,
        headers=HEADERS,
        limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
    ) as client:
        tasks = [
            fetch_market(market, date, client, sem, counter, total)
            for market in markets
        ]
        results: list[list[dict[str, Any]]] = await asyncio.gather(*tasks)

    all_data: list[dict[str, Any]] = [rec for batch in results for rec in batch]
    logger.info("Scrape complete. Total records collected: %s", len(all_data))
    return all_data


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data = asyncio.run(agmarknet())

    output_file = "agmarknet.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info("Saved %s records to %s", len(data), output_file)
    print(f"\n✅ Total records fetched: {len(data)}")
    print(f"📄 Saved to: {output_file}")
