"""Compare Agmarknet 'Market Report' xlsx files against the v2 DB (agmarknet source only).

usage (from /home/kishar/Mandi):
    .venv/bin/python check_agmarknet.py                  # every .xlsx in ~/Agmarknet
    .venv/bin/python check_agmarknet.py "Andhra Pradesh.xlsx" other.xlsx

Reads MONGO_URI (and MANDI_DB_NAME, V2_COLLECTION_PREFIX) from .env or the
environment. Read-only: it never writes to the database.
"""
import glob, os, re, sys, zipfile, collections
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pymongo import MongoClient
try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

FOLDER = os.path.expanduser("~/Agmarknet")

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def read_rows(path):
    z = zipfile.ZipFile(path)
    root = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))
    for r in root.iterfind(".//m:row", NS):
        out = []
        for c in r.iterfind("m:c", NS):
            t = c.find("m:is/m:t", NS)
            v = c.find("m:v", NS)
            out.append(t.text if t is not None else (v.text if v is not None else None))
        yield out


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def parse(path):
    rows = list(read_rows(path))
    date = datetime.strptime(re.search(r"on:\s*(\S+)", rows[0][0]).group(1), "%d-%b-%Y")
    state = market = None
    recs = []
    for r in rows[2:]:
        a = (r[0] or "").strip()
        if a.startswith("State/UT Name"):
            state = a.split(":", 1)[1].strip().lower()
        elif a.startswith("Market Name"):
            market = a.split(":", 1)[1].strip().lower()
        elif a and market:
            r += [None] * (9 - len(r))
            recs.append(dict(state=state, market=market, commodity=a.lower(),
                             arrival=num(r[1]), unit=r[2], variety=(r[3] or "").lower(),
                             grade=(r[4] or "").lower(), mn=num(r[5]), mx=num(r[6]), md=num(r[7])))
    return date.replace(tzinfo=timezone.utc), recs


def check(db, path):
    print("=" * 70); print(os.path.basename(path))
    date, recs = parse(path)
    states = collections.Counter(r["state"] for r in recs)
    print("excel date", date.date(), "rows", len(recs), "states", dict(states))
    pre = os.getenv("V2_COLLECTION_PREFIX", "v2_")
    lst = {d["_id"]: d for d in db[pre + "listings"].find({"source_system": "agmarknet"})}
    prices = list(db[pre + "price_records"].find({"date": date, "market_commodity_id": {"$in": list(lst)}}))
    print("db agmarknet rows on that date:", len(prices))
    if not prices:
        print("-> nothing in the DB for this date; scrape it or use a date the DB has."); return
    st = set(states)
    ex = collections.defaultdict(list)
    for r in recs:
        ex[(r["state"], r["market"], r["commodity"], r["variety"])].append(r)
    dd = collections.defaultdict(list)
    for p in prices:
        l = lst[p["market_commodity_id"]]
        if l.get("state") in st:
            dd[(l.get("state"), l.get("market_name"), l.get("commodity_name"), l.get("variety") or "")].append(p)
    only_ex = [k for k in ex if k not in dd]; only_db = [k for k in dd if k not in ex]
    both = [k for k in ex if k in dd]
    print("keys excel:", len(ex), "db:", len(dd), "both:", len(both), "only_excel:", len(only_ex), "only_db:", len(only_db))
    mism = collections.Counter(); samples = []
    for k in both:
        e, d = ex[k][0], dd[k][0]
        for f, g in (("mn", "min_price"), ("mx", "max_price"), ("md", "modal_price"), ("arrival", "arrival_quantity")):
            if e[f] != d.get(g):
                mism[f] += 1
                if len(samples) < 8: samples.append((k, f, e[f], d.get(g)))
    print("field mismatches:", dict(mism))
    for s_ in samples: print("  ", s_)
    print("only in excel (sample):", only_ex[:10]); print("only in db (sample):", only_db[:10])


if __name__ == "__main__":
    files = sys.argv[1:] or sorted(glob.glob(os.path.join(FOLDER, "*.xlsx")))
    files = [f if os.path.isabs(f) or os.path.exists(f) else os.path.join(FOLDER, f) for f in files]
    db = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=20000)[os.getenv("MANDI_DB_NAME") or "Price"]
    for f in files:
        check(db, f)
