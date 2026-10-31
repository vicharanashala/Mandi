"""
catalog.py — approved name links (v2 data model)
================================================
The scraper never decides what a name means. It looks names up in
person-approved link tables and records anything new as *pending*:

  states       { _id: state code, name, spellings[] }
  crop_names   { source, raw_name, raw_variety ("*" = any), product_id, status, ... }
  mandi_names  { source, state_code, raw_name, mandi_id (→ available_mandi._id), status, ... }

status is "pending" | "approved" | "rejected". Only "approved" links return an id.
A listing therefore gets product_id / mandi_id only from an approved link, and
loses it again if the link is rejected or changed — on the very next run.

All tables are loaded once per run; lookups are in-memory dict hits.
"""

from __future__ import annotations

import os
import re
import unicodedata
from datetime import datetime, timezone

from pymongo import ASCENDING, UpdateOne

STATES_COLLECTION = (os.getenv("STATES_COLLECTION") or "states").strip()
CROP_NAMES_COLLECTION = (os.getenv("CROP_NAMES_COLLECTION") or "crop_names").strip()
MANDI_NAMES_COLLECTION = (os.getenv("MANDI_NAMES_COLLECTION") or "mandi_names").strip()

ANY_VARIETY = "*"


def norm(value) -> str:
    """Lower-case, NFKC, single spaces. Empty string for None."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value)).strip().lower()
    return re.sub(r"\s+", " ", text)


def ensure_catalog_indexes(db) -> None:
    db[STATES_COLLECTION].create_index([("spellings", ASCENDING)], name="spellings")
    db[CROP_NAMES_COLLECTION].create_index(
        [("source", ASCENDING), ("raw_name", ASCENDING), ("raw_variety", ASCENDING)],
        unique=True, name="unique_crop_name",
    )
    db[CROP_NAMES_COLLECTION].create_index([("status", ASCENDING), ("listings", -1)], name="status_listings")
    db[CROP_NAMES_COLLECTION].create_index([("product_id", ASCENDING)], name="product_id")
    db[MANDI_NAMES_COLLECTION].create_index(
        [("source", ASCENDING), ("state_code", ASCENDING), ("raw_name", ASCENDING)],
        unique=True, name="unique_mandi_name",
    )
    db[MANDI_NAMES_COLLECTION].create_index([("status", ASCENDING), ("listings", -1)], name="status_listings")
    db[MANDI_NAMES_COLLECTION].create_index([("mandi_id", ASCENDING)], name="mandi_id")


class Catalog:
    """Lookups for one scraper run. Call ``flush()`` once at the end of the run."""

    def __init__(self, db):
        self.db = db
        self.state_by_spelling: dict[str, str] = {}
        for s in db[STATES_COLLECTION].find({}, {"spellings": 1, "name": 1}):
            for sp in [s.get("name"), *(s.get("spellings") or [])]:
                if sp:
                    self.state_by_spelling[norm(sp)] = s["_id"]

        # (source, raw_name, raw_variety) -> (status, product_id)
        self.crop_links: dict[tuple[str, str, str], tuple[str, object]] = {}
        for d in db[CROP_NAMES_COLLECTION].find({}, {"source": 1, "raw_name": 1, "raw_variety": 1, "status": 1, "product_id": 1}):
            self.crop_links[(d["source"], d["raw_name"], d.get("raw_variety") or ANY_VARIETY)] = (d.get("status"), d.get("product_id"))

        # (source, state_code, raw_name) -> (status, mandi_id)
        self.mandi_links: dict[tuple[str, str, str], tuple[str, object]] = {}
        for d in db[MANDI_NAMES_COLLECTION].find({}, {"source": 1, "state_code": 1, "raw_name": 1, "status": 1, "mandi_id": 1}):
            self.mandi_links[(d["source"], d.get("state_code") or "", d["raw_name"])] = (d.get("status"), d.get("mandi_id"))

        # Seen this run: key -> number of records
        self.crop_seen: dict[tuple[str, str, str], int] = {}
        self.mandi_seen: dict[tuple[str, str, str], int] = {}
        self.unknown_states: dict[str, int] = {}

    # ── lookups ──────────────────────────────────────────────────────────

    def state_code(self, state_text) -> str | None:
        key = norm(state_text)
        if not key:
            return None
        code = self.state_by_spelling.get(key)
        if code is None:
            self.unknown_states[key] = self.unknown_states.get(key, 0) + 1
        return code

    def product_for(self, source, raw_name, raw_variety=None):
        """Approved product id for a crop name, else None (and the name is recorded)."""
        source, name, variety = norm(source), norm(raw_name), norm(raw_variety) or ANY_VARIETY
        if not source or not name:
            return None
        exact = self.crop_links.get((source, name, variety))
        if exact is not None:
            self.crop_seen[(source, name, variety)] = self.crop_seen.get((source, name, variety), 0) + 1
            return exact[1] if exact[0] == "approved" else None
        any_key = (source, name, ANY_VARIETY)
        self.crop_seen[any_key] = self.crop_seen.get(any_key, 0) + 1
        link = self.crop_links.get(any_key)
        return link[1] if link and link[0] == "approved" else None

    def mandi_for(self, source, state_code, raw_name):
        """Approved available_mandi _id for a mandi name, else None (and the name is recorded)."""
        key = (norm(source), state_code or "", norm(raw_name))
        if not key[0] or not key[2]:
            return None
        self.mandi_seen[key] = self.mandi_seen.get(key, 0) + 1
        link = self.mandi_links.get(key)
        return link[1] if link and link[0] == "approved" else None

    # ── write-back ───────────────────────────────────────────────────────

    def new_crop_names(self) -> list[tuple[str, str, str]]:
        return [k for k in self.crop_seen if k not in self.crop_links]

    def new_mandi_names(self) -> list[tuple[str, str, str]]:
        return [k for k in self.mandi_seen if k not in self.mandi_links]

    def flush(self, now: datetime | None = None) -> dict:
        """Insert new names as pending; refresh last_seen on every name seen this run."""
        now = now or datetime.now(timezone.utc)
        crop_ops = [
            UpdateOne(
                {"source": s, "raw_name": n, "raw_variety": v},
                {
                    "$setOnInsert": {"product_id": None, "status": "pending", "suggestion": None,
                                     "decided_by": None, "decided_at": None, "notes": None, "first_seen": now},
                    "$set": {"last_seen": now, "records_last_run": count},
                },
                upsert=True,
            )
            for (s, n, v), count in self.crop_seen.items()
        ]
        mandi_ops = [
            UpdateOne(
                {"source": s, "state_code": st or None, "raw_name": n},
                {
                    "$setOnInsert": {"mandi_id": None, "status": "pending", "suggestion": None,
                                     "decided_by": None, "decided_at": None, "notes": None, "first_seen": now},
                    "$set": {"last_seen": now, "records_last_run": count},
                },
                upsert=True,
            )
            for (s, st, n), count in self.mandi_seen.items()
        ]
        stats = {"new_crop_names": len(self.new_crop_names()), "new_mandi_names": len(self.new_mandi_names())}
        if crop_ops:
            self.db[CROP_NAMES_COLLECTION].bulk_write(crop_ops, ordered=False)
        if mandi_ops:
            self.db[MANDI_NAMES_COLLECTION].bulk_write(mandi_ops, ordered=False)
        return stats
