"""
run.py
======
Agmarknet scraper entry-point.

All scraping logic has moved to run2.py which uses the data.gov.in open API
via curl. This module simply re-exports the public ``agmarknet`` function so
that existing callers (e.g. main.py) continue to work without changes.
"""

from markets.agmarknet.run2 import agmarknet  # noqa: F401

__all__ = ["agmarknet"]
