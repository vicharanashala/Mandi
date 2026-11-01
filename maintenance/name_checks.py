"""
Hints for reviewers: does a crop name look unrelated to the crop it is linked to?
Same rules as the review website (src/lib/heuristics.ts). Never used to link anything.
"""

from __future__ import annotations

import re

FILLER = {
    "common", "local", "other", "others", "general", "ordinary", "normal", "regular", "whole", "split",
    "raw", "dry", "dried", "fresh", "green", "red", "white", "black", "yellow", "big", "small", "medium",
    "fine", "superfine", "grade", "loose", "for", "each", "and", "the", "seed", "seeds", "leaf", "leaves",
    "veg", "vegetable", "vegetables", "fruit", "fruits", "dal", "dhal", "with", "per", "kg", "bunch",
}
_LATIN = re.compile(r"^[a-z0-9]+$")


def words_of(text: str | None) -> list[str]:
    return [w for w in re.split(r"[^\w]+", (text or "").lower()) if len(w) >= 3 and w not in FILLER and "_" not in w]


def _edit_distance(a: str, b: str) -> int:
    prev = list(range(len(b) + 1))
    for i in range(1, len(a) + 1):
        diag, prev[0] = prev[0], i
        for j in range(1, len(b) + 1):
            up = prev[j]
            prev[j] = min(prev[j] + 1, prev[j - 1] + 1, diag + (a[i - 1] != b[j - 1]))
            diag = up
    return prev[len(b)]


def _related(a: str, b: str) -> bool:
    if a == b:
        return True
    n = min(4, len(a), len(b))
    if a[:n] == b[:n] or a in b or b in a:
        return True
    if min(len(a), len(b)) < 5 or _edit_distance(a, b) > max(1, max(len(a), len(b)) // 5):
        return False
    # In short words only a missing or extra letter counts ("cummin"/"cumin"); a changed
    # letter often makes a different crop ("chana"/"chafa").
    return min(len(a), len(b)) >= 7 or len(a) != len(b)


def looks_wrong(name: str, canonical: str | None, group_aliases: list[str] | None = None) -> bool:
    name_words = [w for w in words_of(name) if _LATIN.match(w)]
    if not name_words:
        # Only generic words ("other green and fresh vegetables") says nothing about the
        # crop: flag it. A name in another script cannot be judged by spelling: leave it.
        return bool(re.search(r"[a-z]{3,}", (name or "").lower())) and not re.search(r"[^\x00-\x7f]", name or "")
    canon_words = [w for w in words_of(canonical) if _LATIN.match(w)]
    reference = [w for t in [canonical or "", *(group_aliases or [])[:50]] for w in words_of(t) if _LATIN.match(w)]
    trusted = [r for r in reference if any(_related(r, c) for c in canon_words)]
    if not reference:
        return False
    return not any(_related(w, r) for w in name_words for r in trusted)
