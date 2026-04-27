"""Tiny shared utilities. Module sits at the package root so both `plugin.py`
and `sources/*.py` can import without circulars."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Optional


def parse_iso_utc(s: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp (with trailing Z or offset) into a tz-aware
    datetime. Returns None on any parse failure or if `s` is falsy.

    Centralizes the `s.replace("Z", "+00:00")` dance used by every API client.
    """
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def stable_hash_int(s: str) -> int:
    """Process-stable hash of a string. Python's builtin hash() is salted by
    PYTHONHASHSEED — same input gives different output across restarts. This
    uses md5 truncated to 16 hex chars (~64 bits) which is plenty for marker
    uniqueness and is identical across processes / restarts."""
    digest = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)
