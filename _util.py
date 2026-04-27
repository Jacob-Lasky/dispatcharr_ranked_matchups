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


# Soccer-style trailing club tags. football-data.org canonical names end in
# 'FC' / 'AFC' (e.g. 'Brentford FC', 'Wrexham AFC') but provider channel and
# program titles usually drop the tag. Both scoring.format_channel_name and
# matcher._team_keywords need to know these to match against shortened forms.
TEAM_SUFFIX_TOKENS = ("afc", "fc", "cf", "sc")

# Common second-word soccer suffixes that look distinctive but aren't —
# 'United' alone false-matches Manchester/West Ham/Newcastle/Leeds/Sheffield;
# 'City' alone false-matches Manchester/Leicester/Hull/Cardiff/Swansea/etc.
# matcher._team_keywords excludes these from the last-word fallback so
# 'Manchester United' never gets reduced to 'United' as a match key (which
# would collide with channels like 'Brentford v West Ham United').
GENERIC_TEAM_SECOND_WORDS = (
    "united", "city", "town", "county", "athletic", "albion", "rovers",
    "forest", "wanderers", "rangers", "palace", "hotspur", "villa",
    "wednesday", "real",
)
