"""Dispatcharr Ranked Matchups plugin.

Pipeline:
  1) refresh        -> for each enabled sport source, fetch upcoming games + ranks,
                       compute interestingness score per game (transparent breakdown),
                       LLM-match each game to a Dispatcharr channel via EPG ProgramData,
                       write cache.json. No DB writes.
  2) apply          -> read cache, ensure target ChannelProfile exists, update its
                       membership to the cached channels, optionally rename channels
                       to "CFB 1v5: ...". Honors dry_run.
  3) auto_pipeline  -> refresh + apply.
  4) show_status    -> print cache contents with score breakdown.

Files:
  - cache.json:           last refresh result with per-game score breakdowns
  - llm_descriptions_cache.json: per-game LLM-rewritten prose, keyed by
                          marker + prompt hash. Only written when
                          llm_descriptions_enabled is on. Safe to delete to
                          force regen; structural state is unaffected.
  - cfbd_api_key:         CFBD/CBB-Data bearer token (chmod 600)
  - football_data_api_key: Football-Data.org token (chmod 600)
  - odds_api_key:         The Odds API token (chmod 600)
  - anthropic_api_key:    Claude key (chmod 600), needed for LLM EPG
                          matching, the optional narrative signal, OR the
                          optional LLM-rewritten EPG descriptions.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import types
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # py < 3.9 fallback (won't hit on Dispatcharr's Python 3.13)
    ZoneInfo = None  # type: ignore

from ._util import (
    group_advance_text,
    group_phase_text,
    group_results_lines,
    group_standings_lines,
    is_field_event,
    parse_iso_utc,
    series_phase_text,
    series_record_text,
    series_result_lines,
    stable_channel_number,
    stable_hash_int,
)

# Eager-import tasks so the background-thread launchers and inflight Redis
# helpers are available before any action handler runs. tasks.py used to
# define Celery @shared_tasks but Dispatcharr's worker_ready -> discover_plugins
# wiring fires AFTER the worker's consumer freezes its task strategies dict,
# so plugin-defined Celery tasks are visible to inspect() but rejected by
# the consumer (see tasks.py docstring for the full diagnosis). The threads
# live in the uwsgi worker process and write progress to a Redis key the
# show_status action reads.
from . import tasks  # noqa: F401, E402

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))

# DO NOT derive PLUGIN_KEY from __package__. Dispatcharr's loader wraps every
# plugin in an internal namespace (_dispatcharr_plugin_<key>), so __package__
# resolves to that wrapper, not the folder name. The PluginConfig DB row, the
# REST URL slug, and the Plugins-page card are all keyed on the folder name
# (lowercased, spaces->underscores) per apps/plugins/loader.py. Use the same
# derivation here, or get_current_settings() silently returns {} and the
# scheduler thread idles forever with no error in the logs.
PLUGIN_KEY = os.path.basename(PLUGIN_DIR).replace(" ", "_").lower()

logger = logging.getLogger(f"plugins.{PLUGIN_KEY}")

# Settings key read in two phases: refresh (matcher-side stacking, #108) and
# apply (English-first stream ordering, #111). A shared constant so the two
# reads and plugin.json can't drift apart. Other one-off settings stay inline.
_WIDEN_STREAM_POOL_SETTING = "widen_stream_pool"

# Stream ordering preference. "quality" (default) keeps the historical
# quality-only ordering; "us_preferred" keeps quality primary but breaks ties
# toward US English broadcasts. The value strings are constants (not inline
# literals) so the runtime check and plugin.json can't drift apart silently;
# test_manifest_stream_priority_matches_code pins plugin.json to these.
_STREAM_PRIORITY_SETTING = "stream_priority"
_STREAM_PRIORITY_QUALITY = "quality"       # default: quality-only ordering
_STREAM_PRIORITY_US = "us_preferred"       # quality first, US breaks ties

# Favorites-only curation (Discord req: justinglock40 wanted USMNT-only World
# Cup, not all 48 countries, while keeping the full NFL/NCAA list incl.
# playoffs). A select, not two booleans, so the modes can't contradict each
# other ("both on" is meaningless). The value strings are constants (not inline
# literals) so the runtime read and plugin.json can't drift silently;
# test_manifest_favorites_only_matches_code pins plugin.json to these.
#   - off:        curate every enabled sport normally (historical behavior).
#   - strict:     keep ONLY games involving a Favorites team, across all sports.
#   - postseason: keep Favorites games AND any postseason/playoff game
#                 regardless of favorite (NFL playoffs, NCAA tournament, soccer
#                 knockout). Regular-season league play and WC/EURO GROUP_STAGE
#                 are still favorites-gated, so the all-countries WC group
#                 flood stays suppressed even in this mode.
_FAVORITES_ONLY_SETTING = "favorites_only"
_FAVORITES_ONLY_OFF = "off"                # default: no filtering
_FAVORITES_ONLY_STRICT = "strict"          # favorite-involved games only
_FAVORITES_ONLY_POSTSEASON = "postseason"  # favorites + any postseason game
# The modes that actually filter (everything except OFF / unrecognized).
_FAVORITES_ONLY_ACTIVE_MODES = frozenset({
    _FAVORITES_ONLY_STRICT, _FAVORITES_ONLY_POSTSEASON,
})

CACHE_PATH = os.path.join(PLUGIN_DIR, "cache.json")
# Sidecar cache for LLM-rewritten descriptions. Separate from cache.json so the
# main cache file stays purely deterministic (score, breakdown, score_notes
# unchanged) and the LLM cache can be safely deleted to force regen without
# losing scoring state.
LLM_DESCRIPTIONS_CACHE_PATH = os.path.join(PLUGIN_DIR, "llm_descriptions_cache.json")
# Sidecar cache for SportsDB matchup-thumbnail URLs (marker -> url). Persists
# across runs so apply only HTTP-probes each marker once per _POSITIVE_TTL /
# _NEGATIVE_TTL window. Safe to delete to force re-resolution.
SPORTSDB_THUMB_CACHE_PATH = os.path.join(PLUGIN_DIR, "sportsdb_thumb_cache.json")
CFBD_KEY_PATH = os.path.join(PLUGIN_DIR, "cfbd_api_key")
FD_KEY_PATH = os.path.join(PLUGIN_DIR, "football_data_api_key")
ODDS_KEY_PATH = os.path.join(PLUGIN_DIR, "odds_api_key")
ANTHROPIC_KEY_PATH = os.path.join(PLUGIN_DIR, "anthropic_api_key")
SPORTSDB_KEY_PATH = os.path.join(PLUGIN_DIR, "sportsdb_api_key")
# Free public test tier. Patreon keys unlock higher rate limits per
# https://www.thesportsdb.com/api.php.
SPORTSDB_DEFAULT_KEY = "3"

# Window relative to game start in which the EPG should show the game.
# DEFAULT is calibrated for NCAAF / NFL where broadcasters air long
# pre-game shows + 4h covers overtime. Soccer wants a TIGHTER match
# window so the matcher doesn't false-positive on pre-game wrap-up
# programs that share both team names. See #4.
EPG_PRE_MIN = 30      # 30 min before game starts (default; matches NCAAF / NFL pre-show + OT)
EPG_POST_HOURS = 4    # 4 hours after game starts (default; covers OT)

# Per-sport match-window override (used by _build_epg_lookup to constrain
# the regex pre-filter's time bucket). The ProgramData slot we emit for
# our virtual channel still uses the defaults above: those control
# user-facing display, not match recall. See #4.
#
# Soccer leagues / international tournaments. Soccer games run ~95-120 min
# (90 reg + halftime + stoppage); add ~10 min for extra time + penalties
# in knockouts. 5 min pre + 2.5h post covers the actual broadcast window
# without sweeping in pre-game preview shows from earlier in the day.
_SOCCER_PREFIXES = frozenset({
    "EPL", "EFL", "UCL",
    "BL1", "LaLiga", "SerieA", "Ligue1",
    "Eredivisie", "PrimeiraLiga", "BSA",
    "WC", "EURO",
    "MLS", "NWSL", "LigaMX",
    "NCAAMS", "NCAAWS",
})
_MATCH_WINDOW_OVERRIDE_PRE_MIN: Dict[str, int] = {p: 5 for p in _SOCCER_PREFIXES}
_MATCH_WINDOW_OVERRIDE_POST_HOURS: Dict[str, float] = {p: 2.5 for p in _SOCCER_PREFIXES}


def _epg_match_window(sport_prefix: Optional[str]) -> Tuple[int, float]:
    """Returns (pre_min, post_hours) for the EPG match-window pre-filter.

    Defaults to the NCAAF/NFL-tuned (30, 4); soccer prefixes get (5, 2.5);
    unknown prefixes fall back to the default. See #4.
    """
    pre = _MATCH_WINDOW_OVERRIDE_PRE_MIN.get(sport_prefix or "", EPG_PRE_MIN)
    post = _MATCH_WINDOW_OVERRIDE_POST_HOURS.get(sport_prefix or "", float(EPG_POST_HOURS))
    return pre, post

# Marker we put in tvg_id of cloned channels so we can find/clean them up later
# without needing a custom_properties field on the Channel model.
TVG_ID_PREFIX = "ranked_matchups:"

# Legacy tvg_id values earlier versions of this plugin wrote. The rename
# cleanup pass needs these to recognize leftover EPGData rows on a renamed-from
# EPGSource: a TVG_ID_PREFIX-only check misses sources whose only remaining
# rows are legacy-shaped, leaving the orphan visible in the UI with
# status='error' forever after a target-group rename.
# Add to this tuple when the tvg_id scheme changes; never remove an entry.
_OWNED_TVG_ID_LEGACY_MARKERS: tuple = ("dummy_top_matchups",)


def _owned_tvg_id_q(field_prefix: str = ""):
    """Q matching every tvg_id this plugin has ever written. Used to identify
    rows we own across both the current TVG_ID_PREFIX scheme and any legacy
    markers from earlier plugin versions, so cleanup on group rename is
    complete instead of partial.

    field_prefix: Django ORM lookup path prefix for joined queries. Empty
    when filtering EPGData/Channel directly. Use 'epgs__' from EPGSource,
    'channels__' from ChannelGroup, etc.
    """
    from django.db.models import Q
    return (
        Q(**{f"{field_prefix}tvg_id__startswith": TVG_ID_PREFIX})
        | Q(**{f"{field_prefix}tvg_id__in": _OWNED_TVG_ID_LEGACY_MARKERS})
    )


# ---------- DVR recording preservation (#146) ----------
#
# A completed DVR recording is FK'd to the matchups game channel it was made on
# (apps/channels/models.py: Recording.channel, on_delete=CASCADE). Every apply
# reaps stale (past-game) virtual channels, so without intervention deleting the
# game channel CASCADE-deletes the recording row, orphaning the .mkv on disk and
# making it vanish from the DVR tab. To preserve recordings we re-home them onto
# a persistent archive channel (in a separate, user-named group) BEFORE reaping
# the game channel, and never reap a channel whose recording is still active.

# Default name for the archive group; overridable via the recordings_group_name
# setting. The group is created lazily (only when a recording needs preserving)
# and removed again once it holds no recordings.
DEFAULT_RECORDINGS_GROUP = "Matchups Recordings"

# Stable tvg_id for the single archive channel. Lives under TVG_ID_PREFIX so it
# reads as "ours" and is therefore excluded from the matcher (it must never be
# matched to a game) and from the highest-real-channel scan. It is NOT a game
# marker, so it is never in seen_markers and is scoped out of existing_virtuals
# (which is filtered to the live target_group, not the recordings group).
ARCHIVE_TVG_ID = TVG_ID_PREFIX + "recordings_archive"

# The in-progress value Dispatcharr writes to Recording.custom_properties["status"]
# while a recording is capturing (apps/channels/tasks.py sets it to "recording"
# at start and "completed"/"stopped" at end). We only ever need to recognize the
# in-progress state, to avoid reaping a channel mid-recording. Named here so the
# external contract is documented in one place rather than as a bare literal.
_DVR_STATUS_RECORDING = "recording"


def _recording_is_active(rec, now) -> bool:
    """True when a recording must NOT be stranded by reaping its channel: it is
    in progress, or scheduled/running with an end_time still in the future.

    Pure: `rec` only needs `.custom_properties` (dict or None) and `.end_time`
    (aware datetime or None). Kept ORM-free so it is unit-testable without a
    Django DB (mirrors the offline-policy pattern used elsewhere in this plugin).
    """
    cp = getattr(rec, "custom_properties", None) or {}
    if cp.get("status") == _DVR_STATUS_RECORDING:
        return True
    end = getattr(rec, "end_time", None)
    return end is not None and end > now


def _partition_stale_for_recordings(stale, recs_by_channel, now, archive_enabled):
    """Decide, for stale (past-game) channels, which are safe to reap and which
    recordings to re-home first. Pure policy, no ORM.

    stale: iterable of channel-likes with `.id`.
    recs_by_channel: {channel_id: [recording-likes]} (see _recording_is_active).
    now: aware datetime.
    archive_enabled: whether re-homing is available (False when the archive
        group name clashes with the live group, so recordings cannot be moved).

    Returns (reapable, kept, rehome_rec_ids):
      reapable        channels safe to delete (no recordings, or all done and
                      re-homable).
      kept            channels to leave in place this cycle (active recording,
                      or recordings present but archive disabled so reaping
                      would destroy them). Reconciles next cycle.
      rehome_rec_ids  recording ids on reapable channels to move to the archive.
    """
    reapable, kept, rehome_rec_ids = [], [], []
    for ch in stale:
        recs = recs_by_channel.get(ch.id, [])
        if not recs:
            reapable.append(ch)
            continue
        if any(_recording_is_active(r, now) for r in recs):
            kept.append(ch)
            continue
        if not archive_enabled:
            # No place to preserve them: keep the channel rather than CASCADE
            # the recordings away.
            kept.append(ch)
            continue
        rehome_rec_ids.extend(r.id for r in recs)
        reapable.append(ch)
    return reapable, kept, rehome_rec_ids


def _ensure_archive_channel(recordings_group_name):
    """Get-or-create the persistent recordings group and its single archive
    channel. The archive channel is stream-less (a recording container only),
    has no channel_number (stays out of the highest-real-channel scan), and is
    keyed by ARCHIVE_TVG_ID so it is found again on the next apply."""
    from apps.channels.models import Channel, ChannelGroup
    grp, _ = ChannelGroup.objects.get_or_create(name=recordings_group_name)
    arch = Channel.objects.filter(
        channel_group=grp, tvg_id=ARCHIVE_TVG_ID,
    ).first()
    if arch is None:
        arch = Channel.objects.create(
            name=recordings_group_name,
            channel_group=grp,
            tvg_id=ARCHIVE_TVG_ID,
            channel_number=None,
            auto_created=False,
        )
        logger.info(
            "[ranked_matchups] created recordings archive channel id=%s in group %r",
            arch.id, recordings_group_name,
        )
    return arch


def _cleanup_empty_archive(recordings_group_name) -> bool:
    """Remove the archive channel (and then the group) once no recordings remain
    under it, so the group exists only while it holds recordings. Returns True if
    anything was removed. Safe: the archive channel is deleted only when it has
    zero Recording rows, so the CASCADE takes nothing with it."""
    from apps.channels.models import Channel, ChannelGroup, Recording
    grp = ChannelGroup.objects.filter(name=recordings_group_name).first()
    if grp is None:
        return False
    removed = False
    arch = Channel.objects.filter(channel_group=grp, tvg_id=ARCHIVE_TVG_ID).first()
    if arch is not None and not Recording.objects.filter(channel_id=arch.id).exists():
        arch.delete()
        removed = True
    if Channel.objects.filter(channel_group=grp).count() == 0:
        grp.delete()
        removed = True
    return removed


# Default starting channel number when the user hasn't configured one. Sentinel
# 0 means "auto": pick the first channel number after the highest existing
# non-virtual channel, so we slot in cleanly without colliding with real
# channels.
DEFAULT_VIRTUAL_CHANNEL_BASE = 0

# Default fallback when DEFAULT_VIRTUAL_CHANNEL_BASE is sentinel-0 AND there
# are zero existing channels (fresh install): picked high enough not to
# collide with auto-channel-sync ranges.
_AUTO_BASE_FALLBACK = 9000

# EPGSource fields. DO NOT use source_type="dummy": Dispatcharr's
# EPGGridAPIView treats every channel attached to a dummy source as needing
# joke filler ("Rush Hour - X's alternative to traffic", "What's For Dinner?
# Debate", etc.) and overlays it in the web UI on top of our real ProgramData.
# We're producing real EPG content, so we mark the source as xmltv. is_active
# is False so the EPG refresh task doesn't try to fetch our (None) URL.
EPG_SOURCE_TYPE = "xmltv"
EPG_SOURCE_IS_ACTIVE = False


# Quality-ordering for stacked streams. Lower rank = better quality, listed
# first. We stack multiple provider streams onto a virtual channel as
# fallbacks, so the order matters: clients tend to try in listed order.
_QUALITY_RANK_UHD = 0
_QUALITY_RANK_FHD = 1
_QUALITY_RANK_HD = 2
_QUALITY_RANK_UNKNOWN = 3
_QUALITY_RANK_SD = 4

# Probe-data tiers for the composite sort key. Lower = better.
_PROBE_TIER_VALID = 0      # ffprobe ran and got a real resolution
_PROBE_TIER_NO_PROBE = 1   # ffprobe never ran for this stream
_PROBE_TIER_FAILED = 2     # ffprobe ran but resolution came back 0x0 (likely dead)


def _stream_quality_rank(name: str) -> int:
    """Heuristic quality bucket from a stream/channel name. Whitespace-padded
    so we don't mistake substrings (e.g. 'CHD' won't match 'HD'). Best (UHD/4K)
    sorts smallest so it comes first."""
    if not name:
        return _QUALITY_RANK_UNKNOWN
    n = f" {name.upper()} "
    if " UHD " in n or " 4K " in n or "/UHD" in n or "/4K" in n:
        return _QUALITY_RANK_UHD
    if " FHD " in n or " 1080P " in n or " 1080 " in n:
        return _QUALITY_RANK_FHD
    if " HD " in n or " 720P " in n or " 720 " in n:
        return _QUALITY_RANK_HD
    if " SD " in n or " 480P " in n or " 360P " in n:
        return _QUALITY_RANK_SD
    return _QUALITY_RANK_UNKNOWN


# Language preference tiers for the widen pool (#111). English first, then
# unknown, then non-English. Within each tier the quality ordering still
# applies, so the full order is: English-4K, English-1080, ..., English-SD,
# unknown..., non-English-4K, ..., non-English-SD.
_LANG_RANK_ENGLISH = 0
_LANG_RANK_UNKNOWN = 1
_LANG_RANK_NON_ENGLISH = 2

# Accented Latin letters that mark a Spanish / Portuguese / French feed. ASCII
# punctuation is deliberately excluded so an em dash or "(TM)" doesn't read as
# foreign.
_FOREIGN_ACCENT_CHARS = frozenset("áéíóúñüàèìòùâêîôûäëïöãõçÁÉÍÓÚÑÜÀÈÌÒÙÂÊÎÔÛÄËÏÖÃÕÇ")

# Whitespace-padded tokens that reliably mark an ENGLISH feed. DO NOT add
# "Peacock" or bare "ESPN": Peacock carries both English and Telemundo Spanish,
# and "ESPN Deportes" is Spanish, so both would mislabel non-English feeds.
_ENGLISH_PROVIDER_TOKENS = (
    " BBC ", " ITV ", " TNT ", " TSN ", " SPORTSNET ", " SKY SPORTS ",
    "(UK)", " UK ", " USA ", " ENGLISH ", " ENG ", " EN ",
)

# US-broadcast preference (stream_priority="us_preferred"): rank US English feeds
# ahead of equal-quality non-US ones. US networks ONLY — deliberately excludes
# Canadian (TSN/Sportsnet) and UK (BBC/ITV/Sky) feeds. "TNT" is omitted because
# "TNT Sports" is now UK; "FOX"/"FOX SPORTS" are kept as US (Jake's call). DO NOT
# add bare " US " / " USA ": a USMNT fixture name ("USA vs Mexico") carries those
# as a TEAM and would mislabel a foreign feed as US. A foreign-language marker
# disqualifies a name even with a US token (see _us_broadcast_rank), so ESPN
# Deportes / Telemundo are never treated as the preferred US feed.
_US_PROVIDER_TOKENS = (
    " ESPN ", " ESPN2 ", " ESPNU ", " FS1 ", " FS2 ", " FOX SPORTS ", " FOX ",
    " NBC ", " NBCSN ", " PEACOCK ", " ABC ", " CBS ", " PARAMOUNT ", " TBS ",
    " TRUTV ", " USA NETWORK ", " NFL NETWORK ", " MLB NETWORK ", " NBA TV ",
    " NHL NETWORK ", " BTN ", " BIG TEN NETWORK ", " SEC NETWORK ",
    " ACC NETWORK ", " GOLF CHANNEL ", " TENNIS CHANNEL ", "(US)",
)
_US_RANK_US = 0
_US_RANK_NON_US = 1

# Non-English broadcaster tokens plus Spanish country spellings common in the
# WC Telemundo/Peacock-Spanish feeds whose names carry no accent (e.g. "Estados
# Unidos", "Argelia"). Accented spellings are caught by _FOREIGN_ACCENT_CHARS.
_FOREIGN_PROVIDER_TOKENS = (
    " TELEMUNDO ", " UNIVERSO ", " TUDN ", " DEPORTES ", " MOVISTAR ",
    " CANAL+ ", " SPORTTV ", " SPORT TV ", " GLOBO ", " RAI ", " ZDF ",
    " ARD ", "ESPN DEPORTES", " DAZN ES ", " DAZN DE ", " DAZN IT ",
    " BEIN AR ", " BEIN MENA ",
)
_SPANISH_COUNTRY_HINTS = (
    " ESTADOS UNIDOS ", " ALEMANIA ", " INGLATERRA ", " CROACIA ", " SUIZA ",
    " COSTA DE MARFIL ", " ARGELIA ", " EGIPTO ", " MARRUECOS ", " JORDANIA ",
    " NORUEGA ", " SUECIA ", " DINAMARCA ", " POLONIA ", " GRECIA ", " RUSIA ",
    " UCRANIA ", " ESCOCIA ", " GALES ", " IRLANDA ", " CHEQUIA ",
)

# A foreign-language audio label (e.g. "Czech Feed", "Korean Commentary"): the
# team names are spelled in English but the COMMENTARY is foreign, so the
# English-team-name check would wrongly rank it English. #113: surfaced live on
# a WC channel where "TSN+ Czech Feed" / "Korean Feed" sorted ahead of the
# plain English FIFA feed. "English" is intentionally absent from the language
# list. Matched as "<lang> <feed-noun>" so a team like "Czechia" (no feed noun)
# never trips it.
_FOREIGN_FEED_LANGUAGES = (
    "SPANISH", "FRENCH", "GERMAN", "ITALIAN", "PORTUGUESE", "CZECH", "KOREAN",
    "JAPANESE", "ARABIC", "DUTCH", "POLISH", "RUSSIAN", "TURKISH", "GREEK",
    "DANISH", "SWEDISH", "NORWEGIAN", "CROATIAN", "SERBIAN", "CHINESE",
)
_FOREIGN_FEED_NOUNS = ("FEED", "COMMENTARY", "AUDIO", "COMMS")
_FOREIGN_FEED_MARKERS = tuple(
    f"{lang} {noun}" for lang in _FOREIGN_FEED_LANGUAGES for noun in _FOREIGN_FEED_NOUNS
)


def _has_foreign_language_marker(name: str) -> bool:
    """True when a stream name carries a reliable non-English signal: an
    accented Latin letter, a foreign-language broadcaster, a Spanish country
    spelling, or a foreign-language audio-feed label ("Czech Feed"). Best-effort
    (#111/#113); stays silent on ambiguous names so they land in the unknown
    middle tier rather than being mislabeled."""
    if any(c in _FOREIGN_ACCENT_CHARS for c in name):
        return True
    upper = name.upper()
    if any(marker in upper for marker in _FOREIGN_FEED_MARKERS):
        return True
    padded = f" {upper} "
    if any(tok in padded for tok in _FOREIGN_PROVIDER_TOKENS):
        return True
    if any(tok in padded for tok in _SPANISH_COUNTRY_HINTS):
        return True
    return False


def _stream_language_rank(name: str, home: str = "", away: str = "") -> int:
    """Language-preference bucket for a stream name (#111): English (0),
    unknown (1), non-English (2). Lower sorts earlier.

    Primary signal: both teams' English-name keywords present in the name. The
    WC Spanish feeds spell teams differently ("Turquía" not "Turkey", "Estados
    Unidos" not "United States"), so a name carrying BOTH English spellings is
    an English-language feed. A foreign-marker check (accent / foreign
    broadcaster / Spanish country spelling) runs FIRST and wins, since a loose
    single-word English token would otherwise mislabel a Spanish name. Then the
    both-team-name check, then explicit English provider tokens (for feeds that
    name only one team, e.g. "WC2026: BBC Scotland"). Anything matching nothing
    stays unknown rather than being guessed.
    """
    if not name:
        return _LANG_RANK_UNKNOWN
    # Foreign markers are checked FIRST and win: a single-word English token can
    # otherwise short-circuit to English on a clearly-Spanish name. Real case:
    # "Arabia Saudí v. Uruguay" (game Saudi Arabia vs Uruguay) matches "Arabia"
    # and "Uruguay", yet the "í" in "Saudí" is the reliable Spanish tell.
    if _has_foreign_language_marker(name):
        return _LANG_RANK_NON_ENGLISH
    upper = name.upper()
    if home and away:
        from .matcher import _team_keywords
        home_hit = any(k.upper() in upper for k in _team_keywords(home))
        away_hit = any(k.upper() in upper for k in _team_keywords(away))
        if home_hit and away_hit:
            return _LANG_RANK_ENGLISH
    if any(tok in f" {upper} " for tok in _ENGLISH_PROVIDER_TOKENS):
        return _LANG_RANK_ENGLISH
    return _LANG_RANK_UNKNOWN


def _us_broadcast_rank(name: str) -> int:
    """US-broadcast preference bucket: US English feed (0) vs everything else (1).
    Lower sorts earlier. Used ONLY as a tiebreak BELOW the quality key when
    stream_priority is "us_preferred", so it never promotes a lower-quality US
    feed over a higher-quality non-US one.

    A foreign-language marker disqualifies a name even if it carries a US network
    token, so "ESPN Deportes" / Telemundo Spanish feeds are NOT ranked as the
    preferred US broadcast (the intent is the American ENGLISH feed). DO NOT add
    bare " US " / " USA " tokens to _US_PROVIDER_TOKENS: a USMNT fixture name
    ("USA vs Mexico") carries them as a TEAM and would mislabel a foreign feed.
    """
    if not name or _has_foreign_language_marker(name):
        return _US_RANK_NON_US
    if any(tok in f" {name.upper()} " for tok in _US_PROVIDER_TOKENS):
        return _US_RANK_US
    return _US_RANK_NON_US


def _stream_quality_sort_key(stream_stats, name):
    """Composite quality sort key for a stream. Lower tuple = sorted earlier.

    Tiers (most authoritative first):
      0. Valid ffprobe data: real height ≥ 240 and width ≥ 320.
         Sub-sort by -height (1080p before 720p) then -bitrate.
      1. No probe data at all (Dispatcharr never crawled this stream).
         Sub-sort by name-keyword bucket (UHD > FHD > HD > unknown > SD).
      2. Probe ran and got 0x0: typically a dead/broken stream. Sort last
         even if the name claims UHD, because the probe data overrides
         marketing.
    """
    stats = stream_stats or {}
    if stats:
        height = int(stats.get("height") or 0)
        width = int(stats.get("width") or 0)
        resolution = stats.get("resolution") or ""
        # Some prober runs only populate `resolution` (e.g. '1920x1080')
        # without separate width/height keys. Backfill from the string so
        # those streams don't get misclassified as probe-failed.
        if (not height or not width) and isinstance(resolution, str) and "x" in resolution:
            try:
                w_str, h_str = resolution.split("x", 1)
                if not width:
                    width = int(w_str)
                if not height:
                    height = int(h_str)
            except (TypeError, ValueError):
                pass
        if height >= 240 and width >= 320:
            bitrate = float(stats.get("ffmpeg_output_bitrate") or 0)
            return (_PROBE_TIER_VALID, -height, -bitrate)
        if height == 0 or width == 0 or resolution == "0x0":
            return (_PROBE_TIER_FAILED, 0, 0)
    return (_PROBE_TIER_NO_PROBE, _stream_quality_rank(name), 0)


def _stream_sort_key(stream_stats, name, english_first=False, prefer_us=False, home="", away=""):
    """Stream ordering key. Quality-only by default (historical behavior).

    When english_first is True (#111, set when widen_stream_pool is on), the
    language rank is prepended so ALL English variants sort ahead of ALL
    non-English ones, with the quality ordering preserved within each language
    tier. home/away are the game's English team names, used by
    _stream_language_rank to detect English-language feeds.

    When prefer_us is True (stream_priority="us_preferred"), QUALITY decides
    first and a US-broadcast rank breaks quality ties (a 1080p TSN feed still
    beats a 720p ESPN feed). This DELIBERATELY overrides english_first's
    language-first ordering (#111): with widen_stream_pool on, a high-quality
    feed whose name is not an English token (e.g. "FOX 4K") would otherwise sink
    below a 1080p "TSN" (TSN IS an English token, FOX is not), which is the
    opposite of the quality-first intent. Language is kept only as a final
    sub-tiebreak below quality and US.
    """
    quality = _stream_quality_sort_key(stream_stats, name)
    if prefer_us:
        key = quality + (_us_broadcast_rank(name),)
        if english_first:
            key = key + (_stream_language_rank(name, home, away),)
        return key
    if english_first:
        return (_stream_language_rank(name, home, away),) + quality
    return quality


# ---------- timezone helpers ----------

def _resolve_tz(name: str):
    """Best-effort load a tz; falls back to UTC. Used for 'is_today' classification
    and for formatting kickoff times in the EPG description."""
    if not name:
        name = "UTC"
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone.utc


def _is_today_local(start_utc: datetime, tz) -> bool:
    if start_utc.tzinfo is None:
        start_utc = start_utc.replace(tzinfo=timezone.utc)
    return start_utc.astimezone(tz).date() == datetime.now(tz).date()


def _format_kickoff(start_utc: datetime, tz) -> str:
    if start_utc.tzinfo is None:
        start_utc = start_utc.replace(tzinfo=timezone.utc)
    local = start_utc.astimezone(tz)
    today = datetime.now(tz).date()
    if local.date() == today:
        return f"Today {local.strftime('%-I:%M %p %Z')}"
    if local.date() == today + timedelta(days=1):
        return f"Tomorrow {local.strftime('%-I:%M %p %Z')}"
    return local.strftime("%a %b %-d, %-I:%M %p %Z")


def _format_matchup(home: str, away: str) -> str:
    """Team-pair string for EPG program titles. Plain `Home vs Away`:
    deliberately omits the ★ score prefix the channel name carries, so
    the EPG entry reads like a real broadcast EPG title rather than a
    debug breadcrumb."""
    return f"{home} vs {away}"


# Default duration for the past-event EPG slot when the scheduler is
# disabled OR has no valid scheduled_times. 12h is long enough to bridge
# overnight on any reasonable manual refresh cadence without making the
# past slot unbounded.
_PAST_SLOT_DEFAULT_DURATION = timedelta(hours=12)


def _build_program_title(state: str, matchup: str, kickoff_local: str) -> str:
    """ProgramData.title for one of the three EPG windows on a virtual
    channel. `state` is `"upcoming"` / `"live"` / `"past"`. Truncates
    to 255 chars (the ProgramData column width) with ellipsis."""
    if state == "upcoming":
        title = f"Upcoming: {matchup}, {kickoff_local}" if kickoff_local else f"Upcoming: {matchup}"
    elif state == "live":
        title = f"{matchup} ᴸᶦᵛᵉ"
    elif state == "past":
        title = f"Past: {matchup}"
    else:
        raise ValueError(f"unknown EPG window state: {state!r}")
    if len(title) > 255:
        title = title[:252] + "..."
    return title


def _compute_past_slot_end(prog_end_utc: datetime, settings: Dict[str, Any]) -> datetime:
    """End time for the post-event EPG slot: runs until the next
    scheduled refresh fire-time, so the slot disappears the moment the
    refresh that would replace this channel runs.

    Falls back to prog_end + 12h when:
      - auto_refresh is disabled (no scheduled refreshes coming), OR
      - scheduled_times is empty / malformed (defensive).

    Returns a UTC datetime to match the ProgramData column convention.
    """
    if prog_end_utc.tzinfo is None:
        prog_end_utc = prog_end_utc.replace(tzinfo=timezone.utc)
    if not settings.get("auto_refresh_enabled", False):
        return prog_end_utc + _PAST_SLOT_DEFAULT_DURATION
    tz = _resolve_tz(settings.get("local_timezone", "UTC"))
    times = _parse_scheduled_times(settings.get("scheduled_times", ""))
    if not times:
        return prog_end_utc + _PAST_SLOT_DEFAULT_DURATION
    prog_end_local = prog_end_utc.astimezone(tz)
    next_fire = _next_fire_time(times, tz, now=prog_end_local)
    if next_fire is None:
        return prog_end_utc + _PAST_SLOT_DEFAULT_DURATION
    return next_fire.astimezone(timezone.utc)


# ---------- file helpers ----------

def _read_key(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def _resolve_key(settings: Dict[str, Any], setting_id: str, *fallback_paths: str) -> str:
    """Look up an API key. Setting value (from plugin UI) wins; falls back to
    on-disk file(s) (chmod 600) in order. Returns "" if nothing is present.
    Variadic fallbacks so callers can chain multiple file paths in one call.
    """
    val = settings.get(setting_id) or ""
    if isinstance(val, str) and val.strip():
        return val.strip()
    for path in fallback_paths:
        v = _read_key(path)
        if v:
            return v
    return ""


def _read_cache() -> Dict[str, Any]:
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"games": [], "refreshed_at": None}


def _write_cache(data: Dict[str, Any]) -> None:
    tmp = CACHE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=False, ensure_ascii=False, default=str)
    os.replace(tmp, CACHE_PATH)


# ---------- settings parsing ----------

def _dedup_series_games(games: List[Any], sources: List[Any]) -> Tuple[List[Any], List[Any], int]:
    """Collapse a best-of-N playoff series to its next-scheduled game.

    NHL / NBA / MLB / NCAA-tournament source `fetch_upcoming` calls
    return every scheduled game in a series: including future entries
    whose date is a placeholder for "Game 5/6/7 IF NEEDED". The user
    sees that as 4-7 redundant virtual channels for the same matchup.
    Group rows by `(sport_prefix, frozenset({home, away}))` and keep
    the one with the earliest `start_time`; drop the rest. Returns
    `(deduped_games, deduped_sources, n_dropped)` preserving the
    parallel-index relationship between games and sources.

    DO NOT key on `(sport_prefix, home, away)` with order preserved:
    NHL playoff Games 2 and 4 swap home-ice (the lower-seed gets
    "Carolina at Montreal" alternating with "Montreal at Carolina")
    and a strict ordered key would treat them as distinct.

    League sources also flow through this: same team-pair appearing
    twice in a 7-day lookahead is genuinely rare (would have to be a
    midweek + weekend cup pairing, or a replay), and "show only the
    next one" is the same UX as for playoffs. If a future scenario
    needs both, the dedup needs to grow a `keep_all` opt-out signal
    on the source contract; for now the curation rule is "one channel
    per matchup, soonest first."
    """
    if not games:
        return [], [], 0
    by_key: Dict[Tuple[str, frozenset], int] = {}
    for idx, g in enumerate(games):
        sport_prefix = getattr(g, "sport_prefix", "") or ""
        teams = frozenset({getattr(g, "home", ""), getattr(g, "away", "")})
        key = (sport_prefix, teams)
        prior_idx = by_key.get(key)
        if prior_idx is None:
            by_key[key] = idx
            continue
        # Pick the chronologically earlier of (existing, current).
        # GameRow.start_time is timezone-aware UTC (parse_iso_utc
        # contract); direct comparison is safe.
        prior = games[prior_idx]
        cur_start = getattr(g, "start_time", None)
        prior_start = getattr(prior, "start_time", None)
        if cur_start is not None and (prior_start is None or cur_start < prior_start):
            by_key[key] = idx
    keep_indices = sorted(by_key.values())
    deduped_games = [games[i] for i in keep_indices]
    deduped_sources = [sources[i] for i in keep_indices]
    return deduped_games, deduped_sources, len(games) - len(deduped_games)


def _parse_favorites(raw: str) -> List[str]:
    if not raw:
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


# Curation presets (#9). Each preset bundles a Weights tuning + a max_games
# cap so users don't have to tune 9 individual knobs to get a "vibe":
#
# - "manual": read individual weight + max_games settings (V0 behavior).
# - "high_curation": narrow to ~10 high-leverage games; weights emphasize
#   structural importance + rank, downweight favorites/spread so they don't
#   dominate the list when the user wants tight curation.
# - "balanced": ~25 games with default weights: mirrors the v0 default.
# - "high_coverage": ~50 games with permissive weights so smaller-stakes
#   matchups still show up even on quiet days.
#
# Manual mode and a non-manual preset are mutually exclusive in spirit but
# not enforced. Picking a preset overrides individual weight + max_games
# settings during refresh; users who tune individual knobs should leave the
# preset on "manual".
_CURATION_PRESETS: Dict[str, Dict[str, float]] = {
    "high_curation": {
        "rank": 1.5, "spread": 2.0, "favorite": 4.0, "rivalry": 1.5,
        "tournament": 2.0, "narrative": 0.0, "importance": 4.0,
        "max_games": 10,
    },
    "balanced": {
        # Mirrors Weights() dataclass defaults: kept here for the SAME
        # reason _build_weights pulls from Weights(): if defaults change,
        # the "balanced" preset SHOULD track them. DRY check pinned by tests.
        "rank": 1.0, "spread": 3.0, "favorite": 6.0, "rivalry": 2.0,
        "tournament": 1.5, "narrative": 0.0, "importance": 3.0,
        "max_games": 25,
    },
    "high_coverage": {
        "rank": 0.7, "spread": 4.0, "favorite": 8.0, "rivalry": 2.5,
        "tournament": 1.0, "narrative": 0.0, "importance": 2.5,
        "max_games": 50,
    },
}


def _build_weights(settings: Dict[str, Any]):
    # Source of truth for default values is scoring.Weights's dataclass
    # declaration. Do NOT duplicate the numbers here: when a default
    # changes, the duplicate ALWAYS gets missed and the runtime silently
    # uses the old value until somebody runs the tests.
    from .scoring import Weights
    d = Weights()

    # Preset path: if a non-manual preset is selected, individual weight_*
    # settings are IGNORED and the preset bundle wins. Manual (or any
    # unrecognized preset name) falls through to the individual-setting
    # path so power-users keep their tuning.
    preset_key = str(settings.get("curation_preset", "manual") or "manual").lower()
    if preset_key != "manual" and preset_key in _CURATION_PRESETS:
        p = _CURATION_PRESETS[preset_key]
        return Weights(
            rank=float(p["rank"]),
            spread=float(p["spread"]),
            favorite=float(p["favorite"]),
            rivalry=float(p["rivalry"]),
            tournament=float(p["tournament"]),
            narrative=float(p["narrative"]),
            importance=float(p["importance"]),
        )

    return Weights(
        rank=float(settings.get("weight_rank", d.rank)),
        spread=float(settings.get("weight_spread", d.spread)),
        favorite=float(settings.get("weight_favorite", d.favorite)),
        rivalry=float(settings.get("weight_rivalry", d.rivalry)),
        tournament=float(settings.get("weight_tournament", d.tournament)),
        narrative=float(settings.get("weight_narrative", d.narrative)),
        importance=float(settings.get("weight_importance", d.importance)),
    )


def _resolve_max_games(settings: Dict[str, Any]) -> int:
    """Returns max_games for the curated list, honoring an active preset.

    Non-manual preset → preset's max_games cap wins over individual setting.
    Manual / unrecognized → read the user's max_games setting (default 25,
    matching the balanced preset).
    """
    preset_key = str(settings.get("curation_preset", "manual") or "manual").lower()
    if preset_key != "manual" and preset_key in _CURATION_PRESETS:
        return int(_CURATION_PRESETS[preset_key]["max_games"])
    return int(settings.get("max_games", 25))


# Stage strings that are NOT postseason. A game with any OTHER non-empty
# `extra["stage"]` is treated as a knockout/playoff round. DO NOT invert this
# into an allowlist of postseason stages: every bracket source defines its own
# stage vocabulary (NFL WC/DIV/CONF/SB, NHL/NBA/MLB rounds + FINALS, NCAA
# R64..NCG, BSB_REG/SB_REG regionals, soccer LAST_32..FINAL, golf MAJOR) and new
# ones get added, so over-including an unknown stage as postseason is the safe
# failure direction (mirrors the source layer's "better to over-emit a new
# knockout stage than under-emit a known one" rule in sources/soccer.py).
# Note BSB_REG / SB_REG are postseason Regionals despite the "_REG" suffix, so
# this set is exact strings, never a substring/suffix match. GROUP_STAGE is
# excluded on purpose: it keeps the World Cup / EURO group phase favorites-gated
# even in 'postseason' mode (the original all-countries complaint). The group
# LETTER lives in extra["group_stage"]="GROUP_A", not in extra["stage"].
_NON_POSTSEASON_STAGES = frozenset({
    "REGULAR_SEASON",  # FD.org soccer league play
    "GROUP_STAGE",     # WC / EURO group phase
    "ALLSTAR",         # exhibition (most sources reject it upstream anyway)
    "EVENT",           # field-event regular tour stop (weekly golf/F1/NASCAR)
})


def _empty_refresh_result(msg: str, src_summary: List[str]) -> Dict[str, Any]:
    """Log `msg`, persist an empty cache (so a stale prior cache doesn't keep
    serving games that no longer pass the curator), and return the action's
    ok-with-message payload. Shared by the no-games-fetched and
    everything-filtered-out exits so the two can't drift."""
    logger.info("[ranked_matchups] %s", msg)
    _write_cache({
        "games": [],
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
        "summary": src_summary,
    })
    return {"status": "ok", "message": msg}


def _is_postseason_game(game: Any) -> bool:
    """True when this game is a postseason/playoff round (vs regular season,
    group stage, or a weekly field event). Reads the source-set
    `extra["stage"]`; see `_NON_POSTSEASON_STAGES` for the taxonomy."""
    stage = (getattr(game, "extra", None) or {}).get("stage")
    if not stage:
        # No stage = regular-season league play (NFL/NBA/NHL/MLB only stamp a
        # stage on playoff games; their parsers return None otherwise).
        return False
    return str(stage).strip().upper() not in _NON_POSTSEASON_STAGES


def _filter_favorites_only(games, sources, favorites, mode):
    """Apply the favorites-only curation gate, returning (games, sources,
    dropped_count) filtered in lockstep so the parallel game/source association
    survives. No-op (returns inputs unchanged, dropped=0) when the mode is off,
    unrecognized, or no favorites are configured.

    Favorite matching reuses scoring.match_favorites so the gate and the
    favorite SCORING signal agree on what "involves a favorite" means (same
    word-boundary + soccer-qualifier rules); a divergence would drop a game the
    score still treats as a favorite. In 'postseason' mode a non-favorite game
    is rescued iff `_is_postseason_game` is true."""
    if mode not in _FAVORITES_ONLY_ACTIVE_MODES:
        return games, sources, 0
    if not favorites:
        # Strict mode with no favorites would blank the entire guide; even
        # postseason mode would silently drop all regular-season output. Both
        # are surprising, so favorites-only is a no-op until the user lists at
        # least one favorite. The caller logs a user-facing warning.
        return games, sources, 0

    from .scoring import match_favorites

    kept_games, kept_sources, dropped = [], [], 0
    for g, src in zip(games, sources):
        keep = bool(match_favorites(g.home, g.away, favorites))
        if (
            not keep
            and mode == _FAVORITES_ONLY_POSTSEASON
            and _is_postseason_game(g)
        ):
            keep = True
        if keep:
            kept_games.append(g)
            kept_sources.append(src)
        else:
            dropped += 1
    return kept_games, kept_sources, dropped


def _build_sources(settings: Dict[str, Any]):
    from .sources import (
        GroupStageSoccerSource, KnockoutSoccerSource, MlbPlayoffSource, MlbRegularSource,
        MlsEastSource, MlsWestSource, MlsCupSource, NwslSource, LigaMxSource,
        NbaPlayoffSource, NbaRegularSource,
        WnbaPlayoffSource, WnbaRegularSource,
        NcaawBasketballPlayoffSource, NcaawBasketballRegularSource,
        NcaaBaseballRegularSource, NcaaBaseballPlayoffSource, NcaaBaseballPlayoffBracketSource,
        NcaaSoftballRegularSource, NcaaSoftballPlayoffSource, NcaaSoftballPlayoffBracketSource,
        NcaaSoccerSource, NcaaSoccerCupSource,
        NcaafSource, NcaamSource,
        NflPlayoffSource, NflRegularSource,
        NhlPlayoffSource, NhlRegularSource, SoccerSource,
        F1Source, NascarSource, GolfSource, UfcSource,
        AtpSource, WtaSource,
        InternationalFriendliesSource,
    )
    from .sources.soccer import COMPETITIONS
    from .scoring import LEAGUE_CONTEXTS
    sources = []
    cfbd_key = _resolve_key(settings, "cfbd_api_key", CFBD_KEY_PATH)
    fd_key = _resolve_key(settings, "football_data_api_key", FD_KEY_PATH)
    odds_key = _resolve_key(settings, "odds_api_key", ODDS_KEY_PATH)
    if settings.get("enable_ncaaf", False) and cfbd_key:
        sources.append(NcaafSource(api_key=cfbd_key))
    # NCAAM uses the same CFBD/CBB-Data Bearer token as NCAAF.
    if settings.get("enable_ncaam", False) and cfbd_key:
        sources.append(NcaamSource(api_key=cfbd_key))

    def _make_soccer(comp_key: str):
        """Pick the right SoccerSource subclass for a given competition based
        on its LEAGUE_CONTEXTS format. League-format (PL, ELC, BL1, etc.)
        uses SoccerSource. Knockout-format (CL, EL, etc.) uses
        KnockoutSoccerSource: a different state machine for bracket shape.
        """
        cfg = COMPETITIONS.get(comp_key)
        ctx = LEAGUE_CONTEXTS.get(cfg.fd_code) if cfg else None
        cls = KnockoutSoccerSource if (ctx and ctx.format == "knockout") else SoccerSource
        return cls(comp_key, fd_api_key=fd_key, odds_api_key=odds_key)

    if settings.get("enable_epl", False) and fd_key:
        sources.append(_make_soccer("epl"))
    if settings.get("enable_championship", False) and fd_key:
        sources.append(_make_soccer("championship"))
    if settings.get("enable_ucl", False) and fd_key:
        sources.append(_make_soccer("ucl"))
    # Top-flight European leagues. Same FD.org key as EPL etc.;
    # _make_soccer routes each to SoccerSource because their LEAGUE_CONTEXTS
    # entries default to format="league".
    if settings.get("enable_bundesliga", False) and fd_key:
        sources.append(_make_soccer("bundesliga"))
    if settings.get("enable_la_liga", False) and fd_key:
        sources.append(_make_soccer("la_liga"))
    if settings.get("enable_serie_a", False) and fd_key:
        sources.append(_make_soccer("serie_a"))
    if settings.get("enable_ligue_1", False) and fd_key:
        sources.append(_make_soccer("ligue_1"))
    # International tournaments. Each toggle fans out to TWO sources
    # sharing the same FD.org competition fetch:
    #   - KnockoutSoccerSource via _make_soccer (KO_STAGES bracket); its
    #     fetch_upcoming filters GROUP_STAGE out so it doesn't double
    #     up with the group-stage source.
    #   - GroupStageSoccerSource for the per-group "advance / eliminated"
    #     importance signal (top 2 per group).
    if settings.get("enable_world_cup", False) and fd_key:
        wc_knockout = _make_soccer("world_cup")
        wc_groups = GroupStageSoccerSource(
            "world_cup", fd_api_key=fd_key, odds_api_key=odds_key,
        )
        # Wire the cross-source chain (#53): while wc_knockout's
        # _fetch_bracket_games returns empty (pre-tournament FD.org
        # state), group-game importance routes through
        # monte_carlo_importance_batch_chain so R16+ leverage fires.
        # Once FD publishes real LAST_32 teams, the chain toggles off
        # automatically.
        if isinstance(wc_knockout, KnockoutSoccerSource):
            wc_groups.set_paired_knockout_source(wc_knockout)
        sources.append(wc_knockout)
        sources.append(wc_groups)
    if settings.get("enable_euros", False) and fd_key:
        sources.append(_make_soccer("euros"))
        sources.append(GroupStageSoccerSource(
            "euros", fd_api_key=fd_key, odds_api_key=odds_key,
        ))

    # International friendlies (ESPN, no API key). Exhibition games with no
    # standings, so these sources don't support importance simulation; a
    # friendly surfaces on favorite / rivalry / narrative signals alone. This
    # is the only path for a pre-tournament national-team warm-up (e.g. a
    # USMNT friendly the week before the World Cup) to reach the guide: the
    # world_cup source carries only FD.org tournament fixtures, not friendlies.
    # friendlies_favorites_only (default on) gates these to favorite national
    # teams only: a FIFA window yields dozens of exhibitions between teams the
    # user doesn't follow, and a friendly has no standings/rank to earn a slot
    # on its own. The source does the matching (reusing scoring.match_favorites)
    # so we just hand it the user's favorites and the toggle.
    intl_favorites = _parse_favorites(settings.get("favorites", ""))
    intl_favorites_only = bool(settings.get("friendlies_favorites_only", True))
    if settings.get("enable_intl_friendlies", False):
        sources.append(InternationalFriendliesSource(
            gender="m",
            favorites=intl_favorites,
            favorites_only=intl_favorites_only,
        ))
    if settings.get("enable_intl_friendlies_women", False):
        sources.append(InternationalFriendliesSource(
            gender="w",
            favorites=intl_favorites,
            favorites_only=intl_favorites_only,
        ))

    # Additional FD.org free-tier leagues. Same _make_soccer
    # router; LEAGUE_CONTEXTS uses format="league" so dispatch goes
    # to SoccerSource (not KnockoutSoccerSource).
    if settings.get("enable_eredivisie", False) and fd_key:
        sources.append(_make_soccer("eredivisie"))
    if settings.get("enable_primeira_liga", False) and fd_key:
        sources.append(_make_soccer("primeira_liga"))
    if settings.get("enable_brazilian_serie_a", False) and fd_key:
        sources.append(_make_soccer("brazilian_serie_a"))

    # NFL: no API key required (ESPN unofficial). Same pair-and-seed
    # pattern as NHL/MLB/NBA. Bracket is single-game elimination
    # (SERIES_LENGTH=1 per stage) across 4 rounds: WC -> DIV -> CONF
    # -> SB. Strength sharing matters most here because NFL teams
    # play only 17 regular-season games: even fewer baseline games
    # than WNBA.
    if settings.get("enable_nfl", False):
        nfl_reg = NflRegularSource()
        sources.append(nfl_reg)
        nfl_po = NflPlayoffSource()
        try:
            nfl_po.set_regular_season_strengths(nfl_reg.estimate_strengths())
        except Exception as exc:  # noqa: BLE001
            logger.warning("[nfl] could not seed playoff strengths: %s", exc)
        sources.append(nfl_po)

    # NHL: no API key required (api-web.nhle.com is free). Pair the
    # regular and playoff sources together: the playoff source borrows
    # regular-season strength estimates from the regular source so a
    # 70-game per-team baseline informs playoff-game sampling instead
    # of the league-average prior. If the user enables only one of the
    # two, the playoff source falls back to the 3.0/3.0 prior.
    if settings.get("enable_nhl", False):
        nhl_reg = NhlRegularSource()
        sources.append(nhl_reg)
        nhl_po = NhlPlayoffSource()
        try:
            # Seed playoff strengths from the regular-season fetch so
            # Cup-Final importance reflects per-team scoring skill
            # (not just the league-average prior).
            nhl_po.set_regular_season_strengths(nhl_reg.estimate_strengths())
        except Exception as exc:  # noqa: BLE001
            # Don't let a strengths-seeding hiccup gate the playoff
            # source: it can still run on the default prior.
            logger.warning("[nhl] could not seed playoff strengths: %s", exc)
        sources.append(nhl_po)

    # MLB: no API key required (statsapi.mlb.com is free). Same pair-and-
    # seed pattern as NHL: the playoff source borrows regular-season
    # strength estimates from the regular source so postseason game-
    # sampling reflects per-team scoring skill instead of the 4.5/4.5
    # league-average prior.
    if settings.get("enable_mlb", False):
        mlb_reg = MlbRegularSource()
        sources.append(mlb_reg)
        mlb_po = MlbPlayoffSource()
        try:
            mlb_po.set_regular_season_strengths(mlb_reg.estimate_strengths())
        except Exception as exc:  # noqa: BLE001
            logger.warning("[mlb] could not seed playoff strengths: %s", exc)
        sources.append(mlb_po)

    # NBA: no API key required. ESPN unofficial API is used (stats.nba.com
    # is WAF-blocked from most homelab egress); same pair-and-seed pattern
    # as NHL/MLB: the playoff source borrows regular-season strength
    # estimates from the regular source so a 60-game per-team baseline
    # informs playoff-game sampling instead of the 115/115 league-average
    # prior. If the user enables only one of the two, the playoff source
    # falls back to that default.
    if settings.get("enable_nba", False):
        nba_reg = NbaRegularSource()
        sources.append(nba_reg)
        nba_po = NbaPlayoffSource()
        try:
            nba_po.set_regular_season_strengths(nba_reg.estimate_strengths())
        except Exception as exc:  # noqa: BLE001
            logger.warning("[nba] could not seed playoff strengths: %s", exc)
        sources.append(nba_po)

    # MLS: issue #30 part A: register MlsEastSource + MlsWestSource
    # for per-conference standings importance (playoff seeding is per-
    # conference, not aggregate league). The closeness-only MlsSource
    # stays as the base class for NwslSource / LigaMxSource but is NOT
    # registered for MLS here; the East/West sources own the MLS
    # emission and carry the same closeness signal forward via the
    # shared Odds API helpers from mls.py.
    #
    # Issue #30 part B: MlsCupSource pairs with East+West for strength
    # sharing. Mixed-format bracket: Wild Card single-leg, R1 best-of-3,
    # then single-leg CSF / CF / MLS Cup Final. Per-stage series length
    # via `_series_length_for_stage` (same hook MLB uses for its
    # WC/LDS/LCS/WS mix). Strengths are merged across both conferences
    # before seeding the cup source: the MLS Cup Final is cross-
    # conference, so per-team scoring rates need to be in one dict.
    if settings.get("enable_mls", False):
        mls_east = MlsEastSource(odds_api_key=odds_key or "")
        mls_west = MlsWestSource(odds_api_key=odds_key or "")
        sources.append(mls_east)
        sources.append(mls_west)
        mls_cup = MlsCupSource()
        try:
            merged_strengths: Dict[str, Dict[str, float]] = {}
            merged_strengths.update(mls_east.estimate_strengths())
            merged_strengths.update(mls_west.estimate_strengths())
            mls_cup.set_regular_season_strengths(merged_strengths)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[mls_cup] could not seed playoff strengths: %s", exc)
        sources.append(mls_cup)

    # NWSL: same V1 minimal pattern as MLS (schedule + closeness).
    # Subclasses MlsSource with NWSL-specific endpoint and Odds API
    # key. No importance / playoff bracket in V1.
    if settings.get("enable_nwsl", False):
        sources.append(NwslSource(odds_api_key=odds_key or ""))

    # Liga MX: Mexican top-flight. Same V1 minimal pattern.
    if settings.get("enable_liga_mx", False):
        sources.append(LigaMxSource(odds_api_key=odds_key or ""))

    # Field events (racing + golf). No two-team head-to-head;
    # each row is one race or tournament. Low event volume (~1/week)
    # means "surface if toggled" is the right product: no importance
    # ranking needed.
    if settings.get("enable_f1", False):
        sources.append(F1Source())
    if settings.get("enable_nascar", False):
        sources.append(NascarSource())
    if settings.get("enable_golf", False):
        sources.append(GolfSource())

    # UFC. Same field-event shape: each fight card is one
    # row with home=card title ("UFC 309: Jones vs. Miocic"). PPVs
    # (numbered UFC events) get MAJOR tier, Fight Nights get EVENT.
    if settings.get("enable_ufc", False):
        sources.append(UfcSource())

    # Tennis. ESPN's tennis scoreboard returns whole
    # tournaments (one entry per active event), not individual
    # matches: so tennis fits the FieldEventSource model. Grand
    # Slams + year-end Finals get MAJOR; regular tour stops get
    # EVENT.
    if settings.get("enable_atp", False):
        sources.append(AtpSource())
    if settings.get("enable_wta", False):
        sources.append(WtaSource())

    # WNBA: ESPN unofficial API; same pair-and-seed pattern as NHL/MLB/
    # NBA. Per-stage series lengths via BestOfNSeriesSource hooks: R1
    # best-of-3, SF best-of-5, FINALS best-of-5 in 2024 / best-of-7
    # in 2025+.
    if settings.get("enable_wnba", False):
        wnba_reg = WnbaRegularSource()
        sources.append(wnba_reg)
        wnba_po = WnbaPlayoffSource()
        try:
            wnba_po.set_regular_season_strengths(wnba_reg.estimate_strengths())
        except Exception as exc:  # noqa: BLE001
            logger.warning("[wnba] could not seed playoff strengths: %s", exc)
        sources.append(wnba_po)

    # NCAA Women's Basketball + March Madness: no API key required.
    # Single-game-elim bracket (SERIES_LENGTH=1 per stage). Pair-and-
    # seed strength sharing identical to NHL/MLB/NBA/WNBA.
    if settings.get("enable_ncaaw_basketball", False):
        ncaaw_reg = NcaawBasketballRegularSource()
        sources.append(ncaaw_reg)
        ncaaw_po = NcaawBasketballPlayoffSource()
        try:
            ncaaw_po.set_regular_season_strengths(ncaaw_reg.estimate_strengths())
        except Exception as exc:  # noqa: BLE001
            logger.warning("[ncaaw_basketball] could not seed playoff strengths: %s", exc)
        sources.append(ncaaw_po)

    # NCAA Division I baseball. Free ESPN unofficial API + D1Baseball
    # poll. Regular-season win-count thresholds (30 / 35 / 40 / 45 / 50)
    # drive the in-season importance signal. Two playoff sources fan
    # out under this single toggle:
    #   - NcaaBaseballPlayoffSource: best-of-3 Super Regional + MCWS Final
    #     (BSB_SR + MCWS_F). ESPN headlines carry game numbers, so the
    #     BestOfNSeriesSource state machine fits.
    #   - NcaaBaseballPlayoffBracketSource: 4-team Regional double-elim
    #     + 8-team MCWS bracket (BSB_REG + MCWS). Uses chronological
    #     inference + headline site labels for grouping.
    # All three (regular + 2 playoff sources) share strength data so
    # postseason Poisson sampling reflects per-team scoring skill.
    if settings.get("enable_ncaa_baseball", False):
        ncbsb_reg = NcaaBaseballRegularSource()
        sources.append(ncbsb_reg)
        try:
            ncbsb_strengths = ncbsb_reg.estimate_strengths()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[ncaa_baseball] could not estimate regular-season strengths: %s", exc)
            ncbsb_strengths = {}
        ncbsb_po = NcaaBaseballPlayoffSource()
        ncbsb_po.set_regular_season_strengths(ncbsb_strengths)
        sources.append(ncbsb_po)
        ncbsb_br = NcaaBaseballPlayoffBracketSource()
        ncbsb_br.set_regular_season_strengths(ncbsb_strengths)
        sources.append(ncbsb_br)

    # NCAA Division I softball. Same two-playoff-source fan-out as
    # baseball above: NcaaSoftballPlayoffSource owns the best-of-3
    # stages (SB_SR + WCWS_F), NcaaSoftballPlayoffBracketSource owns
    # the Regional + 8-team WCWS bracket (SB_REG + WCWS).
    if settings.get("enable_ncaa_softball", False):
        ncsbl_reg = NcaaSoftballRegularSource()
        sources.append(ncsbl_reg)
        try:
            ncsbl_strengths = ncsbl_reg.estimate_strengths()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[ncaa_softball] could not estimate regular-season strengths: %s", exc)
            ncsbl_strengths = {}
        ncsbl_po = NcaaSoftballPlayoffSource()
        ncsbl_po.set_regular_season_strengths(ncsbl_strengths)
        sources.append(ncsbl_po)
        ncsbl_br = NcaaSoftballPlayoffBracketSource()
        ncsbl_br.set_regular_season_strengths(ncsbl_strengths)
        sources.append(ncsbl_br)

    # NCAA D1 men's + women's soccer. One source class
    # parametrized on gender: same structure / endpoints / threshold
    # semantics for both, only the ESPN URL slug differs. Standings
    # points (3 W / 1 D / 0 L) drive the importance signal because
    # draws are common in college soccer.
    #
    # Issue #24: NcaaSoccerCupSource (College Cup bracket) pairs with
    # the regular-season source like NHL/MLB/NBA/WNBA: playoff
    # source borrows regular-season strength estimates via
    # `set_regular_season_strengths`. Without the seed, College Cup
    # samples fall back to the 1.5/1.5 league-average prior; with it,
    # the 20-game regular season informs scoring rates for each team.
    if settings.get("enable_ncaa_mens_soccer", False):
        ncaa_msoc_reg = NcaaSoccerSource(gender="m")
        sources.append(ncaa_msoc_reg)
        ncaa_msoc_cup = NcaaSoccerCupSource(gender="m")
        try:
            ncaa_msoc_cup.set_regular_season_strengths(
                ncaa_msoc_reg.estimate_strengths(),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[ncaa_mens_soccer] could not seed cup strengths: %s", exc)
        sources.append(ncaa_msoc_cup)
    if settings.get("enable_ncaa_womens_soccer", False):
        ncaa_wsoc_reg = NcaaSoccerSource(gender="w")
        sources.append(ncaa_wsoc_reg)
        ncaa_wsoc_cup = NcaaSoccerCupSource(gender="w")
        try:
            ncaa_wsoc_cup.set_regular_season_strengths(
                ncaa_wsoc_reg.estimate_strengths(),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[ncaa_womens_soccer] could not seed cup strengths: %s", exc)
        sources.append(ncaa_wsoc_cup)
    return sources


# ---------- refresh ----------

def _action_refresh(settings: Dict[str, Any]) -> Dict[str, Any]:
    from .scoring import GameSignals, score_game
    from .matcher import match_games_to_channels
    from .sources.soccer import _clear_fd_caches

    # Reset the refresh-scoped FD.org caches at the top of every
    # refresh. Soccer sources share two module-level caches (tier-wide
    # fixtures + per-competition season matches) for the lifetime of a
    # refresh; without this reset, a long-running plugin instance would
    # serve stale data on the second and subsequent refreshes.
    _clear_fd_caches()

    favorites = _parse_favorites(settings.get("favorites", ""))
    weights = _build_weights(settings)
    lookahead = int(settings.get("lookahead_days", 7))
    max_games = _resolve_max_games(settings)

    sources = _build_sources(settings)
    if not sources:
        return {"status": "error", "message": "No sport sources enabled."}

    # 1. Fetch. Keep the (source, game) association: compute_match_importance
    # needs the source object per game to run the season-replay simulator. Plain
    # game rows lose the link to which adapter produced them, and reconstructing
    # it from extra["fd_competition_code"] only works for soccer.
    all_games: List[Any] = []
    game_sources: List[Any] = []  # parallel to all_games; source index matches game index
    src_summary = []
    for src in sources:
        try:
            games = src.fetch_upcoming(days_ahead=lookahead)
        except Exception as e:
            logger.exception("[ranked_matchups] source %s fetch failed", src.sport_label)
            src_summary.append(f"{src.sport_label}: error ({e})")
            continue
        all_games.extend(games)
        game_sources.extend([src] * len(games))
        src_summary.append(f"{src.sport_label}: {len(games)} games")
        logger.info("[ranked_matchups] %s: pulled %d games", src.sport_label, len(games))

    if not all_games:
        return _empty_refresh_result(
            "No games found in lookahead window. " + " | ".join(src_summary),
            src_summary,
        )

    # 1a. Series dedup. Best-of-N playoff sources (NHL, NBA, MLB, NCAA
    # basketball/baseball) return EVERY scheduled game in a series, so a
    # Carolina vs Montreal best-of-7 can produce 4-7 separate GameRows
    # with the same team-pair and different dates. The user wants ONE
    # channel per series showing the next game; subsequent series games
    # appear after the prior one is removed by a later refresh. Group
    # by (sport_prefix, frozenset of teams) and keep the chronologically
    # earliest start_time in each group; the rest are dropped from this
    # refresh. League sources are unaffected: same team-pair appearing
    # twice in a 7-day window is rare and means a real fixture/replay.
    all_games, game_sources, deduped_count = _dedup_series_games(
        all_games, game_sources,
    )
    if deduped_count:
        logger.info(
            "[ranked_matchups] series dedup: dropped %d redundant "
            "series-game rows (kept earliest per team-pair)",
            deduped_count,
        )

    # 1b. Favorites-only gate (Discord req). Applied BEFORE scoring so the
    # Monte Carlo importance simulation never runs on a game we're about to
    # drop. Independent of and composable with the per-source
    # friendlies_favorites_only gate (a friendly already gated to favorites
    # simply survives this too). See `_filter_favorites_only`.
    fav_only_mode = str(settings.get(_FAVORITES_ONLY_SETTING, _FAVORITES_ONLY_OFF)
                        or _FAVORITES_ONLY_OFF).lower()
    if fav_only_mode in _FAVORITES_ONLY_ACTIVE_MODES and not favorites:
        logger.warning(
            "[ranked_matchups] favorites_only=%s but no Favorites are "
            "configured; gate is a no-op (would otherwise blank the guide)",
            fav_only_mode,
        )
        src_summary.append("favorites-only: ignored (no favorites set)")
    else:
        all_games, game_sources, fav_dropped = _filter_favorites_only(
            all_games, game_sources, favorites, fav_only_mode,
        )
        if fav_dropped:
            postseason_note = (
                " (postseason games kept)"
                if fav_only_mode == _FAVORITES_ONLY_POSTSEASON else ""
            )
            logger.info(
                "[ranked_matchups] favorites_only=%s: dropped %d non-favorite "
                "game(s)%s", fav_only_mode, fav_dropped, postseason_note,
            )
            src_summary.append(f"favorites-only ({fav_only_mode}): dropped {fav_dropped}")
        if not all_games:
            return _empty_refresh_result(
                "Favorites-only filter dropped every game in the lookahead "
                "window. " + " | ".join(src_summary),
                src_summary,
            )

    # 2. Score. The Lahvička Monte Carlo importance signal covers three
    # related concerns structurally:
    #   - stakes for the playing teams → home + away queries in the batch
    #   - impact_on_favorites for non-playing favorites → favorite queries
    #     in the same batch (one season replay per match shared across all
    #     queries via monte_carlo_importance_batch)
    #   - late-season amplification → emerges naturally from the
    #     contingency table getting sharper as elimination drops more
    #     outcomes out of contention.
    from .scoring import (
        match_favorites, LEAGUE_CONTEXTS, build_impact_narratives,
        compute_match_importance,
    )
    n_importance_sims = int(settings.get("n_importance_sims", 500))
    scored: List[Tuple[Any, GameSignals, Any]] = []
    for g, src in zip(all_games, game_sources):
        extra = g.extra or {}
        comp_code = extra.get("fd_competition_code")
        league_ctx = LEAGUE_CONTEXTS.get(comp_code) if comp_code else None

        # Standings table is still consumed downstream by
        # build_impact_narratives (which writes the natural-language
        # "rooting against X" prose for the EPG description). The
        # importance signal pulls its standings from the simulator's
        # initial_state, not this table: they're built from the same
        # FD.org payload so they agree.
        standings_table = extra.get("standings_table") or []
        favs_with_standings: List[Dict[str, Any]] = []
        if standings_table:
            for fav in favorites:
                fav_lc = fav.lower()
                for entry in standings_table:
                    name = entry.get("name", "")
                    if fav_lc in name.lower():
                        favs_with_standings.append({
                            "name": name,
                            "position": entry["position"],
                            "points": entry.get("points"),
                        })
                        break
        # Pre-render the natural-language impact narrative now and stash
        # on the row so it survives the post-score cap/resort. Apply
        # reads it straight from the cache without redoing the standings
        # lookup. Narrative is editorial output (separate from scoring);
        # the importance signal handles scoring directly.
        g.extra["impact_narratives"] = build_impact_narratives(
            g.rank_home, g.rank_away, g.home, g.away,
            favs_with_standings, standings_table,
        )

        # Monte Carlo importance. Queries cover the two playing teams'
        # outcome bands AND any in-league favorites' outcome bands
        # (so a non-favorite game that swings a favorite's relegation
        # chance gets credit structurally). weight_importance=0
        # disables the per-game cost entirely. Catches any exception
        # so a flaky source can't take down the entire refresh.
        importance_pts: float = 0.0
        importance_notes: List[str] = []
        importance_thresholds_hit: List[str] = []
        if (
            weights.importance > 0
            and getattr(src, "supports_importance", False)
            and league_ctx is not None
            and league_ctx.thresholds
        ):
            try:
                importance_pts, importance_notes, importance_thresholds_hit = (
                    compute_match_importance(
                        src, g, league_ctx, n_sims=n_importance_sims,
                        favorites_in_league=[f["name"] for f in favs_with_standings],
                    )
                )
            except Exception as e:
                logger.warning(
                    "[ranked_matchups] importance failed for %s vs %s: %s",
                    g.home, g.away, e,
                )

        # Rivalry detection: source-set is_rivalry takes precedence (no adapter
        # currently does this, but the door is open); otherwise we consult the
        # static rivalries.json list. See #8.
        from .rivalries import is_rivalry as _is_known_rivalry
        rivalry_flag = bool(g.is_rivalry) or _is_known_rivalry(
            g.home, g.away, g.sport_prefix,
        )

        signals = GameSignals(
            rank_a=g.rank_home,
            rank_b=g.rank_away,
            team_a=g.home,
            team_b=g.away,
            favorite_match=match_favorites(g.home, g.away, favorites),
            spread=g.spread,
            closeness=g.closeness,
            is_rivalry=rivalry_flag,
            tournament_stage=extra.get("stage"),
            importance_points=importance_pts,
            importance_notes=importance_notes,
            importance_thresholds_hit=importance_thresholds_hit,
        )
        score = score_game(signals, weights)
        scored.append((g, signals, score))

    # Adaptive normalization (#7): re-derive each game's final score from
    # the batch's median raw, so top games feel "top" regardless of
    # where in the season we are. Opt-in via `adaptive_scoring`; the
    # default keeps the absolute-tanh compression in score_game.
    if bool(settings.get("adaptive_scoring", False)) and scored:
        from .scoring import adaptive_compress
        raws = [score.raw for _, _, score in scored]
        finals = adaptive_compress(raws)
        for (game, sigs, score), new_final in zip(scored, finals):
            del game, sigs  # unpacking only; the GameScore is what we update
            score.final = round(new_final, 2)

    # Sort: today's games first (0 before 1), then 0-10 score desc, then raw as
    # tiebreak, then start_time ascending. So a game today with ★7 outranks a
    # game next week with ★9.5.
    tz_local = _resolve_tz(settings.get("local_timezone", "UTC"))

    def _sort_key(item):
        # Favorites-first within today's bucket: even a lukewarm Tottenham
        # game should beat a 9.5-rated title-race contender for THIS user.
        # The favorite-weight bump alone wasn't enough: Man City still
        # dominated the top-5 because their stakes+rank pile was bigger
        # than weight_favorite=6 could overcome. Adding favorite-match as
        # a hard sort key guarantees slots 1..N_favorites belong to the
        # favorite-involved games whenever they exist in the bucket.
        game, signals, score = item
        return (
            0 if _is_today_local(game.start_time, tz_local) else 1,
            0 if signals.favorite_match else 1,
            -score.final,
            -score.raw,
            game.start_time,
        )

    scored.sort(key=_sort_key)

    # 3. Cap to max_games but always include favorites. Re-sort using the same
    # key so today-first + final + raw tiebreak survive the cap (otherwise the
    # whole "today's games occupy lowest channel numbers" guarantee breaks
    # whenever the cap kicks in).
    if len(scored) > max_games:
        favs = [s for s in scored if s[1].favorite_match]
        non_favs = [s for s in scored if not s[1].favorite_match]
        keep_non_favs = non_favs[: max(0, max_games - len(favs))]
        scored = sorted(favs + keep_non_favs, key=_sort_key)

    # 4. EPG match each game to a Dispatcharr channel.
    # _build_epg_lookup excludes ALL our virtual channels by tvg_id prefix:
    # covers both the current target group and any orphans from a renamed group.
    epg_lookup = _build_epg_lookup()
    api_key = _resolve_key(settings, "anthropic_api_key", ANTHROPIC_KEY_PATH)
    model = settings.get("model", "claude-haiku-4-5")
    # #108: off-by-default. When on, the matcher stacks same-fixture provider
    # variants as fallback streams so a single upstream 503 doesn't dark out
    # the matchup channel.
    widen = bool(settings.get(_WIDEN_STREAM_POOL_SETTING, False))
    matches = match_games_to_channels(scored, epg_lookup, api_key, model, widen=widen)

    # 5. Build cache payload (transparent: every signal + breakdown stored)
    games_payload = []
    for (game, signals, score), match in zip(scored, matches):
        games_payload.append({
            "sport_prefix": game.sport_prefix,
            "sport_label": game.sport_label,
            "home": game.home,
            "away": game.away,
            "rank_home": game.rank_home,
            "rank_away": game.rank_away,
            "start_time_utc": game.start_time.isoformat(),
            "kickoff_local": _format_kickoff(game.start_time, tz_local),
            "is_today": _is_today_local(game.start_time, tz_local),
            "venue": game.venue,
            "spread": game.spread,
            "closeness": game.closeness,
            "score": score.final,           # 0-10 (display-facing)
            "score_raw": score.raw,         # unbounded sum (sort tiebreak)
            "score_breakdown": score.breakdown,
            "score_notes": score.notes,
            "favorites_matched": signals.favorite_match,
            "is_rivalry": signals.is_rivalry,
            "tournament_stage": signals.tournament_stage,
            "importance_points": signals.importance_points,
            "importance_notes": signals.importance_notes,
            "importance_thresholds_hit": signals.importance_thresholds_hit,
            "channel_id": match.channel_id,           # primary, kept for backward-compat
            "channel_ids": list(match.channel_ids),   # whole-channel matches, primary first
            "stream_ids": list(match.stream_ids),     # stream-granular (Path C stream-name matches)
            "channel_name_current": match.channel_name,
            "program_title": match.program_title,
            "match_method": match.method,
            "match_note": match.note,
            "extra": dict(game.extra) if game.extra else {},
        })

    cache = {
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
        "summary": src_summary,
        "max_games": max_games,
        "weights": asdict(weights),
        "games": games_payload,
    }
    _write_cache(cache)

    matched = sum(1 for g in games_payload if g["channel_id"])
    return {
        "status": "ok",
        "message": (
            f"Refreshed: {len(games_payload)} games scored, {matched} matched to a broadcast. "
            + " | ".join(src_summary)
        ),
    }


# ---------- EPG lookup (closure over Django ORM) ----------

def _build_epg_lookup():
    """Return a callable: GameRow -> List[ChannelCandidate]. Closure over ORM.

    Excludes any channel that is one of OUR virtual channels (see
    _owned_tvg_id_q): covers the configured target group AND any old groups
    left over from a prior target_group_name. Without this, the matcher
    self-matches against our prior-run channels because their EPG titles
    literally contain the team names.

    Pre-filters at the DB level: only fetches programs that are TEAM-relevant
    (program title contains a team keyword OR program's channel has a team
    keyword in its name). Prime-time windows can carry 4000+ programs across
    a Dispatcharr instance: fetching all of them and filtering in Python had
    a 2000-row hard cap that silently dropped real matches (regression
    against the old uncapped path was ch_id=111919 'EPL 07ⓧ: ... vs Brentford
    FC' getting omitted from the candidate list for the live game window).
    """
    from .matcher import ChannelCandidate, _team_keywords, both_teams_in_one_segment
    from apps.channels.models import Channel, Stream
    from apps.epg.models import ProgramData
    from django.db.models import Q

    def lookup(game) -> List[ChannelCandidate]:
        # Per-sport match window: soccer needs a tighter window to avoid
        # false-matching pre-game preview shows earlier in the day; NCAAF
        # / NFL keep the wide default for long pre-game shows + OT. See #4.
        pre_min, post_hours = _epg_match_window(game.sport_prefix)
        window_start = game.start_time - timedelta(minutes=pre_min)
        window_end = game.start_time + timedelta(hours=post_hours)

        # Field events (#127) have no opponent: the away side is the "Field"
        # sentinel. Match on the event name (home) alone, both for the Path A
        # title pre-filter and the Path B channel-name query below, mirroring
        # the matcher's single-sided tiers. Two-team games keep both sides.
        field = is_field_event(game.away, getattr(game, "extra", None))
        home_kws = _team_keywords(game.home)
        away_kws = [] if field else _team_keywords(game.away)
        if field:
            all_kws = home_kws
            if not home_kws:
                return []
        else:
            all_kws = home_kws + away_kws
            if not (home_kws and away_kws):
                return []

        def _or_icontains(dbfield, kws):
            """OR of `<dbfield>__icontains=kw` over kws. One source of truth for
            the three keyword-substring queries below (programme title, channel
            name, stream name).

            Empty kws returns a match-NOTHING Q, NOT a bare `Q()`: an empty
            Django Q matches EVERY row, so a future caller passing no keywords
            would silently select the whole table. All current call sites guard
            against empty kws, but the safe contract is enforced here too.
            """
            if not kws:
                return Q(pk__in=[])
            q = Q()
            for kw in kws:
                q |= Q(**{f"{dbfield}__icontains": kw})
            return q

        # Path A: programs in window whose TITLE mentions any team keyword.
        title_q = _or_icontains("title", all_kws)
        title_progs = list(
            ProgramData.objects
            .filter(start_time__lt=window_end, end_time__gt=window_start)
            .filter(title_q)
            .only("id", "title", "start_time", "end_time", "epg_id")
        )

        # Path B: channels whose NAME mentions BOTH teams. Include them even
        # without EPG entries in window: provider channels often advertise the
        # match in the channel NAME but carry no program data (e.g.
        # 'AU (STAN 01) | Manchester United v Brentford ...', or the World Cup
        # per-game feeds 'FIFA World Cup 2026 06: USA 02:00 Paraguay'). Requiring
        # BOTH teams here (not just any keyword) is deliberate (#123): it is
        # exactly what Tier-1 strict needs, it stops single-team team-branded
        # home channels from leaking into the Tier-3 LLM candidate set, and it
        # keeps short broadcast aliases (e.g. 'USA') from dragging in every
        # unrelated channel that merely contains the substring.
        if field:
            # Single-sided: a channel naming the event is the broadcast. ANDing
            # the "Field" sentinel here (as the two-team path does) would match
            # nothing, which is the #127 bug.
            name_q = _or_icontains("name", home_kws)
        else:
            name_q = _or_icontains("name", home_kws) & _or_icontains("name", away_kws)
        name_match_chans = list(
            Channel.objects
            .filter(name_q)
            .exclude(_owned_tvg_id_q())
            .only("id", "name", "epg_data_id")
        )

        # Merge: programs (Path A) → resolved channels, plus name-matched
        # channels (Path B) which contribute one synthetic candidate each
        # using the game's broadcast slot when they have no real EPG entry.
        out: List[ChannelCandidate] = []
        seen_channel_ids = set()

        if title_progs:
            epg_ids = {p.epg_id for p in title_progs if p.epg_id}
            chan_qs = (
                Channel.objects
                .filter(epg_data_id__in=epg_ids)
                .exclude(_owned_tvg_id_q())
                .only("id", "name", "epg_data_id")
            )
            chan_by_epg = {}
            for c in chan_qs:
                chan_by_epg.setdefault(c.epg_data_id, []).append(c)
            for p in title_progs:
                for c in chan_by_epg.get(p.epg_id, []):
                    if c.id in seen_channel_ids:
                        continue
                    seen_channel_ids.add(c.id)
                    out.append(ChannelCandidate(
                        channel_id=c.id,
                        channel_name=c.name,
                        program_title=p.title or "",
                        program_start=p.start_time,
                        program_end=p.end_time,
                    ))

        for c in name_match_chans:
            if c.id in seen_channel_ids:
                continue
            seen_channel_ids.add(c.id)
            out.append(ChannelCandidate(
                channel_id=c.id,
                channel_name=c.name,
                program_title="",  # no real program; tier-1 matches via channel name
                program_start=game.start_time,
                program_end=window_end,
            ))

        # Path C: STREAMS whose NAME names both teams (or, for field events, the
        # event name). Providers spin up dedicated per-match feeds whose matchup
        # lives in the STREAM name ('USA Soccer10: ... Iran vs New Zealand')
        # while the parent channel is generically named and carries no EPG, so
        # Path A (EPG title) and Path B (channel name) both miss them. We match
        # the stream by name and attach ONLY that stream (stream-granular):
        # channel_id is a negative sentinel (never a real PK), and channel_name
        # is the stream name so the matcher's Tier-1 treats a stream naming both
        # teams with the same confidence as a channel naming both teams. Streams
        # carry no schedule, so there is no time-window filter; the specific
        # team-pair gate is what keeps it tight. Re-selecting a stream already
        # attached to our own virtual channel is harmless: the apply de-dupes
        # and the end state converges. The DB predicate is identical to Path B's
        # `name_q` (both query a `name` field with the same both-teams gate), so
        # it is reused rather than rebuilt; the per-segment refinement below is
        # what makes the stream-name case safe against feed-prefix collisions.
        for s in Stream.objects.filter(name_q).only("id", "name"):
            # Guard the feed-prefix false positive (e.g. 'USA Soccer09: Australia
            # vs Turkey' matching United States vs Australia: 'USA' is the feed
            # label, not the team). Two-team games require both sides to co-occur
            # in ONE ':'/'|'-delimited segment of the name. Field events are
            # single-sided (just the event name), so the segment gate does not
            # apply.
            if not field and not both_teams_in_one_segment(
                s.name or "", home_kws, away_kws
            ):
                continue
            out.append(ChannelCandidate(
                channel_id=-s.id,            # sentinel: not a real channel PK
                channel_name=s.name or "",   # stream name → Tier-1 sees both teams
                program_title=s.name or "",
                program_start=game.start_time,
                program_end=window_end,
                stream_id=s.id,
            ))
        return out

    return lookup


# ---------- apply ----------

def _build_marker_key(game: Dict[str, Any]) -> str:
    """Stable identifier per game so reruns find existing virtual channels.

    Format: ranked_matchups:<sport>:<source-id-or-fallback>

    The fallback hash MUST be process-stable: Python's builtin hash() is
    salted by PYTHONHASHSEED so it changes between restarts. That would
    cause every soccer match (no cfbd_id) to look like a different game on
    each refresh, which spuriously deletes-and-recreates the virtual
    channel every run. Use stable_hash_int instead.
    """
    sport = game.get("sport_prefix", "?")
    extra = game.get("extra") or {}
    cfbd_id = extra.get("cfbd_id")
    if cfbd_id:
        return f"{TVG_ID_PREFIX}{sport}:{cfbd_id}"
    fd_id = extra.get("fd_id")
    if fd_id:
        return f"{TVG_ID_PREFIX}{sport}:fd_{fd_id}"
    fallback = f"{game.get('away','')}|{game.get('home','')}|{game.get('start_time_utc','')}"
    return f"{TVG_ID_PREFIX}{sport}:{stable_hash_int(fallback)}"


def _resolve_virtual_base(settings: Dict[str, Any], highest_non_virtual: float) -> int:
    """Resolve the starting channel number for our virtual channels.

    `virtual_channel_base` setting:
      - Positive int → use that as the base.
      - 0 (sentinel) → auto: pick (highest existing non-virtual channel) + 1,
        so virtuals slot in just after the user's real channels.
      - Anything unparseable → treat as auto.

    `highest_non_virtual` is the max channel_number across all channels that
    are NOT ours (see _owned_tvg_id_q for the ownership predicate). Caller
    passes 0 if there are no other channels.

    The auto path falls back to _AUTO_BASE_FALLBACK on a fresh install (no
    other channels exist) so we don't return 1 and squat on prime real estate.
    """
    raw = settings.get("virtual_channel_base", DEFAULT_VIRTUAL_CHANNEL_BASE)
    try:
        base = int(float(raw))
    except (TypeError, ValueError):
        base = 0
    if base > 0:
        return base
    candidate = int(highest_non_virtual) + 1
    return candidate if candidate > 1 else _AUTO_BASE_FALLBACK


def _resolve_park_base(max_target_number: int) -> int:
    """Pick a parking range that's guaranteed past every target number we'll
    write. Parking + writing happens in one transaction within our own group,
    so we only need to clear our own target range; +1000 slack keeps the
    parking range comfortably out of the highest target we're about to assign.

    `max_target_number` is the largest channel number this apply will write
    (channel numbers are kickoff-time based, so the ceiling is driven by how far
    ahead the slate reaches, NOT by the game count). Callers pass the target
    base itself when the slate is empty so the result stays sane."""
    return int(max_target_number) + 1000


def _assign_channel_numbers(
    games: List[Dict[str, Any]], virtual_base: int, tz
) -> Dict[str, int]:
    """Map each game's marker to its stable channel number for this apply.

    Numbers come from `stable_channel_number` (a pure function of kickoff time +
    marker), so a given game keeps the same integer for its whole life
    regardless of how the slate is ranked or which other games are present. That
    stability is what lets both the default M3U/XMLTV output and the Xtream Codes
    API bind the EPG correctly with no client setting (#121), while the numbers
    still increase with kickoff time so the list sorts soonest-first.

    Two DIFFERENT games can only land on the same number if they share a kickoff
    minute AND a hash slot (uncommon; see CHANNEL_NUMBER_TIEBREAK_SLOTS). That
    would violate the unique ``(channel_group, channel_number)`` constraint, so
    we resolve it deterministically: walk the games in (number, marker) order and
    bump any exact duplicate to the next free integer. This perturbs ONLY a
    colliding same-minute game, never games at other minutes, and is reproducible
    across applies."""
    pairs: List[Tuple[str, int]] = []
    for g in games:
        start_dt = parse_iso_utc(g.get("start_time_utc"))
        if start_dt is None:
            continue
        marker = _build_marker_key(g)
        pairs.append((marker, stable_channel_number(virtual_base, start_dt, marker, tz)))

    pairs.sort(key=lambda mn: (mn[1], mn[0]))
    assigned: Dict[str, int] = {}
    used: set = set()
    collisions = 0
    for marker, number in pairs:
        while number in used:
            number += 1
            collisions += 1
        used.add(number)
        assigned[marker] = number
    if collisions:
        logger.warning(
            "[ranked_matchups] resolved %d channel-number collision(s) by +1 nudge; "
            "raise CHANNEL_NUMBER_TIEBREAK_SLOTS in _util.py if this recurs",
            collisions,
        )
    return assigned


def _build_signals_score_from_payload(g: Dict[str, Any]):
    """Reconstruct GameSignals + GameScore from cache.json payload.

    Cache files written by older versions of this plugin used a different
    key for the tagline thresholds (`stakes_thresholds_hit` instead of
    `importance_thresholds_hit`) and may carry retired fields that the
    current GameSignals doesn't accept (stakes_a/_b, season_progress,
    impact_on_favorites). Reading both keys gives a one-cycle migration
    window so the first apply against a stale cache still produces a
    tagline; the retired fields are simply ignored.
    """
    from .scoring import GameSignals, GameScore
    thresholds_hit = (
        g.get("importance_thresholds_hit")
        or g.get("stakes_thresholds_hit")
        or []
    )
    # is_rivalry: cache shape may or may not carry it explicitly (older caches
    # won't), but the score_breakdown will have a "rivalry" key when the
    # signal fired during the original refresh. Reading the breakdown is the
    # cheapest single-source-of-truth; no need to re-walk rivalries.json here.
    breakdown_peek = g.get("score_breakdown") or {}
    rivalry_from_cache = bool(g.get("is_rivalry")) or ("rivalry" in breakdown_peek)

    signals = GameSignals(
        rank_a=g.get("rank_home"),
        rank_b=g.get("rank_away"),
        team_a=g.get("home", ""),
        team_b=g.get("away", ""),
        favorite_match=g.get("favorites_matched", []),
        spread=g.get("spread"),
        closeness=g.get("closeness"),
        is_rivalry=rivalry_from_cache,
        tournament_stage=g.get("tournament_stage"),
        # Cache files predating this signal default to 0.0 / []: graceful
        # degradation: the importance block in score_game just contributes
        # nothing for that entry.
        importance_points=float(g.get("importance_points") or 0.0),
        importance_notes=list(g.get("importance_notes") or []),
        importance_thresholds_hit=list(thresholds_hit),
    )
    # `score_raw` is the unbounded sum, `score` is the 0-10 compressed value.
    # If the cache predates `score_raw`, fall back to the breakdown sum (still
    # raw-scale) rather than to `score` (which would mix the two scales).
    breakdown = g.get("score_breakdown") or {}
    raw = g.get("score_raw")
    if raw is None:
        raw = sum(v for v in breakdown.values() if isinstance(v, (int, float)))
    score = GameScore(
        raw=float(raw),
        final=float(g.get("score", 0.0)),
        breakdown=breakdown,
        notes=g.get("score_notes", []),
    )
    return signals, score


def _close_game_descriptor(
    closeness: Optional[float], spread: Optional[float]
) -> Optional[str]:
    """Map the close-game signal to a human-readable label. Prefers the
    closeness measure (devigged probabilities, [0,1]) when present; falls
    back to point-spread thresholds otherwise.

    Soccer's `closeness >= 0.7` (each side ≥35% of a 3-way market) is the
    coinflip-magnitude analog of NCAAF's `spread <= 3`. The two bands
    are calibrated so an EPL toss-up and an NCAAF toss-up produce the
    same label.
    """
    if closeness is not None:
        if closeness >= 0.7:
            return "toss-up"
        if closeness >= 0.45:
            return "close spread"
        return None
    if spread is None:
        return None
    if spread <= 3:
        return "toss-up"
    if spread <= 6:
        return "close spread"
    return None


def _build_subtitle(g: Dict[str, Any], tagline: str) -> str:
    """Compressed one-line summary for the EPG sub-title field. Three pieces
    joined by ' · ': tagline, matchday/week, spread descriptor. Falls back
    to the sport label when nothing else fires."""
    parts: List[str] = []
    if tagline:
        parts.append(tagline)
    extra = g.get("extra") or {}
    matchday = extra.get("matchday")
    matchdays_total = extra.get("matchdays_total")
    if matchday and matchdays_total:
        # Lowercase "catch-up" prefix for sub-title (consistent with the
        # rest of the subtitle's lowercase fragments). See #3.
        prefix = "catch-up matchday" if _is_catchup_matchday(g) else "matchday"
        parts.append(f"{prefix} {matchday}/{matchdays_total}")
    elif extra.get("week"):  # NCAAF / NCAAM week-based sports
        parts.append(f"week {extra['week']}")
    spread_desc = _close_game_descriptor(g.get("closeness"), g.get("spread"))
    if spread_desc:
        parts.append(spread_desc)
    if not parts:
        return g.get("sport_label", "")
    return " · ".join(parts)


def _league_context_for(g: Dict[str, Any]):
    """Resolve the LEAGUE_CONTEXTS entry for a cache row. Returns the
    LeagueContext or None. Single source of truth for both the deterministic
    description builder and the LLM context builder.
    """
    from .scoring import LEAGUE_CONTEXTS
    fd_code = (g.get("extra") or {}).get("fd_competition_code")
    return LEAGUE_CONTEXTS.get(fd_code) if fd_code else None


# A fixture's matchday is "catch-up" when it's at least this many rounds
# behind the league's current matchday (= max playedGames across the table).
# 1 isn't enough: normal weekly scheduling routinely puts midweek games at
# matchday N-1 vs weekend games at matchday N. 2 catches genuine
# rescheduled-postponed fixtures (FA Cup, weather) without false-firing.
_CATCHUP_MATCHDAY_GAP = 2


def _is_catchup_matchday(g: Dict[str, Any]) -> bool:
    """True when this fixture's matchday is ≥ _CATCHUP_MATCHDAY_GAP rounds
    behind the rest of the league.

    The league's "current matchday" is derived from `max(played)` across the
    cached standings table. Returns False for non-league fixtures (no
    standings table → no current-matchday signal) and for caches that
    predate the 'played' field. See #3.
    """
    extra = g.get("extra") or {}
    matchday = extra.get("matchday")
    if not isinstance(matchday, int):
        return False
    table = extra.get("standings_table") or []
    played_counts = [e.get("played") for e in table if isinstance(e.get("played"), int)]
    if not played_counts:
        return False
    league_current = max(played_counts)
    return matchday <= league_current - _CATCHUP_MATCHDAY_GAP


_ORDINAL_SUFFIXES = ("th", "st", "nd", "rd")


def _ordinal(n: int) -> str:
    """1 → '1st', 2 → '2nd', 4 → '4th', 11 → '11th', 23 → '23rd'.

    DO NOT use `min(n%10, 3)` to index the suffixes: that clamps 4..9 to
    "rd", yielding "4rd"/"5rd"/etc. The teen rule (11-13 are "th", not
    "st"/"nd"/"rd") handles n%100 in [11..13]; everything else uses the
    last digit, defaulting to "th" for 0 and 4..9.
    """
    last_two = n % 100
    if 11 <= last_two <= 13:
        return f"{n}th"
    last = n % 10
    suffix = _ORDINAL_SUFFIXES[last] if last <= 3 else "th"
    return f"{n}{suffix}"


def _build_standings_posture_line(g: Dict[str, Any]) -> Optional[str]:
    """One-line standings summary for league fixtures.

    Format: "<home> <pos>, <pts> pts. <away> <pos>, <pts> pts: <gap>."
    where <gap> is computed for the away team relative to the home team.

    Returns None if there's no standings table (knockout / non-soccer), if
    neither playing team appears in the table (e.g. cold-start with
    promoted teams), or if essential fields are missing.

    Surfaces the position+points data the soccer source already cached
    under extra.standings_table: see #10.
    """
    extra = g.get("extra") or {}
    table = extra.get("standings_table") or []
    if not table:
        return None

    home_name = g.get("home", "")
    away_name = g.get("away", "")
    if not home_name or not away_name:
        return None

    # Exact-name lookup against the FD.org table. The home/away names in
    # the GameRow come from the SAME FD.org fixture payload as the table,
    # so they match byte-for-byte: no fuzzy matching needed.
    by_name = {entry.get("name"): entry for entry in table if entry.get("name")}
    home_entry = by_name.get(home_name)
    away_entry = by_name.get(away_name)
    if not home_entry and not away_entry:
        return None

    def _format(name: str, entry: Optional[Dict[str, Any]]) -> Optional[str]:
        if not entry:
            return None
        pos = entry.get("position")
        pts = entry.get("points")
        if pos is None or pts is None:
            return None
        return f"{name} {_ordinal(int(pos))}, {int(pts)} pts"

    home_str = _format(home_name, home_entry)
    away_str = _format(away_name, away_entry)
    if not home_str and not away_str:
        return None
    if not home_str:
        return away_str + "."
    if not away_str:
        return home_str + "."

    # Both teams in the table: add a gap descriptor for the away team
    # relative to the home team. Reads naturally: "...69 pts: 1 pt behind."
    home_pts = int(home_entry["points"])
    away_pts = int(away_entry["points"])
    diff = away_pts - home_pts
    if diff > 0:
        unit = "pt" if diff == 1 else "pts"
        gap = f"{diff} {unit} ahead"
    elif diff < 0:
        n = -diff
        unit = "pt" if n == 1 else "pts"
        gap = f"{n} {unit} behind"
    else:
        # Tied on points: goal difference is the actual league
        # tiebreaker, so surface it when both entries have GD cached
        # (older caches predating #10 won't have it; fall through to the
        # bare "level on points" framing in that case).
        home_gd = home_entry.get("goal_difference")
        away_gd = away_entry.get("goal_difference")
        if home_gd is not None and away_gd is not None:
            gd_diff = away_gd - home_gd
            if gd_diff > 0:
                gap = f"level on points, {gd_diff} GD ahead"
            elif gd_diff < 0:
                gap = f"level on points, {-gd_diff} GD behind"
            else:
                gap = "level on points and goal difference"
        else:
            gap = "level on points"

    return f"{home_str}. {away_str}: {gap}."


def _build_description(
    g: Dict[str, Any],
    tagline: str,
    placeholder: bool,
) -> str:
    """Build the EPG ProgramData description in natural-language form.

    Layout (each block separated by a blank line):
      1. Placeholder note (only if EPG hasn't matched a source yet)
      2. Headline: tagline + spread descriptor ("A title race: toss-up.")
      2b. Series state (playoff best-of-N games only): phase + record, then
          a completed-game recap line. Renders nothing for league fixtures.
      2c. Group-stage state (WC / EURO group games only): round + group
          standings + finished results + advancement rule. Renders nothing
          for non-group fixtures.
      3. Matchday + league boundary summary (where applicable)
      4. Standings posture line (league fixtures only: see #10)
      5. Favorite-impact narratives (rooting framing, both deltas)
      6. "Favorite is your team" line if the favorite is playing this game
      7. Source channel line (only if matched)

    Deliberately dropped: kickoff time (already shown by EPG client time
    blocks), score breakdown (already in channel name as ★X.X), spread's
    raw line value (just say "toss-up"), late-season multiplier
    annotation (uniform across all current league games: adds no signal).
    """
    extra = g.get("extra") or {}
    favorites_matched = g.get("favorites_matched") or []
    impact_narratives = (
        extra.get("impact_narratives")
        or g.get("impact_narratives")
        or []
    )

    sections: List[str] = []

    # 1. Placeholder note.
    if placeholder:
        sections.append(
            "_Channel match pending: broadcaster's EPG hasn't published "
            "this fixture yet. Will activate on the next refresh once it "
            "appears._"
        )

    # 2. Headline: tagline + spread descriptor.
    headline_parts = []
    if tagline:
        article = "An" if tagline[:1].lower() in "aeiou" else "A"
        headline_parts.append(f"{article} {tagline}")
    spread_desc = _close_game_descriptor(g.get("closeness"), g.get("spread"))
    if spread_desc:
        headline_parts.append(spread_desc)
    if headline_parts:
        sections.append(": ".join(headline_parts) + ".")

    # 2b. Series state for playoff best-of-N games. `extra["series"]` is the
    # sport-agnostic schema populated by series sources (NHL today). The phase
    # + record sentence is what tells the reader this is Game 1 of 7 with the
    # series tied, instead of leaving the framing to guesswork: it's the same
    # grounding the LLM context uses to avoid hallucinating "elimination".
    series = extra.get("series")
    series_phase = series_phase_text(series)
    series_record = series_record_text(series, g.get("home", ""), g.get("away", ""))
    if series_phase or series_record:
        parts = []
        if series_phase:
            parts.append(series_phase + ".")
        if series_record:
            parts.append(series_record + ".")
        sections.append(" ".join(parts))
        recap = series_result_lines(series)
        if recap:
            sections.append(" · ".join(recap))

    # 2c. Group-stage state for WC / EURO group games. `extra["group_stage"]`
    # is the schema GroupStageSoccerSource populates (group letter, current
    # table, finished results, advance rule). Same grounding the LLM context
    # uses: it's what stops a group game's prose inventing a "shock opening
    # loss" when the standings actually show the team top of the group.
    group_stage = extra.get("group_stage")
    group_phase = group_phase_text(group_stage)
    if group_phase:
        sections.append(group_phase + ".")
        standings_lines = group_standings_lines(group_stage)
        if standings_lines:
            sections.append(" · ".join(standings_lines))
        result_lines = group_results_lines(group_stage)
        if result_lines:
            sections.append(" · ".join(result_lines))
        advance = group_advance_text(group_stage)
        if advance:
            sections.append(advance)

    # 3. Matchday line + league boundary reminder. Both are league-based
    # ("why is this a race"). Matchday tells you where in the season we
    # are; boundary_summary explains what positions get what.
    league_ctx = _league_context_for(g)
    matchday = extra.get("matchday")
    matchdays_total = extra.get("matchdays_total") or (
        league_ctx.matchdays_total if league_ctx else None
    )
    matchday_line_parts: List[str] = []
    # Group-stage games already carry "Group C, Matchday 2 of 3" in section 2c;
    # don't repeat the matchday here.
    if matchday and matchdays_total and not group_phase:
        # "Catch-up matchday X of Y" when fixture is meaningfully behind
        # the league's current pacing: see _is_catchup_matchday and #3.
        # Without the label, an end-of-season "Matchday 40 of 46" reads as
        # if the team has 6 games left when really it's a postponement
        # being replayed late and they have 1.
        label = "Catch-up matchday" if _is_catchup_matchday(g) else "Matchday"
        matchday_line_parts.append(f"{label} {matchday} of {matchdays_total}.")
    if league_ctx and league_ctx.boundary_summary:
        matchday_line_parts.append(league_ctx.boundary_summary + ".")
    if matchday_line_parts:
        sections.append(" ".join(matchday_line_parts))

    # 4. Standings posture (league fixtures only: knockout cups have no
    # standings table, so this renders None and is skipped). Comes BEFORE
    # the favorite-impact narratives so the reader gets the raw "where are
    # they in the table?" before the editorial "why should you care?"
    # framing.
    standings_line = _build_standings_posture_line(g)
    if standings_line:
        sections.append(standings_line)

    # 5. Favorite-impact narratives.
    for narrative in impact_narratives:
        sections.append(narrative)

    # 6. Favorite is playing in this game.
    if favorites_matched:
        labels = ", ".join(favorites_matched)
        if len(favorites_matched) == 1:
            sections.append(f"{labels} is your favorite.")
        else:
            sections.append(f"Your favorites: {labels}.")

    # 7. Source channel.
    src_name = g.get("channel_name_current")
    if src_name:
        sections.append(f"Source: {src_name}.")

    return "\n\n".join(sections)


def _action_apply(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Clone-into-group + EPG-overlay strategy:

    For each scored+matched game we:
      1. Get-or-create a virtual Channel in 'Top Matchups' ChannelGroup,
         linked to the same streams as the source channel (so playback works).
      2. Get-or-create an EPGData entry on our 'Top Matchups' EPGSource
         (source_type=xmltv, is_active=False: we write programs directly,
         and we mustn't be source_type=dummy or Dispatcharr's UI overlays
         joke-filler descriptions on top of ours).
         Then replace its ProgramData for the game's airtime with title=matchup
         and description=WHY breakdown.

    The description shows up natively in TiviMate/Plex/Jellyfin guides.
    Source channels are never touched. Stale virtual channels (game no longer
    in cache) are deleted along with their EPG entries.
    """
    from apps.channels.models import Channel, ChannelGroup, ChannelStream, Recording, Stream
    from apps.epg.models import EPGSource, EPGData, ProgramData
    from django.db import transaction

    from .scoring import format_channel_name

    cache = _read_cache()
    games = cache.get("games", [])
    if not games:
        return {"status": "ok", "message": "Cache empty; run refresh first."}

    group_name = settings.get("channel_profile_name", "Top Matchups")
    # Archive group for preserved DVR recordings (#146). Blank falls back to the
    # default. It MUST differ from the live group: if they collide the archive
    # channel would land in the reaped group and defeat the feature, so we
    # disable preservation (and keep recordings safe by not reaping channels
    # that have them) until the user sets a distinct name.
    recordings_group_name = (
        str(settings.get("recordings_group_name", "") or "").strip()
        or DEFAULT_RECORDINGS_GROUP
    )
    archive_enabled = recordings_group_name.lower() != str(group_name).strip().lower()
    if not archive_enabled:
        logger.warning(
            "[ranked_matchups] recordings_group_name (%r) matches the live group; "
            "recording preservation disabled until a distinct name is set. "
            "Channels with recordings will be kept rather than reaped.",
            recordings_group_name,
        )
    dry_run = bool(settings.get("dry_run", True))
    # #111: when the widen pool is on, order each channel's pooled streams
    # English+quality first, then non-English. Same toggle as the matcher-side
    # widening (widen_stream_pool); language only matters once a channel has
    # more than one stream, which is what widening produces.
    english_first = bool(settings.get(_WIDEN_STREAM_POOL_SETTING, False))
    # Stream ordering: quality decides first always; "us_preferred" additionally
    # breaks quality ties toward US English broadcasts (see _stream_sort_key).
    prefer_us = settings.get(_STREAM_PRIORITY_SETTING, _STREAM_PRIORITY_QUALITY) == _STREAM_PRIORITY_US

    # Channel-name template (issue #100). Empty/unset -> the built-in default.
    # A malformed custom template must never crash an apply or poison live
    # channel names: validate, and on any problem fall back to the default and
    # log loudly (the "test naming convention" action is where users catch this
    # before it ships).
    from . import naming
    name_template = str(settings.get("name_template") or "").strip() or None
    if name_template is not None:
        tmpl_errors = naming.validate_template(name_template)
        if tmpl_errors:
            logger.warning(
                "[ranked_matchups] invalid name_template (%s); using default",
                "; ".join(tmpl_errors),
            )
            name_template = None
    apply_tz = _resolve_tz(settings.get("local_timezone", "UTC"))

    # 1. Ensure target ChannelGroup. Also detect any old groups/sources we own
    # (from a previous target_group_name) and clean them up: fixes the case
    # where the user renames "Top Matchups" → "!Top Matchups" between runs.
    target_group = ChannelGroup.objects.filter(name=group_name).first()

    # Find any other groups containing channels we own. Helper covers both the
    # current TVG_ID_PREFIX scheme and any legacy markers from earlier plugin
    # versions (a prefix-only check would miss leftovers on rename).
    # Exclude the recordings archive group/source (#146) from the rename-cleanup
    # sweep: the archive channel is intentionally "ours" (so the matcher skips
    # it), which would otherwise make this sweep migrate it into the live group
    # and delete the archive group out from under preserved recordings.
    _protected_group_names = [group_name, recordings_group_name]
    foreign_owned_groups = list(
        ChannelGroup.objects.exclude(name__in=_protected_group_names)
        .filter(_owned_tvg_id_q("channels__"))
        .distinct()
    )
    foreign_epg_sources = list(
        EPGSource.objects.exclude(name__in=_protected_group_names)
        .filter(_owned_tvg_id_q("epgs__"))
        .distinct()
    )

    if not target_group:
        if dry_run:
            return {
                "status": "ok",
                "message": (
                    f"[dry] Would create ChannelGroup {group_name!r} + EPGSource and clone "
                    f"{sum(1 for g in games if g.get('channel_id'))} matched games into it. "
                    f"Would also clean up {len(foreign_owned_groups)} stale group(s) and "
                    f"{len(foreign_epg_sources)} stale EPGSource(s) from prior target names."
                ),
            }
        target_group = ChannelGroup.objects.create(name=group_name)
        logger.info("[ranked_matchups] created ChannelGroup id=%s name=%r",
                    target_group.id, group_name)

    # Migrate any virtual channels in old groups INTO the target group, in
    # place. DO NOT delete + recreate: Channel.id is the stable handle that
    # ChannelProfileMembership, IPTV-client playlist caches, and the user's
    # pinned-channel state all key off. A delete-then-create cycle silently
    # orphans every one of those: profile memberships are gone (Dispatcharr
    # auto-adds new channels to existing profiles ONLY at profile-creation
    # time, never on channel-creation), and IPTV clients that cached the old
    # tvg-id render an empty slot for the renamed channel until the user
    # manually refreshes: exactly what bit us during the #1 live-verify.
    #
    # The .update() bypasses Django's post_save signal (mirroring the same
    # signal-bypass pattern at apps/channels/signals.py:60 / our epg_data
    # write at the bottom of the loop). The downstream main loop will hit
    # the "existing" branch keyed by tvg_id and update name / channel_number
    # / logo / streams in place. ChannelStream rows are FK'd to Channel, so
    # they survive the channel_group change untouched.
    migrated_from_old_group = 0
    deleted_old_groups = 0
    if not dry_run and foreign_owned_groups:
        for old_g in foreign_owned_groups:
            old_chans = Channel.objects.filter(_owned_tvg_id_q(), channel_group=old_g)
            n = old_chans.count()
            old_chans.update(channel_group=target_group)
            migrated_from_old_group += n
            # If the old group is now empty AND was named like a Top Matchups
            # (heuristic: contains 'matchup' or 'top' in name), delete it too.
            if (
                Channel.objects.filter(channel_group=old_g).count() == 0
                and any(s in old_g.name.lower() for s in ("matchup", "top"))
            ):
                old_g.delete()
                deleted_old_groups += 1
                logger.info("[ranked_matchups] deleted empty old group %r", old_g.name)

    # Same for old EPGSources we own
    deleted_old_sources = 0
    if not dry_run and foreign_epg_sources:
        for old_src in foreign_epg_sources:
            # Only auto-delete if all its EPGData entries are ours (no other plugin owns it)
            total = EPGData.objects.filter(epg_source=old_src).count()
            ours = EPGData.objects.filter(_owned_tvg_id_q(), epg_source=old_src).count()
            if total > 0 and total == ours:
                old_src.delete()  # cascades EPGData + ProgramData
                deleted_old_sources += 1
                logger.info("[ranked_matchups] deleted old EPGSource %r (%d entries)",
                            old_src.name, total)
            else:
                # Mixed: just nuke our entries, leave the source alone
                EPGData.objects.filter(_owned_tvg_id_q(), epg_source=old_src).delete()

    # 2. Ensure our EPGSource exists and is configured to sit out of
    # Dispatcharr's joke-filler path (see EPG_SOURCE_TYPE comment above).
    epg_source = EPGSource.objects.filter(name=group_name).first()
    if not epg_source:
        if dry_run:
            logger.info("[ranked_matchups] [dry] would create EPGSource name=%r type=%s",
                        group_name, EPG_SOURCE_TYPE)
        else:
            epg_source = EPGSource.objects.create(
                name=group_name,
                source_type=EPG_SOURCE_TYPE,
                is_active=EPG_SOURCE_IS_ACTIVE,
                refresh_interval=0,
            )
            logger.info("[ranked_matchups] created EPGSource id=%s name=%r",
                        epg_source.id, group_name)
    else:
        # Existing source: upgrade away from any prior source_type="dummy" /
        # is_active=True we may have written. Idempotent for already-correct rows.
        upgrade_fields = []
        if epg_source.source_type != EPG_SOURCE_TYPE:
            upgrade_fields.append("source_type")
            epg_source.source_type = EPG_SOURCE_TYPE
        if epg_source.is_active != EPG_SOURCE_IS_ACTIVE:
            upgrade_fields.append("is_active")
            epg_source.is_active = EPG_SOURCE_IS_ACTIVE
        if upgrade_fields:
            if dry_run:
                logger.info("[ranked_matchups] [dry] would upgrade EPGSource id=%s fields=%s",
                            epg_source.id, upgrade_fields)
            else:
                epg_source.save(update_fields=upgrade_fields)
                logger.info("[ranked_matchups] upgraded EPGSource id=%s fields=%s",
                            epg_source.id, upgrade_fields)

    # 3. Existing virtual channels we'll update or delete
    existing_virtuals = {
        ch.tvg_id: ch for ch in Channel.objects.filter(
            _owned_tvg_id_q(), channel_group=target_group,
        )
    }

    # Resolve the virtual channel base. In auto mode we slot in just after
    # the user's highest real channel (excluding our own virtuals) so we
    # don't squat on prime numbers like 1-100.
    from django.db.models import Max as _Max
    highest_other = (
        Channel.objects.exclude(_owned_tvg_id_q())
        .aggregate(m=_Max("channel_number"))["m"] or 0
    )
    virtual_base = _resolve_virtual_base(settings, highest_other)
    # Stable per-game channel numbers (marker -> number). Computed once up front
    # so collision resolution sees the whole slate and so park_base can be set
    # above the true ceiling (kickoff-time based, so driven by how far ahead the
    # slate reaches, not by the game count).
    chnum_by_marker = _assign_channel_numbers(games, virtual_base, apply_tz)
    max_target = max(chnum_by_marker.values(), default=virtual_base)
    park_base = _resolve_park_base(max_target)
    logger.info(
        "[ranked_matchups] virtual_base=%d (highest_other=%s, setting=%r), "
        "max_target=%d, park_base=%d",
        virtual_base, highest_other,
        settings.get("virtual_channel_base", DEFAULT_VIRTUAL_CHANNEL_BASE),
        max_target, park_base,
    )

    created = 0
    updated = 0
    deleted_stale = 0
    skipped_unmatched = 0
    rehomed_recordings = 0      # #146: recordings moved to the archive this run
    kept_for_recording_n = 0    # #146: stale channels kept (active recording)
    seen_markers = set()

    placeholder_threshold = float(settings.get("placeholder_min_score", 5.0))
    placeholder_channels_created = 0

    # Optional Claude-rewritten EPG descriptions. Default off; when on, prose
    # replaces the deterministic `_build_description` output for non-placeholder
    # games. Failures fall back silently. cache.json (scores, breakdown,
    # score_notes) is untouched: only ProgramData.description changes.
    from . import llm_descriptions
    llm_enabled = bool(settings.get("llm_descriptions_enabled", False))
    llm_api_key = ""
    llm_model = ""
    llm_cache: Dict[str, str] = {}
    llm_used = 0
    llm_failed = 0
    if llm_enabled:
        llm_api_key = _resolve_key(settings, "anthropic_api_key", ANTHROPIC_KEY_PATH)
        llm_model = str(settings.get("model", "claude-haiku-4-5") or "claude-haiku-4-5")
        if not llm_api_key:
            logger.warning(
                "[ranked_matchups] llm_descriptions_enabled=on but no anthropic_api_key resolved; "
                "falling back to deterministic descriptions."
            )
            llm_enabled = False
        else:
            llm_cache = llm_descriptions.read_cache(LLM_DESCRIPTIONS_CACHE_PATH)

    # Per-matchup logo: TheSportsDB pre-renders a 960x540 graphic for every
    # event (both crests + league wordmark + region backdrop). Look up by
    # team-name pair, download to /data/logos/ranked_matchups_<hash>.jpg, and
    # point Channel.logo at it. On any miss (no event indexed, network failure,
    # field-event source, dry_run) we fall through to the source channel's
    # logo: preserving the v0 behavior for the long tail. Per-marker thumb
    # URLs cached on disk to keep API hits to once per fixture per ~14 days.
    from . import logos as matchup_logos
    matchup_logos_enabled = bool(settings.get("enable_matchup_logos", True))
    sportsdb_api_key = SPORTSDB_DEFAULT_KEY
    thumb_cache: Optional[matchup_logos.ThumbCache] = None
    matchup_logos_used = 0
    matchup_logos_badge = 0
    matchup_logos_fallback = 0
    if matchup_logos_enabled and not dry_run:
        sportsdb_api_key = (
            _resolve_key(settings, "sportsdb_api_key", SPORTSDB_KEY_PATH)
            or SPORTSDB_DEFAULT_KEY
        )
        thumb_cache = matchup_logos.ThumbCache(SPORTSDB_THUMB_CACHE_PATH)

    def _resolve_league_badge_id(game: Dict[str, Any]) -> Optional[int]:
        """Resolve a cached league/tournament badge Logo id for this game, or
        None when the sport_prefix is unmapped or the badge can't be fetched.

        Badges are shared per league: one file per league id, reused across all
        that league's games, and skipped by the per-game sweep. The local file
        IS the cache (no badge re-fetch once downloaded). Only reached on the
        matchup-thumb miss path, which already requires logos-enabled + not
        dry_run, so the HTTP here is appropriately gated.
        """
        league_id = matchup_logos.league_id_for(
            game.get("sport_prefix"), game.get("tournament_stage"),
        )
        if league_id is None:
            return None
        badge_path = os.path.join(
            matchup_logos.LOGO_DIR, matchup_logos.badge_filename(league_id),
        )
        if not os.path.exists(badge_path):
            badge_url = matchup_logos.resolve_league_badge_url(league_id, sportsdb_api_key)
            if not badge_url:
                return None
            try:
                os.makedirs(matchup_logos.LOGO_DIR, exist_ok=True)
            except OSError as e:
                logger.warning("[ranked_matchups] cannot mkdir %s: %s", matchup_logos.LOGO_DIR, e)
                return None
            if not matchup_logos.download_thumb(badge_url, badge_path):
                return None
        from apps.channels.models import Logo
        logo_obj, _ = Logo.objects.get_or_create(
            url=badge_path,
            defaults={"name": f"League badge: {game.get('sport_prefix','?')}"},
        )
        return logo_obj.id

    def _resolve_matchup_logo_id(
        game: Dict[str, Any], marker: str, source,
    ) -> Tuple[Optional[int], str]:
        """Returns (logo_id, outcome) where outcome is one of "matchup", "badge",
        or "channel" so the caller can tally each per apply.

        Resolution order (issue #102): team-vs-team matchup thumbnail → league /
        tournament badge → source-channel logo. The provider channel logo is the
        last resort, not the first fallback. Dry_run and feature-disabled paths
        short-circuit to the channel logo before any HTTP.
        """
        channel_logo = source.logo_id if source else None
        if not matchup_logos_enabled or dry_run or thumb_cache is None:
            return channel_logo, "channel"

        def _badge_or_channel() -> Tuple[Optional[int], str]:
            badge_id = _resolve_league_badge_id(game)
            if badge_id is not None:
                return badge_id, "badge"
            return channel_logo, "channel"

        fresh, cached_url = thumb_cache.get(marker)
        thumb_url = cached_url
        if not fresh:
            start_dt = parse_iso_utc(game.get("start_time_utc"))
            if start_dt is None:
                return _badge_or_channel()
            thumb_url = matchup_logos.resolve_thumb_url(
                home=game.get("home", ""),
                away=game.get("away", ""),
                expected_dt=start_dt,
                sport_prefix=game.get("sport_prefix"),
                api_key=sportsdb_api_key,
            )
            thumb_cache.put(marker, thumb_url)
        if not thumb_url:
            return _badge_or_channel()
        # Ensure /data/logos/ exists (Dispatcharr creates it lazily on first
        # upload via the UI; we might race a fresh install).
        try:
            os.makedirs(matchup_logos.LOGO_DIR, exist_ok=True)
        except OSError as e:
            logger.warning("[ranked_matchups] cannot mkdir %s: %s", matchup_logos.LOGO_DIR, e)
            return _badge_or_channel()
        local_path = os.path.join(
            matchup_logos.LOGO_DIR, matchup_logos.marker_to_filename(marker),
        )
        if not os.path.exists(local_path):
            if not matchup_logos.download_thumb(thumb_url, local_path):
                return _badge_or_channel()
        from apps.channels.models import Logo
        logo_obj, _ = Logo.objects.get_or_create(
            url=local_path,
            defaults={"name": f"Top Matchup: {game.get('home','?')} vs {game.get('away','?')}"},
        )
        return logo_obj.id, "matchup"

    # Resolve ALL network-backed values BEFORE opening the transaction (#136).
    # The apply transaction below must hold only fast DB writes. LLM-description
    # and SportsDB-logo calls are network I/O; making them INSIDE
    # transaction.atomic() holds the transaction (and its DB connection) open
    # across every slow call, which starves the login/token worker on large
    # instances and on the scheduled refresh (observed: a 14s login timeout +
    # a 13.9s Postgres checkpoint right after an apply on a ~6.3k-channel box).
    # Both calls are cache-backed, so this pass moves them earlier, it does not
    # add work. Each surviving game's precomputed plan is stored by marker; the
    # transaction then does nothing but writes. This pre-pass mirrors the skip /
    # placeholder / bad-start-time semantics of the write loop exactly (same
    # marker key, same seen_markers set, same counters) so the two stay in lockstep.
    from types import SimpleNamespace
    from .scoring import pick_tagline
    prep_by_marker: Dict[str, Any] = {}
    for g in games:
        source_ids = list(g.get("channel_ids") or [])
        # Path C stream-granular matches: specific streams to attach without
        # pulling in their parent channel's other streams. Older caches lack
        # this key (→ []), so behaviour is unchanged for them.
        explicit_stream_ids = list(g.get("stream_ids") or [])
        if not source_ids and not explicit_stream_ids:
            primary_id = g.get("channel_id")
            # A positive primary is a real channel; a negative one is a Path C
            # stream sentinel with no whole-channel to expand, so ignore it here
            # (its stream rides in explicit_stream_ids).
            if primary_id and primary_id > 0:
                source_ids = [primary_id]
        sources = list(Channel.objects.filter(id__in=source_ids))
        sources_by_id = {c.id: c for c in sources}
        sources = [sources_by_id[sid] for sid in source_ids if sid in sources_by_id]
        source = sources[0] if sources else None

        placeholder = False
        # A stream-only match (no whole-channel source, but explicit streams) is
        # a real match, NOT a placeholder.
        if not source and not explicit_stream_ids:
            score_val = float(g.get("score", 0.0))
            if score_val >= placeholder_threshold:
                placeholder = True
                placeholder_channels_created += 1
            else:
                skipped_unmatched += 1
                continue

        marker = _build_marker_key(g)
        seen_markers.add(marker)

        signals, score = _build_signals_score_from_payload(g)
        extra = g.get("extra") or {}
        rank_source = extra.get("rank_source", "poll")
        tagline = pick_tagline(
            score_breakdown=g.get("score_breakdown", {}),
            favorites_matched=g.get("favorites_matched", []),
            spread=g.get("spread"),
            closeness=g.get("closeness"),
            importance_thresholds=(
                g.get("importance_thresholds_hit")
                or g.get("stakes_thresholds_hit")  # pre-C.4 cache fallback
                or []
            ),
            tournament_stage=g.get("tournament_stage"),
            rank_a=g.get("rank_home"),
            rank_b=g.get("rank_away"),
            rank_source=rank_source,
        )
        start_dt = parse_iso_utc(g.get("start_time_utc"))
        if start_dt is None:
            logger.warning("[ranked_matchups] bad start_time_utc on %s", marker)
            continue

        new_name = format_channel_name(
            g["sport_prefix"], signals, score, g["home"], g["away"],
            tagline=tagline,
            template=name_template,
            rank_source=rank_source,
            sport_label=g.get("sport_label", ""),
            venue=g.get("venue"),
            start_dt=start_dt,
            tz=apply_tz,
        )

        description = _build_description(g=g, tagline=tagline, placeholder=placeholder)
        # Optional Claude-rewritten prose (network, cached). Placeholders keep the
        # deterministic "match pending" note. Failures fall back to `description`.
        if llm_enabled and not placeholder:
            _ctx = _league_context_for(g)
            _boundary = _ctx.boundary_summary if _ctx else ""
            before = description
            description = llm_descriptions.llm_describe_or_fallback(
                g=g,
                tagline=tagline,
                fallback_description=description,
                api_key=llm_api_key,
                model=llm_model,
                cache=llm_cache,
                boundary_summary=_boundary,
                marker=marker,
            )
            if description is before:
                llm_failed += 1
            else:
                llm_used += 1

        # Rank the streams from every matched source channel (DB reads only).
        stream_pool = []  # list of (sort_key, src_order, stream_id)
        seen_stream_ids = set()
        for src_order, src in enumerate(sources):
            for s in src.streams.all().only("id", "name", "stream_stats"):
                if s.id in seen_stream_ids:
                    continue
                seen_stream_ids.add(s.id)
                key = _stream_sort_key(
                    s.stream_stats, s.name or src.name or "",
                    english_first=english_first, prefer_us=prefer_us,
                    home=g["home"], away=g["away"],
                )
                stream_pool.append((key, src_order, s.id))
        # Path C: attach the specific stream-name-matched streams, NOT their
        # parent channels' other (unrelated) streams. Ordered after the channel
        # sources; the quality sort below re-ranks the whole pool anyway, so the
        # base offset is only a stable tiebreak. De-duped against channel
        # streams already pooled.
        if explicit_stream_ids:
            base = len(sources)
            by_id = {
                s.id: s for s in Stream.objects.filter(id__in=explicit_stream_ids)
                .only("id", "name", "stream_stats")
            }
            for j, sid in enumerate(explicit_stream_ids):
                s = by_id.get(sid)
                if s is None or s.id in seen_stream_ids:
                    continue
                seen_stream_ids.add(s.id)
                key = _stream_sort_key(
                    s.stream_stats, s.name or "",
                    english_first=english_first, prefer_us=prefer_us,
                    home=g["home"], away=g["away"],
                )
                stream_pool.append((key, base + j, s.id))
        stream_pool.sort()
        source_streams = [sid for _, _, sid in stream_pool]

        resolved_logo_id, logo_outcome = _resolve_matchup_logo_id(g, marker, source)
        if matchup_logos_enabled and not dry_run:
            if logo_outcome == "matchup":
                matchup_logos_used += 1
            elif logo_outcome == "badge":
                matchup_logos_badge += 1
            else:
                matchup_logos_fallback += 1

        prep_by_marker[marker] = SimpleNamespace(
            new_name=new_name,
            description=description,
            tagline=tagline,
            logo_id=resolved_logo_id,
            source_streams=source_streams,
            start_dt=start_dt,
        )

    with transaction.atomic():
        # Phase 0: park existing virtual channels in a high temporary number
        # range so we can renumber based on cache order without colliding with
        # the unique (channel_group, channel_number) constraint. park_base is
        # guaranteed to be past every target number we're about to write.
        # Capture pre-park numbers so a channel we end up KEEPING (active
        # recording, #146) can be restored to its real number instead of being
        # stranded at a ~park_base value: only "seen" games get renumbered in
        # the loop below, so without this a kept stale channel would sit at the
        # parking number until the next cycle reaps it.
        prepark_numbers = {ch.id: ch.channel_number for ch in existing_virtuals.values()}
        if not dry_run and existing_virtuals:
            parked = list(existing_virtuals.values())
            for i, ch in enumerate(parked):
                ch.channel_number = float(park_base + i)
            # One UPDATE rather than N per-row saves (#136): on a large slate of
            # existing virtuals the park step shouldn't issue a query per channel.
            Channel.objects.bulk_update(parked, ["channel_number"])

        for g in games:
            # Per-game writes consume the plan resolved in the pre-pass above
            # (#136); the transaction holds no network I/O. `prep` is None for
            # games the pre-pass skipped (unmatched below the placeholder
            # threshold, or a bad start_time): keying off the same marker and
            # skipping here keeps the two passes in lockstep without re-running
            # resolution or double-counting. _build_marker_key is pure, so the
            # marker computed here equals the one the pre-pass stored under.
            marker = _build_marker_key(g)
            prep = prep_by_marker.get(marker)
            if prep is None:
                continue
            new_name = prep.new_name
            description = prep.description
            tagline = prep.tagline
            resolved_logo_id = prep.logo_id
            source_streams = prep.source_streams
            start_dt = prep.start_dt
            # Stable, kickoff-time-based channel number (keyed by marker, see
            # _assign_channel_numbers / #121). Guaranteed present: both this map
            # and the pre-pass skip exactly the same bad-start_time rows, so any
            # marker with a plan also has a number here.
            target_chnum = chnum_by_marker[marker]
            prog_start = start_dt - timedelta(minutes=EPG_PRE_MIN)
            prog_end = start_dt + timedelta(hours=EPG_POST_HOURS)
            existing = existing_virtuals.get(marker)

            if existing:
                changed = False
                if existing.name != new_name:
                    existing.name = new_name
                    changed = True
                if existing.logo_id != resolved_logo_id:
                    existing.logo_id = resolved_logo_id
                    changed = True
                if existing.channel_number != target_chnum:
                    existing.channel_number = target_chnum
                    changed = True
                if changed and not dry_run:
                    existing.save(update_fields=["name", "logo", "channel_number"])
                if not dry_run:
                    # Compare by ORDERED list, not set: when our sort key
                    # changes (e.g. probe-aware quality re-ranks an existing
                    # set of streams) we need to rewrite the ChannelStream
                    # rows to flip their order, even if the membership is
                    # unchanged.
                    current_ordered = list(
                        ChannelStream.objects
                        .filter(channel=existing)
                        .order_by("order")
                        .values_list("stream_id", flat=True)
                    )
                    if current_ordered != source_streams:
                        ChannelStream.objects.filter(channel=existing).delete()
                        for order, sid in enumerate(source_streams):
                            ChannelStream.objects.create(
                                channel=existing, stream_id=sid, order=order,
                            )
                vc = existing
                updated += 1
            else:
                if dry_run:
                    created += 1
                    vc = None
                else:
                    vc = Channel.objects.create(
                        name=new_name,
                        channel_number=target_chnum,
                        channel_group=target_group,
                        tvg_id=marker,
                        logo_id=resolved_logo_id,
                        auto_created=False,
                    )
                    for order, sid in enumerate(source_streams):
                        ChannelStream.objects.create(
                            channel=vc, stream_id=sid, order=order,
                        )
                    created += 1

            # 4. EPG: get-or-create EPGData + replace ProgramData. Two
            # programs per channel:
            #   (a) pre-game filler: now → kickoff-30min, "Up next: ..."
            #       so the EPG always shows the description even when
            #       looking days ahead of kickoff.
            #   (b) game window: kickoff-30min → kickoff+4h, full title.
            # Skip (a) if kickoff is imminent (<5 min lead time) or past.
            if not dry_run and epg_source is not None and vc is not None:
                epg_data, _ = EPGData.objects.get_or_create(
                    epg_source=epg_source,
                    tvg_id=marker,
                    defaults={"name": new_name},
                )
                if epg_data.name != new_name:
                    epg_data.name = new_name
                    epg_data.save(update_fields=["name"])
                if vc.epg_data_id != epg_data.id:
                    # DO NOT use vc.save(update_fields=["epg_data"]):
                    # apps/channels/signals.py post_save fires
                    # parse_programs_for_tvg_id which unconditionally deletes
                    # ProgramData for the tvg_id (apps/epg/tasks.py:1308) before
                    # attempting an EPG-source refetch. Our EPGSource has no
                    # URL/file (we write programs directly), so the refetch
                    # fails and the rows stay deleted until the next plugin
                    # tick: wiping the EPG grid for 0–3 minutes per new
                    # channel attach. .update() bypasses the post_save signal,
                    # mirroring the pattern at apps/channels/signals.py:60.
                    Channel.objects.filter(pk=vc.pk).update(epg_data_id=epg_data.id)
                    vc.epg_data_id = epg_data.id  # keep in-memory mirror in sync
                ProgramData.objects.filter(epg=epg_data).delete()

                now = datetime.now(timezone.utc)
                pregame_lead = (prog_start - now).total_seconds()
                # ProgramData title shape mirrors how real broadcast EPGs
                # render: "Upcoming: Home vs Away, Fri 7:30 PM EDT" pregame,
                # "Home vs Away ᴸᶦᵛᵉ" during the window, "Past: Home vs Away"
                # after the game ends until the next refresh. The matchup
                # string deliberately drops the ★ score prefix the channel
                # name carries: the EPG entry should read like a program,
                # not a debug breadcrumb.
                matchup = _format_matchup(g["home"], g["away"])
                kickoff_local = g.get("kickoff_local", "")
                upnext_title = _build_program_title("upcoming", matchup, kickoff_local)
                live_title = _build_program_title("live", matchup, kickoff_local)
                past_title = _build_program_title("past", matchup, kickoff_local)
                past_end = _compute_past_slot_end(prog_end, settings)
                # Sub-title is the condensed informative one-liner: tagline,
                # matchday, toss-up.
                subtitle = _build_subtitle(g, tagline)
                if pregame_lead > 5 * 60:  # ≥ 5 min until kickoff window
                    ProgramData.objects.create(
                        epg=epg_data,
                        start_time=now,
                        end_time=prog_start,
                        title=upnext_title,
                        sub_title=subtitle,
                        description=description,
                        tvg_id=marker,
                    )
                ProgramData.objects.create(
                    epg=epg_data,
                    start_time=prog_start,
                    end_time=prog_end,
                    title=live_title,
                    sub_title=subtitle,
                    description=description,
                    tvg_id=marker,
                )
                # Past slot: bridges game-end to the next scheduled refresh
                # so the channel doesn't go blank in the EPG between the
                # final whistle and the apply that drops the channel. The
                # past slot's end_time is computed against the scheduler
                # config (with a 12h fallback when auto-refresh is off).
                ProgramData.objects.create(
                    epg=epg_data,
                    start_time=prog_end,
                    end_time=past_end,
                    title=past_title,
                    sub_title=subtitle,
                    description=description,
                    tvg_id=marker,
                )

        # 5. Reap stale virtual channels (not seen this refresh), preserving any
        # DVR recordings first (#146). Recording.channel is on_delete=CASCADE, so
        # a bare Channel.delete() would take the user's recordings with it. We
        # re-home completed recordings onto a persistent archive channel and skip
        # reaping any channel whose recording is still active (it reconciles next
        # cycle once the recording finishes).
        stale = [ch for marker, ch in existing_virtuals.items() if marker not in seen_markers]
        if stale and not dry_run:
            now_reap = datetime.now(timezone.utc)
            stale_ids_all = [c.id for c in stale]
            recs_by_channel: Dict[int, list] = {}
            for r in Recording.objects.filter(channel_id__in=stale_ids_all):
                recs_by_channel.setdefault(r.channel_id, []).append(r)
            reapable, kept_for_recording, rehome_rec_ids = _partition_stale_for_recordings(
                stale, recs_by_channel, now_reap, archive_enabled,
            )
            if rehome_rec_ids:
                archive_ch = _ensure_archive_channel(recordings_group_name)
                # .update() on the Recording queryset: re-point the FK without
                # invoking per-row save hooks. Recording carries no destructive
                # post_save signal (unlike Channel.epg_data), so this is a plain
                # bulk re-home.
                rehomed_recordings = Recording.objects.filter(
                    id__in=rehome_rec_ids,
                ).update(channel_id=archive_ch.id)
                logger.info(
                    "[ranked_matchups] re-homed %d recording(s) to archive %r before reaping",
                    rehomed_recordings, recordings_group_name,
                )
            reap_ids = [c.id for c in reapable]
            reap_markers = [c.tvg_id for c in reapable]
            if reap_ids:
                ChannelStream.objects.filter(channel_id__in=reap_ids).delete()
                Channel.objects.filter(id__in=reap_ids).delete()
                if epg_source is not None:
                    EPGData.objects.filter(
                        epg_source=epg_source, tvg_id__in=reap_markers,
                    ).delete()
            # Restore kept channels (active recording) to their pre-park number
            # so they don't linger at a ~park_base value. Only restore numbers
            # not claimed by a current game this run (assigned set), so the
            # unique (channel_group, channel_number) constraint can't be hit;
            # any (rare) collision leaves that channel parked, still safe.
            if kept_for_recording:
                assigned_now = set(chnum_by_marker.values())
                restore = []
                for ch in kept_for_recording:
                    orig = prepark_numbers.get(ch.id)
                    if orig is not None and orig not in assigned_now:
                        ch.channel_number = orig
                        restore.append(ch)
                if restore:
                    Channel.objects.bulk_update(restore, ["channel_number"])
            deleted_stale = len(reap_ids)
            kept_for_recording_n = len(kept_for_recording)
        elif stale and dry_run:
            deleted_stale = len(stale)

        # 6. Defensive: clean up orphan EPGData entries on our source whose
        # tvg_id has no corresponding virtual channel (left over from earlier
        # runs / migrations).
        orphan_epg_deleted = 0
        if not dry_run and epg_source is not None:
            kept_markers = seen_markers | {
                ch.tvg_id for ch in Channel.objects.filter(
                    _owned_tvg_id_q(), channel_group=target_group,
                )
            }
            orphans = EPGData.objects.filter(
                _owned_tvg_id_q(), epg_source=epg_source,
            ).exclude(tvg_id__in=kept_markers)
            orphan_epg_deleted, _ = orphans.delete()

        # 7. Archive hygiene (#146): the recordings group/channel exists only
        # while it holds recordings. Drop the archive channel and its group once
        # empty (e.g. after the user deletes the last preserved recording).
        if not dry_run and archive_enabled:
            _cleanup_empty_archive(recordings_group_name)

    # Persist the LLM-description cache (prune entries whose marker is no
    # longer in this refresh; keep file bounded to live games). Save outside
    # the atomic block: sidecar JSON file is independent of the DB.
    if llm_enabled and not dry_run:
        pruned = llm_descriptions.prune_cache(llm_cache, seen_markers)
        llm_descriptions.write_cache(LLM_DESCRIPTIONS_CACHE_PATH, pruned)

    # Persist the SportsDB thumb-URL cache and sweep stale matchup logo files
    # from /data/logos/. Both prune to the live marker set so disk usage
    # doesn't grow unbounded across many refresh cycles. Logo rows pointing
    # at deleted files are left for Dispatcharr's own cleanup_unused_logos
    # endpoint: deleting them here would race with concurrent UI reads.
    stale_logo_files_swept = 0
    if matchup_logos_enabled and not dry_run and thumb_cache is not None:
        thumb_cache.prune(seen_markers)
        thumb_cache.save()
        stale_logo_files_swept = matchup_logos.sweep_stale_logo_files(seen_markers)

    prefix = "[dry] " if dry_run else ""
    rename_msg = ""
    if migrated_from_old_group or deleted_old_groups or deleted_old_sources:
        rename_msg = (
            f" Migrated from old target: {migrated_from_old_group} channel(s) "
            f"moved (same Channel.id) from {deleted_old_groups} old group(s), "
            f"{deleted_old_sources} old EPGSource(s) deleted."
        )
    # `placeholders` is a *subset* of (created + updated): placeholder games
    # go through the same upsert path as matched ones, so they're already
    # counted there. Report as "(placeholders=N included)" to avoid the
    # "10 created + 3 placeholders == 13?" misread.
    llm_msg = ""
    if llm_enabled:
        llm_msg = f" LLM descriptions: {llm_used} written, {llm_failed} fell back to deterministic."
    logo_msg = ""
    if matchup_logos_enabled and not dry_run:
        logo_msg = (
            f" Matchup logos: {matchup_logos_used} matchup thumbnails, "
            f"{matchup_logos_badge} league/tournament badges, "
            f"{matchup_logos_fallback} fell back to source-channel logo, "
            f"{stale_logo_files_swept} stale file(s) swept."
        )
    rec_msg = ""
    if rehomed_recordings or kept_for_recording_n:
        rec_msg = (
            f" Recordings: {rehomed_recordings} re-homed to {recordings_group_name!r}, "
            f"{kept_for_recording_n} channel(s) kept (active recording)."
        )
    msg = (
        f"{prefix}Group {group_name!r}: created={created}, updated={updated} "
        f"(placeholders={placeholder_channels_created} included), "
        f"stale_deleted={deleted_stale}, "
        f"orphan_epg_deleted={orphan_epg_deleted if 'orphan_epg_deleted' in locals() else 0}, "
        f"unmatched_skipped={skipped_unmatched}.{rename_msg}{rec_msg}{llm_msg}{logo_msg} "
        f"WHY descriptions written to EPG source."
    )
    return {"status": "ok", "message": msg}


# ---------- auto pipeline ----------

def _action_auto_pipeline_sync(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Synchronous auto_pipeline body (refresh then apply).

    DO NOT call this inline in a uwsgi worker (neither from the HTTP
    `Plugin.run` dispatch NOR from the scheduler loop). The refresh step
    runs a pure-Python Monte Carlo that holds the GIL, and uwsgi runs
    under gevent on 0.26.0, so running it in-worker freezes the hub and
    hangs login + live streams (prod outage 2026-06-10). It is meant to
    execute inside `_pipeline_runner.py` (a subprocess with no gevent
    patch). Both entry points reach it the same way -- via
    `tasks.run_pipeline_subprocess("auto_pipeline", settings)`:
      - HTTP `Plugin.run("auto_pipeline")` -> `_action_auto_pipeline_async`
        -> daemon thread -> run_pipeline_subprocess (returns a queued
        envelope immediately so the browser doesn't time out, #84).
      - `_scheduler_loop` -> run_pipeline_subprocess.

    The CALLER (the daemon thread / scheduler, in the worker process) owns
    the inflight Redis key and the scheduler lock around the subprocess.
    This body only transitions the inflight phase between refresh and
    apply so both entry points get the same fine-grained show_status
    progress -- the child writes that phase update to the same Redis key."""
    r1 = _action_refresh(settings)
    if r1.get("status") != "ok":
        return r1
    tasks._update_inflight_phase("apply")
    r2 = _action_apply(settings)
    return {
        "status": r2.get("status", "ok"),
        "message": f"refresh: {r1.get('message')} | apply: {r2.get('message')}",
    }


def _action_auto_pipeline_async(settings: Dict[str, Any]) -> Dict[str, Any]:
    """HTTP-facing auto_pipeline. Spawns a daemon thread to run
    refresh + apply in the background and returns immediately with
    `{status: queued, task_id}`. The UI polls show_status to learn
    when cache.json mtime advances and apply completes."""
    task_id = tasks.run_auto_pipeline_background(settings)
    return {
        "status": "queued",
        "task_id": task_id,
        "message": (
            f"Auto pipeline queued (task {task_id}). "
            f"Run 'Show current state' to watch progress; "
            f"refresh + apply runs in the background."
        ),
    }


def _action_refresh_async(settings: Dict[str, Any]) -> Dict[str, Any]:
    """HTTP-facing refresh. Spawns a daemon thread and returns
    immediately. Refresh alone runs ~25s which is borderline-over the
    30s browser fetch default; queueing it keeps the UI honest."""
    task_id = tasks.run_refresh_background(settings)
    return {
        "status": "queued",
        "task_id": task_id,
        "message": (
            f"Refresh queued (task {task_id}). "
            f"Run 'Show current state' to watch progress; "
            f"cache.json mtime advances when complete."
        ),
    }


# ---------- preview / test naming convention ----------

def _action_preview_names(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Render the current name_template against canned sample games so a user
    can eyeball the layout before applying it to live channels (issue #100).

    On an empty or invalid template it previews the DEFAULT (clearly labeled)
    and lists the validation problems, so a broken template is caught here and
    never reaches the channel list.
    """
    from . import naming

    raw = str(settings.get("name_template") or "").strip()
    tz = _resolve_tz(settings.get("local_timezone", "UTC"))

    using_default = not raw
    template = raw or naming.DEFAULT_NAME_TEMPLATE
    errors = naming.validate_template(template)
    if errors and not using_default:
        template = naming.DEFAULT_NAME_TEMPLATE
        using_default = True

    _, rendered = naming.preview_lines(template, tz=tz)

    lines: List[str] = []
    if errors:
        lines.append("Template problems (the DEFAULT is previewed below):")
        lines += [f"  - {e}" for e in errors]
        lines.append("")
    lines.append(f"Previewing {'DEFAULT template' if using_default else 'your template'}:")
    lines.append(f"  {template}")
    lines.append("")
    for label, name in rendered:
        lines.append(f"  {name}")
        lines.append(f"      ({label})")
    lines.append("")
    lines.append("Variables (wrap optional ones in { } so the group vanishes when blank):")
    for token, desc in naming.TOKENS.items():
        lines.append(f"  {{{token}}}: {desc}")

    return {"status": "error" if errors else "ok", "message": "\n".join(lines)}


# ---------- show status ----------

def _action_show_status(settings: Dict[str, Any]) -> Dict[str, Any]:
    del settings  # interface-required (Plugin.run dispatch), not read here
    cache = _read_cache()
    inflight = tasks.read_inflight()
    inflight_line: Optional[str] = None
    if inflight:
        kind = inflight.get("kind", "?")
        phase = inflight.get("phase", "?")
        started_at = inflight.get("started_at", "?")
        tid = inflight.get("task_id", "")
        tid_short = tid[:8] if isinstance(tid, str) and len(tid) > 8 else tid
        inflight_line = (
            f"[in flight] {kind} ({phase}) since {started_at}"
            f"{f' task={tid_short}' if tid_short else ''}. "
            f"cache.json below reflects the LAST completed run; mtime "
            f"advances when refresh completes."
        )
    games = cache.get("games", [])
    if not games:
        msg = "Cache empty. Run refresh."
        if inflight_line:
            msg = inflight_line + "\n" + msg
        return {"status": "ok", "message": msg}
    lines = []
    if inflight_line:
        lines.append(inflight_line)
        lines.append("")
    lines += [
        f"Refreshed: {cache.get('refreshed_at')}",
        f"Sources: {' | '.join(cache.get('summary', []))}",
        f"Total games: {len(games)}",
        "",
        "Top games:",
    ]
    for i, g in enumerate(games[:25], 1):
        rh = g.get("rank_home")
        ra = g.get("rank_away")
        if rh is not None and ra is not None:
            lo, hi = sorted([rh, ra])
            rank_str = f"{lo}v{hi}"
        elif rh is not None or ra is not None:
            rank_str = f"{rh or ra}vUR"
        else:
            rank_str = "UR"
        chan_str = f"→ {g.get('channel_name_current') or '(unmatched)'}"
        bd = g.get("score_breakdown", {})
        bd_str = ", ".join(f"{k}={v}" for k, v in bd.items())
        score_disp = g.get("score", 0.0)
        kickoff = g.get("kickoff_local", "")
        today_marker = " 🔴" if g.get("is_today") else ""
        lines.append(
            f"  {i:2d}. {g['sport_prefix']} {rank_str} ★{score_disp:.1f}{today_marker}  "
            f"{g['away']} at {g['home']}  ({kickoff})  [{bd_str}]  {chan_str}"
        )
    return {"status": "ok", "message": "\n".join(lines)}


# ---------- diagnose matching ----------

# How many ProgramData rows to scan when looking for the game in its window.
# Bounds the query; the deep dive then surfaces at most one naming listing.
_DIAGNOSE_WINDOW_FETCH = 300

# A program is a "matchup listing" if its title contains a head-to-head
# separator. That is the shape of a game in ANY language ("Japan vs Netherlands",
# "Copa Mundial: Japon vs P. Bajos", "Hurricanes @ Golden Knights"), so it
# surfaces the game even when our team keywords miss its spelling. We filter the
# window dump to ONLY these, NOT to a sports vocabulary: matching sport words
# ("liga", "golf", "league") just floods the list with every sports channel and
# buries the one game we are hunting. Separators are spaced to stay specific.
# Deliberately NO " at ": it matches "at <time>" ("...Stage 1 at 10:00PM") in
# every schedule blurb. US "Team at Team" listings are caught by " @ " / "vs".
_MATCHUP_MARKERS = (" vs ", " vs. ", " v. ", " v ", " @ ")

# Title fragments that mark a PREVIEW / upcoming / non-broadcast card rather than
# the live game. Such a card is never the channel actually carrying the match, so
# the window dump drops it (otherwise a provider's "Next Event/Next Game" promos
# flood the list). Superset of the matcher's own preview patterns.
_DIAGNOSE_SKIP = (
    "next ", "coming up", "upcoming", "preview", "pregame", "pre-game",
    "pre game", "post-game", "postgame", "highlight", "press conference", "weigh",
)


def _named_sides(text: str, home_kws: List[str], away_kws: List[str]) -> Tuple[bool, bool]:
    """Whether `text` (a channel name or program title) names the home / away
    side. Delegates to matcher._kw_hit, the SAME test the matcher tiers use, so
    the diagnostic reports exactly what the matcher saw, not a parallel
    heuristic that could drift."""
    from .matcher import _kw_hit
    return _kw_hit(text, home_kws), _kw_hit(text, away_kws)


def _diagnose_window_sample(start_dt, sport_prefix, home, away, home_kws, away_kws,
                            field=False):
    """Return rows of candidate programming airing in the chosen game's window,
    so a human can spot the game sitting under a spelling our keywords miss.

    Two-team games: only programs whose title looks like a head-to-head
    ("A vs B", "A @ B") are returned (see _MATCHUP_MARKERS): that is what a game
    listing is in any language, and it keeps the list to actual fixtures instead
    of every sports channel in the window. annotation flags whether the row
    names BOTH teams, one team, or neither.

    Field events (#127): there is no opponent, so the head-to-head separators
    don't apply (golf/F1/NASCAR titles have none). Instead we surface programs
    whose title names the event itself (home keywords), and annotate " [event]"
    when the channel/title names it. `away_kws` is empty in this mode.

    rows: list of (channel_name, program_title, annotation), one per channel,
    ordered with naming channels first.
    """
    from apps.epg.models import ProgramData
    from apps.channels.models import Channel
    from django.db.models import Q

    pre_min, post_hours = _epg_match_window(sport_prefix)
    win_start = start_dt - timedelta(minutes=pre_min)
    win_end = start_dt + timedelta(hours=post_hours)

    title_q = Q()
    if field:
        # Find programs that name the event; there is no h2h separator to key on.
        for kw in home_kws:
            title_q |= Q(title__icontains=kw)
    else:
        for t in _MATCHUP_MARKERS:
            title_q |= Q(title__icontains=t)

    progs = list(
        ProgramData.objects
        .filter(start_time__lt=win_end, end_time__gt=win_start)
        .filter(title_q)
        .only("id", "title", "epg_id", "start_time")
        .order_by("start_time")[:_DIAGNOSE_WINDOW_FETCH]
    )

    epg_ids = {p.epg_id for p in progs if p.epg_id}
    chan_by_epg: Dict[Any, list] = {}
    for c in (Channel.objects.filter(epg_data_id__in=epg_ids)
              .exclude(_owned_tvg_id_q()).only("id", "name", "epg_data_id")):
        chan_by_epg.setdefault(c.epg_data_id, []).append(c)

    rows = []
    seen_chan = set()
    for p in progs:
        title = p.title or ""
        low = title.lower()
        if any(s in low for s in _DIAGNOSE_SKIP):   # preview/upcoming card, not live
            continue
        for c in chan_by_epg.get(p.epg_id, []):
            if c.id in seen_chan:
                continue
            seen_chan.add(c.id)
            nh, na = _named_sides(c.name, home_kws, away_kws)
            th, ta = _named_sides(title, home_kws, away_kws)
            h, a = (nh or th), (na or ta)
            if field:
                # Single-sided: there is only the event name to find.
                ann, rank = (" [event]", 1) if h else ("", 2)
            elif h and a:
                ann, rank = " [BOTH TEAMS]", 0
            elif h:
                ann, rank = f" [{home}]", 1
            elif a:
                ann, rank = f" [{away}]", 1
            else:
                ann, rank = "", 2
            rows.append((rank, c.name, title, ann))
    # Team-naming channels first (the actionable ones), then the rest in the
    # order found (already chronological from the query).
    rows.sort(key=lambda r: r[0])
    return [(name, title, ann) for _, name, title, ann in rows]


def _action_diagnose(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Diagnose why games aren't matching, in two views (#128).

    Picks the SOONEST unmatched game (the one with EPG to examine and the one a
    user is likely asking about) and checks the matchup listings airing in its
    window for the teams.

    Returns a SHORT toast (<=3 lines: the game, one naming listing if any, a
    verdict) because the UI shows the result as a single bottom-anchored toast
    that clips long messages. ALSO logs a VERBOSE block (full window listings +
    every unmatched game + the matched set) to docker logs, so a user can paste
    the toast and hand over the logs for deeper help.

    Read-only: no DB writes, no apply; reflects the EPG as it stands now.
    """
    del settings  # interface-required (Plugin.run dispatch), not read here
    from types import SimpleNamespace
    from .matcher import _team_keywords, _regex_filter_channel_name

    cache = _read_cache()
    games = cache.get("games", [])
    if not games:
        return {"status": "ok", "message": "Cache empty. Run refresh first, then re-run this."}

    matched = [g for g in games if g.get("channel_name_current")]
    unmatched = [g for g in games if not g.get("channel_name_current")]

    def _label(g: Dict[str, Any]) -> str:
        # Field events have no opponent, so render the event name alone instead
        # of the misleading "Field at <event>".
        if is_field_event(g.get("away"), g.get("extra")):
            return f"{g.get('sport_prefix', '?')} {g.get('home', '')} ({g.get('kickoff_local', '')})"
        return (f"{g.get('sport_prefix', '?')} {g.get('away', '')} at "
                f"{g.get('home', '')} ({g.get('kickoff_local', '')})")

    # The result renders as a Mantine toast (Plugins.md), NOT a scrollable modal:
    # it anchors bottom-right and grows UPWARD, so a tall message overflows off
    # the top of the screen with no way to scroll up. Keep the message SHORT and
    # put the verdict LAST, where the bottom-anchored toast stays visible.
    if not unmatched:
        return {"status": "ok",
                "message": f"Ranked Matchups: {len(games)} games, all matched. Nothing to diagnose."}

    # Deep-dive the SOONEST-starting unmatched game. Soonest, not
    # highest-scored: a game is only diagnosable once its guide exists, so a
    # far-future game would trivially show an empty window. Soonest is also the
    # game a user is most likely asking about, regardless of its
    # interestingness score. Field events (#127) are matched single-sided and
    # are ordinary diagnosable targets, not skipped.
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    diggable = []
    for g in unmatched:
        dt = parse_iso_utc(g.get("start_time_utc"))
        if dt is not None:
            diggable.append((dt, g))
    upcoming = sorted((x for x in diggable if x[0] >= now - timedelta(hours=3)),
                      key=lambda x: x[0])
    if upcoming:                       # soonest game still upcoming / just-started
        target = upcoming[0][1]
    elif diggable:                     # everything already passed: most recent
        target = max(diggable, key=lambda x: x[0])[1]
    else:
        target = None

    # Gather the facts once, then emit TWO views: a SHORT toast (the return
    # value, shown as one bottom-anchored notification) and a VERBOSE block to
    # the logs (docker logs Dispatcharr), so a user can paste the toast AND hand
    # over the full detail. See #128 and the toast-length note in the skill.
    def _trunc(s, n):
        s = s or ""
        return s if len(s) <= n else s[:n - 1] + "…"

    home = away = ""
    home_kws = away_kws = []
    rows: List[Any] = []
    stream_hits: List[str] = []  # Path C: stream NAMES that name the team(s)
    verdict = ""
    toast: List[str] = []
    field = False

    if target is None:
        verdict = "Could not pick a game to diagnose (no kickoff time in cache); send us this."
        toast = [verdict]
    else:
        home, away = target.get("home", ""), target.get("away", "")
        # Field events (#127) match single-sided on the event name; there is no
        # away side to search for, so away_kws is empty and the both-teams
        # bookkeeping below collapses to a single "names the event" signal.
        field = is_field_event(away, target.get("extra"))
        home_kws = _team_keywords(home)
        away_kws = [] if field else _team_keywords(away)
        start_dt = parse_iso_utc(target.get("start_time_utc"))
        head = f"Closest of {len(unmatched)} unmatched: {_label(target)}"
        if start_dt is None:
            verdict = "Can't diagnose this one: bad start time in cache."
            toast = [head, f"=> {verdict}"]
        else:
            try:
                rows = _diagnose_window_sample(
                    start_dt, target.get("sport_prefix"), home, away,
                    home_kws, away_kws, field=field)
            except Exception:  # diagnostic must never raise
                rows = []
            # Single most relevant listing that names a team (both > one); an
            # unrelated head-to-head airing at the same time is not evidence.
            both = next((r for r in rows if r[2] == " [BOTH TEAMS]"), None)
            one = next((r for r in rows if r[2] and r[2] != " [BOTH TEAMS]"), None)
            shim = SimpleNamespace(sport_prefix=target.get("sport_prefix"),
                                   start_time=start_dt, home=home, away=away,
                                   extra=target.get("extra") or {})
            try:
                cands = _build_epg_lookup()(shim)
            except Exception:
                cands = []
            # Path C: stream NAMES (not channel names) that name the team(s).
            # These carry stream_id; surface them so a user can see the dedicated
            # per-match feeds the matcher now keys on.
            stream_hits = [c.program_title for c in cands if c.stream_id is not None]
            # Single-sided channel-name check for field events (team_b=None).
            name_match = _regex_filter_channel_name(
                cands, home, None if field else away) if cands else []
            chan_name_match = [c for c in name_match if c.stream_id is None]
            stream_name_match = [c for c in name_match if c.stream_id is not None]
            if chan_name_match:
                verdict = (("A channel name has the event but it didn't match - "
                            if field else
                            "A channel name has both teams but it didn't match - ")
                           + "unexpected; please send us this.")
            elif stream_name_match:
                verdict = (("A stream name has the event but it didn't match - "
                            if field else
                            "A stream name has both teams but it didn't match - ")
                           + "unexpected; please send us this.")
            elif both:
                verdict = ("A listing names BOTH teams but wasn't auto-picked - reply "
                           "and we'll fix it.")
            elif one:
                verdict = ("Likely a name/spelling gap: if 'Saw' above is this game, "
                           "reply with that exact title.")
            elif field:
                verdict = ("Nothing in your guide names this event - likely not carried "
                           "(often a future event), or named differently; reply if you spot it.")
            else:
                verdict = ("Nothing in your guide names either team - likely not carried "
                           "(often a future game), or named differently; reply if you spot it.")
            toast = [head]
            if both or one:
                name, title, ann = both or one
                toast.append(f"Saw {_trunc(name, 22)}: \"{_trunc(title, 48)}\"{ann}")
            toast.append(f"=> {verdict}")

    # Verbose companion -> logs: full window listings, every unmatched game, and
    # the matched set, none of which fits the toast. Grep "diagnose (verbose)".
    vlog = [
        "===== Ranked Matchups diagnose (verbose) =====",
        f"{len(games)} games | {len(matched)} matched | {len(unmatched)} unmatched",
    ]
    if target is not None:
        vlog.append(f"deep dive (soonest unmatched): {_label(target)}")
        if field:
            vlog.append(f"  searched - event {home}: {', '.join(home_kws)} "
                        "(field event, no opponent)")
        else:
            vlog.append(f"  searched - {away}: {', '.join(away_kws)} | "
                        f"{home}: {', '.join(home_kws)}")
        if rows:
            vlog.append(f"  head-to-head listings in its window ({len(rows)}):")
            for name, title, ann in rows[:40]:
                vlog.append(f"    {name}: \"{title}\"{ann}")
            if len(rows) > 40:
                vlog.append(f"    +{len(rows) - 40} more")
        else:
            vlog.append("  head-to-head listings in its window: none")
        # Path C: streams whose NAME names the team(s). A non-empty list here on
        # an UNMATCHED game indicates a bug (these should match via Path C), so
        # it is the actionable signal to send us.
        if stream_hits:
            vlog.append(f"  streams naming the team(s) ({len(stream_hits)}):")
            for nm in stream_hits[:40]:
                vlog.append(f"    \"{nm}\"")
            if len(stream_hits) > 40:
                vlog.append(f"    +{len(stream_hits) - 40} more")
        else:
            vlog.append("  streams naming the team(s): none")
    vlog.append(f"  verdict: {verdict}")
    vlog.append(f"all unmatched ({len(unmatched)}):")
    for g in unmatched:
        tag = " (field event)" if is_field_event(g.get("away"), g.get("extra")) else ""
        vlog.append(f"  {_label(g)}{tag}")
    if matched:
        vlog.append(f"matched ({len(matched)}):")
        for g in matched:
            vlog.append(f"  {_label(g)} -> {g.get('channel_name_current')}")
    vlog.append("===== end diagnose =====")
    logger.info("[ranked_matchups] diagnose (verbose)\n%s", "\n".join(vlog))

    return {"status": "ok", "message": "\n".join(toast)}


# ---------- scheduler ----------

# Scheduler state (the running thread + its stop Event) lives in a registry
# stashed in sys.modules under a synthetic key, NOT in module globals, so it
# SURVIVES a plugin reload. #110: the loader's force_reload path (reload
# endpoint / .reload_token / settings-save) unloads this module by file path and
# re-imports it WITHOUT calling stop() — it only calls stop() on disable/delete.
# Module globals reset on reload, so the new incarnation could never see the
# thread the old one started, orphaning it (and its DB connection) every reload.
# That re-leaks the exact thread #82's stop() was meant to reclaim. Keeping the
# state in a reload-stable registry lets the new incarnation's __init__ find and
# tear down the prior thread before starting its own. The synthetic module has
# no __file__/__path__ under the plugin dir, so the loader's _unload_path_modules
# leaves it in place across reloads.
_SCHEDULER_REGISTRY_KEY = "_dispatcharr_ranked_matchups_scheduler_state"


def _scheduler_registry():
    reg = sys.modules.get(_SCHEDULER_REGISTRY_KEY)
    if reg is None:
        reg = types.ModuleType(_SCHEDULER_REGISTRY_KEY)
        reg.thread = None
        reg.stop_event = None
        sys.modules[_SCHEDULER_REGISTRY_KEY] = reg
    return reg


def _stop_scheduler(thread, stop_event, reason: str) -> None:
    """Signal a scheduler thread to exit and briefly block to confirm it did.

    The loop polls its own stop_event between work units (and on every park via
    _scheduler_sleep), so setting it drops the loop out at its next wake-up
    (immediate: set() wakes a thread waiting on the Event); the loop's finally
    then closes its DB connection. Only stop() (disable/delete) calls this now:
    __init__ is idempotent and leaves a healthy thread running rather than
    restarting it (#82/#136), so there is no longer a reload path that needed the
    old non-blocking (join=False) variant.
    """
    if stop_event is not None:
        stop_event.set()
    if thread is not None and thread.is_alive():
        thread.join(timeout=5.0)
        if thread.is_alive():
            logger.warning(
                "[ranked_matchups] scheduler thread did not exit within 5s of "
                "%s; it will linger until process exit", reason,
            )


def _parse_scheduled_times(raw: str) -> List[Tuple[int, int]]:
    """Parse 'HHMM,HHMM,...' into [(hour, minute), ...], sorted, deduped.
    Tolerates whitespace / colons / 'HH:MM'. Bad entries are skipped."""
    out: List[Tuple[int, int]] = []
    if not raw:
        return out
    for tok in raw.replace(";", ",").split(","):
        s = tok.strip().replace(":", "")
        if not s:
            continue
        if len(s) == 3:
            s = "0" + s
        if len(s) != 4 or not s.isdigit():
            logger.warning("[ranked_matchups] bad scheduled_times token: %r", tok)
            continue
        h, m = int(s[:2]), int(s[2:])
        if 0 <= h < 24 and 0 <= m < 60:
            out.append((h, m))
        else:
            logger.warning("[ranked_matchups] out-of-range scheduled_times token: %r", tok)
    return sorted(set(out))


def _next_fire_time(times: List[Tuple[int, int]], tz, now: Optional[datetime] = None) -> Optional[datetime]:
    """Return the next occurrence of any time in `times` in the given tz."""
    if not times:
        return None
    if now is None:
        now = datetime.now(tz)
    today = now.date()
    candidates = [datetime.combine(today, datetime.min.time(), tz).replace(hour=h, minute=m)
                  for h, m in times]
    candidates += [datetime.combine(today + timedelta(days=1), datetime.min.time(), tz).replace(hour=h, minute=m)
                   for h, m in times]
    future = [c for c in candidates if c > now]
    return min(future) if future else None


def _scheduler_close_db():
    """Release this scheduler greenlet's Django DB connection.

    Django opens a connection lazily on first query and, in a NON-request thread,
    NEVER closes it automatically (the request/response cycle is what normally
    closes connections). The scheduler reads settings from the DB each tick, so
    without this it pins one Postgres backend open the entire time it sleeps; and
    because the loader spawns a fresh scheduler per discovery, orphaned ones
    accumulate one leaked connection apiece until `max_connections` is hit and
    every request blocks on connection acquisition. That is the #82 lock-up,
    rediscovered in #136. Closing before every park keeps a sleeping scheduler at
    ZERO held connections; the next tick reopens transparently. DO NOT swap the
    _scheduler_sleep calls below for a bare `stop_event.wait`: that reintroduces
    the leak.
    """
    try:
        from django.db import connection
        connection.close()
    except Exception:
        pass


def _scheduler_sleep(stop_event, timeout):
    """Close the DB connection (see _scheduler_close_db), THEN park on the stop
    event. Every scheduler sleep MUST go through here so a parked scheduler holds
    no connection. Returns True if the stop event fired (caller should exit)."""
    _scheduler_close_db()
    return stop_event.wait(timeout=timeout)


def _scheduler_loop(plugin_ref, stop_event):
    """Auto-refresh + apply at every time listed in scheduled_times.

    stop_event is THIS thread's own Event, held in the reload-stable registry
    (#110), not a module global: a later reload can stop this exact thread even
    after the module that started it has been unloaded and its globals reset.

    Holds NO DB connection while parked: every sleep goes through
    _scheduler_sleep (which closes first), and the finally closes on exit so a
    reclaimed (reload-orphaned or stopped) greenlet releases its backend instead
    of leaking it (#82/#136).
    """
    try:
        while not stop_event.is_set():
            try:
                settings = plugin_ref.get_current_settings()
                if not settings.get("auto_refresh_enabled", False):
                    _scheduler_sleep(stop_event, 300)
                    continue
                tz = _resolve_tz(settings.get("local_timezone", "UTC"))
                times = _parse_scheduled_times(settings.get("scheduled_times", "0400"))
                if not times:
                    logger.warning("[ranked_matchups] no valid scheduled_times; idling 5m")
                    _scheduler_sleep(stop_event, 300)
                    continue
                target = _next_fire_time(times, tz)
                if target is None:
                    _scheduler_sleep(stop_event, 300)
                    continue
                sleep_s = (target - datetime.now(tz)).total_seconds()
                logger.info(
                    "[ranked_matchups] scheduler sleeping %.0fs until %s (next of %s)",
                    sleep_s, target.isoformat(), times,
                )
                if _scheduler_sleep(stop_event, sleep_s):
                    return
                logger.info("[ranked_matchups] scheduler firing auto_pipeline")
                # Delegates to the shared lock/inflight/subprocess path so the
                # scheduler runs the work OUT OF PROCESS (the Monte Carlo scoring
                # holds the GIL and would freeze this gevent worker's hub if run
                # inline) and can't drift out of sync with the HTTP launchers.
                # run_scheduled_pipeline acquires the cross-worker lock itself and
                # no-ops if another worker holds it. See tasks.py header.
                tasks.run_scheduled_pipeline(settings)
            except Exception:
                logger.exception("[ranked_matchups] scheduler loop crashed; sleeping 10m")
                _scheduler_sleep(stop_event, 600)
    finally:
        _scheduler_close_db()


_SCHEDULER_LOCK_KEY = f"plugins:{PLUGIN_KEY}:scheduler:lock"


def _try_acquire_scheduler_lock() -> bool:
    """Cross-worker mutex via Redis. ttl 30 min so a crashed run releases."""
    try:
        from core.utils import RedisClient
        r = RedisClient.get_client()
        return bool(r.set(_SCHEDULER_LOCK_KEY, "1", nx=True, ex=1800))
    except Exception as e:
        logger.warning("[ranked_matchups] redis lock failed (%s); proceeding without lock", e)
        return True


def _release_scheduler_lock() -> None:
    try:
        from core.utils import RedisClient
        RedisClient.get_client().delete(_SCHEDULER_LOCK_KEY)
    except Exception:
        pass


# ---------- Plugin entry ----------

# Single source of truth mapping every action id to its handler. run() dispatches
# through this, and plugin.json's "actions" must declare exactly these ids: the
# contract test (tests/test_diagnose.py) asserts the two sets match, so a manifest
# button with no handler (or a handler with no button) fails CI instead of
# silently returning "Unknown action" at runtime. Every handler has the uniform
# (settings) -> dict shape. refresh/auto_pipeline map to their ASYNC entrypoints
# (the HTTP-facing wrappers); see #84 and the run() docstring.
_ACTION_HANDLERS = {
    "refresh": _action_refresh_async,
    "apply": _action_apply,
    "auto_pipeline": _action_auto_pipeline_async,
    "show_status": _action_show_status,
    "preview_names": _action_preview_names,
    "diagnose": _action_diagnose,
}


class Plugin:
    name = "Ranked Matchups (Top Games)"
    # Single source of truth for the displayed version: the loader uses this
    # class attr over plugin.json's "version". Keep all three in sync
    # (this attr, plugin.json, __init__.py __version__).
    version = "1.9.0"

    def __init__(self):
        # The scheduler reads settings live from the DB on each tick rather than
        # relying on stale init-time settings.
        reg = _scheduler_registry()
        # IDEMPOTENT across the loader's per-discovery re-instantiation (#82/#136).
        # The loader re-execs plugin.py and rebuilds this Plugin on EVERY discovery
        # (discover_plugins defaults to use_cache=False). Two paths, both verified
        # live (2026-06-15):
        #   - Routine discovery (plugins-list view / run): does NOT call stop(),
        #     so the prior incarnation's scheduler is still in the reload-stable
        #     registry (#110) with is_alive()==True. We MUST leave it running:
        #     stopping+starting on every UI poll churned a thread and (pre-fix)
        #     leaked a DB connection per poll until max_connections was hit (the
        #     #82 lock-up). This early return is what prevents that churn.
        #   - The reload endpoint: the loader calls stop() FIRST (loader.py), which
        #     clears reg.thread, so we fall through and start a fresh one; the old
        #     thread was already signaled and releases its DB connection in
        #     _scheduler_loop's finally, and the new one parks holding none.
        # Either way no connection leaks. Code updates ship via container restart
        # (fresh process -> fresh thread), so a kept thread never runs stale code.
        if reg.thread is not None and reg.thread.is_alive():
            return
        stop_event = threading.Event()
        t = threading.Thread(target=_scheduler_loop, args=(self, stop_event), daemon=True,
                             name="ranked_matchups-scheduler")
        t.start()
        reg.thread = t
        reg.stop_event = stop_event
        logger.info("[ranked_matchups] scheduler thread started (pid=%s)", os.getpid())

    def get_current_settings(self) -> Dict[str, Any]:
        try:
            from apps.plugins.models import PluginConfig
            pc = PluginConfig.objects.filter(key=PLUGIN_KEY).first()
            if pc and pc.settings:
                return dict(pc.settings)
        except Exception as e:
            logger.warning("[ranked_matchups] could not read settings from DB: %s", e)
        return {}

    def stop(self, context: Optional[Dict[str, Any]] = None) -> None:
        """Tear down the scheduler thread when the loader disables / deletes the
        plugin. The loader calls stop() on disable/delete (NOT on reload, see
        #110), so the reload-orphan reclaim lives in __init__; this path handles
        the explicit-teardown case. Without it the thread keeps polling Postgres
        on a 5-min loop, leaking a DB connection per worker until max_connections
        is hit and every API request 500s with "too many clients" (#82).

        State is read from the reload-stable registry (#110), not module globals,
        so stop() reclaims the live thread even if it was started by a different
        module incarnation than the one this instance belongs to.
        """
        del context  # unused; conform to loader's stop(context) shape
        reg = _scheduler_registry()
        _stop_scheduler(reg.thread, reg.stop_event, "stop signal")
        reg.thread = None
        reg.stop_event = None

    def run(self, action: Optional[str] = None,
            params: Optional[Dict[str, Any]] = None,
            context: Optional[Dict[str, Any]] = None):
        ctx = context or {}
        settings = dict(ctx.get("settings") or {})
        if params:
            settings.update(params)
        try:
            # _ACTION_HANDLERS is the single dispatch table. refresh + auto_pipeline
            # map to ASYNC entrypoints because they run too long for browser / axios
            # default fetch timeouts (~30s): they queue and return a task id, and the
            # UI polls show_status for progress. See #84 and tasks.py. apply stays
            # synchronous (~17s, fits).
            handler = _ACTION_HANDLERS.get(action)
            if handler is None:
                return {"status": "error", "message": f"Unknown action: {action!r}"}
            return handler(settings)
        except Exception as e:
            logger.exception("[ranked_matchups] action %r failed", action)
            return {"status": "error", "message": f"{type(e).__name__}: {e}"}
