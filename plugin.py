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
import threading
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # py < 3.9 fallback (won't hit on Dispatcharr's Python 3.13)
    ZoneInfo = None  # type: ignore

from ._util import parse_iso_utc, stable_hash_int

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
# our virtual channel still uses the defaults above — those control
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
# EPGSource — a TVG_ID_PREFIX-only check misses sources whose only remaining
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

# Default starting channel number when the user hasn't configured one. Sentinel
# 0 means "auto" — pick the first channel number after the highest existing
# non-virtual channel, so we slot in cleanly without colliding with real
# channels.
DEFAULT_VIRTUAL_CHANNEL_BASE = 0

# Default fallback when DEFAULT_VIRTUAL_CHANNEL_BASE is sentinel-0 AND there
# are zero existing channels (fresh install) — picked high enough not to
# collide with auto-channel-sync ranges.
_AUTO_BASE_FALLBACK = 9000

# EPGSource fields. DO NOT use source_type="dummy" — Dispatcharr's
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


def _stream_sort_key(stream_stats, name):
    """Composite sort key for a stream. Lower tuple = sorted earlier.

    Tiers (most authoritative first):
      0. Valid ffprobe data: real height ≥ 240 and width ≥ 320.
         Sub-sort by -height (1080p before 720p) then -bitrate.
      1. No probe data at all (Dispatcharr never crawled this stream).
         Sub-sort by name-keyword bucket (UHD > FHD > HD > unknown > SD).
      2. Probe ran and got 0x0 — typically a dead/broken stream. Sort last
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
    """Team-pair string for EPG program titles. Plain `Home vs Away` —
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
    """End time for the post-event EPG slot — runs until the next
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
# - "balanced": ~25 games with default weights — mirrors the v0 default.
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
        # Mirrors Weights() dataclass defaults — kept here for the SAME
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
    # declaration. Do NOT duplicate the numbers here — when a default
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


def _build_sources(settings: Dict[str, Any]):
    from .sources import (
        GroupStageSoccerSource, KnockoutSoccerSource, MlbPlayoffSource, MlbRegularSource,
        MlsSource, NwslSource, LigaMxSource,
        NbaPlayoffSource, NbaRegularSource,
        WnbaPlayoffSource, WnbaRegularSource,
        NcaawBasketballPlayoffSource, NcaawBasketballRegularSource,
        NcaaBaseballRegularSource, NcaaBaseballPlayoffSource, NcaaBaseballPlayoffBracketSource,
        NcaaSoftballRegularSource, NcaaSoftballPlayoffSource, NcaaSoftballPlayoffBracketSource,
        NcaaSoccerSource,
        NcaafSource, NcaamSource,
        NflPlayoffSource, NflRegularSource,
        NhlPlayoffSource, NhlRegularSource, SoccerSource,
        F1Source, NascarSource, GolfSource, UfcSource,
        AtpSource, WtaSource,
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
        KnockoutSoccerSource — a different state machine for bracket shape.
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
        sources.append(_make_soccer("world_cup"))
        sources.append(GroupStageSoccerSource(
            "world_cup", fd_api_key=fd_key, odds_api_key=odds_key,
        ))
    if settings.get("enable_euros", False) and fd_key:
        sources.append(_make_soccer("euros"))
        sources.append(GroupStageSoccerSource(
            "euros", fd_api_key=fd_key, odds_api_key=odds_key,
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

    # NFL — no API key required (ESPN unofficial). Same pair-and-seed
    # pattern as NHL/MLB/NBA. Bracket is single-game elimination
    # (SERIES_LENGTH=1 per stage) across 4 rounds: WC -> DIV -> CONF
    # -> SB. Strength sharing matters most here because NFL teams
    # play only 17 regular-season games — even fewer baseline games
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

    # NHL — no API key required (api-web.nhle.com is free). Pair the
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
            # source — it can still run on the default prior.
            logger.warning("[nhl] could not seed playoff strengths: %s", exc)
        sources.append(nhl_po)

    # MLB — no API key required (statsapi.mlb.com is free). Same pair-and-
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

    # NBA — no API key required. ESPN unofficial API is used (stats.nba.com
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

    # MLS — ESPN schedule + Odds API closeness. V1 surfaces games with
    # favorite + closeness signals only; no standings-based importance
    # because conference standings bands and the MLS Cup mixed-format
    # bracket (best-of-3 R1 + single-leg subsequent rounds) are both
    # follow-ups. Closeness still requires the Odds API key — without
    # it MLS games surface but with no closeness signal.
    if settings.get("enable_mls", False):
        sources.append(MlsSource(odds_api_key=odds_key or ""))

    # NWSL — same V1 minimal pattern as MLS (schedule + closeness).
    # Subclasses MlsSource with NWSL-specific endpoint and Odds API
    # key. No importance / playoff bracket in V1.
    if settings.get("enable_nwsl", False):
        sources.append(NwslSource(odds_api_key=odds_key or ""))

    # Liga MX — Mexican top-flight. Same V1 minimal pattern.
    if settings.get("enable_liga_mx", False):
        sources.append(LigaMxSource(odds_api_key=odds_key or ""))

    # Field events (racing + golf). No two-team head-to-head;
    # each row is one race or tournament. Low event volume (~1/week)
    # means "surface if toggled" is the right product — no importance
    # ranking needed.
    if settings.get("enable_f1", False):
        sources.append(F1Source())
    if settings.get("enable_nascar", False):
        sources.append(NascarSource())
    if settings.get("enable_golf", False):
        sources.append(GolfSource())

    # UFC. Same field-event shape — each fight card is one
    # row with home=card title ("UFC 309: Jones vs. Miocic"). PPVs
    # (numbered UFC events) get MAJOR tier, Fight Nights get EVENT.
    if settings.get("enable_ufc", False):
        sources.append(UfcSource())

    # Tennis. ESPN's tennis scoreboard returns whole
    # tournaments (one entry per active event), not individual
    # matches — so tennis fits the FieldEventSource model. Grand
    # Slams + year-end Finals get MAJOR; regular tour stops get
    # EVENT.
    if settings.get("enable_atp", False):
        sources.append(AtpSource())
    if settings.get("enable_wta", False):
        sources.append(WtaSource())

    # WNBA — ESPN unofficial API; same pair-and-seed pattern as NHL/MLB/
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

    # NCAA Women's Basketball + March Madness — no API key required.
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
    # baseball above — NcaaSoftballPlayoffSource owns the best-of-3
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
    # parametrized on gender — same structure / endpoints / threshold
    # semantics for both, only the ESPN URL slug differs. Standings
    # points (3 W / 1 D / 0 L) drive the importance signal because
    # draws are common in college soccer.
    if settings.get("enable_ncaa_mens_soccer", False):
        sources.append(NcaaSoccerSource(gender="m"))
    if settings.get("enable_ncaa_womens_soccer", False):
        sources.append(NcaaSoccerSource(gender="w"))
    return sources


# ---------- refresh ----------

def _action_refresh(settings: Dict[str, Any]) -> Dict[str, Any]:
    from .scoring import GameSignals, score_game
    from .matcher import match_games_to_channels

    favorites = _parse_favorites(settings.get("favorites", ""))
    weights = _build_weights(settings)
    lookahead = int(settings.get("lookahead_days", 7))
    max_games = _resolve_max_games(settings)

    sources = _build_sources(settings)
    if not sources:
        return {"status": "error", "message": "No sport sources enabled."}

    # 1. Fetch. Keep the (source, game) association — compute_match_importance
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
        msg = "No games found in lookahead window. " + " | ".join(src_summary)
        logger.info("[ranked_matchups] %s", msg)
        cache = {"games": [], "refreshed_at": datetime.now(timezone.utc).isoformat(),
                 "summary": src_summary}
        _write_cache(cache)
        return {"status": "ok", "message": msg}

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
        # initial_state, not this table — they're built from the same
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

    # Sort: today's games first (0 before 1), then 0-10 score desc, then raw as
    # tiebreak, then start_time ascending. So a game today with ★7 outranks a
    # game next week with ★9.5.
    tz_local = _resolve_tz(settings.get("local_timezone", "UTC"))

    def _sort_key(item):
        # Favorites-first within today's bucket: even a lukewarm Tottenham
        # game should beat a 9.5-rated title-race contender for THIS user.
        # The favorite-weight bump alone wasn't enough — Man City still
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
    # _build_epg_lookup excludes ALL our virtual channels by tvg_id prefix —
    # covers both the current target group and any orphans from a renamed group.
    epg_lookup = _build_epg_lookup()
    api_key = _resolve_key(settings, "anthropic_api_key", ANTHROPIC_KEY_PATH)
    model = settings.get("model", "claude-haiku-4-5")
    matches = match_games_to_channels(scored, epg_lookup, api_key, model)

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
            "channel_ids": list(match.channel_ids),   # all matched, primary first
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
            f"Refreshed: {len(games_payload)} games scored, {matched} matched to channels. "
            + " | ".join(src_summary)
        ),
    }


# ---------- EPG lookup (closure over Django ORM) ----------

def _build_epg_lookup():
    """Return a callable: GameRow -> List[ChannelCandidate]. Closure over ORM.

    Excludes any channel that is one of OUR virtual channels (see
    _owned_tvg_id_q) — covers the configured target group AND any old groups
    left over from a prior target_group_name. Without this, the matcher
    self-matches against our prior-run channels because their EPG titles
    literally contain the team names.

    Pre-filters at the DB level: only fetches programs that are TEAM-relevant
    (program title contains a team keyword OR program's channel has a team
    keyword in its name). Prime-time windows can carry 4000+ programs across
    a Dispatcharr instance — fetching all of them and filtering in Python had
    a 2000-row hard cap that silently dropped real matches (regression
    against the old uncapped path was ch_id=111919 'EPL 07ⓧ: ... vs Brentford
    FC' getting omitted from the candidate list for the live game window).
    """
    from .matcher import ChannelCandidate, _team_keywords
    from apps.channels.models import Channel
    from apps.epg.models import ProgramData
    from django.db.models import Q

    def lookup(game) -> List[ChannelCandidate]:
        # Per-sport match window — soccer needs a tighter window to avoid
        # false-matching pre-game preview shows earlier in the day; NCAAF
        # / NFL keep the wide default for long pre-game shows + OT. See #4.
        pre_min, post_hours = _epg_match_window(game.sport_prefix)
        window_start = game.start_time - timedelta(minutes=pre_min)
        window_end = game.start_time + timedelta(hours=post_hours)

        all_kws = _team_keywords(game.home) + _team_keywords(game.away)
        if not all_kws:
            return []

        # Path A: programs in window whose TITLE mentions any team keyword.
        title_q = Q()
        for kw in all_kws:
            title_q |= Q(title__icontains=kw)
        title_progs = list(
            ProgramData.objects
            .filter(start_time__lt=window_end, end_time__gt=window_start)
            .filter(title_q)
            .only("id", "title", "start_time", "end_time", "epg_id")
        )

        # Path B: channels whose NAME mentions any team keyword. Include
        # them even without EPG entries in window — provider channels often
        # advertise the match in the channel NAME but have no program data
        # (e.g. 'AU (STAN 01) | Manchester United v Brentford ...'). Tier-1
        # strict still discriminates by 'both teams in channel name', so
        # noise channels with only one team mentioned won't pass through.
        name_q = Q()
        for kw in all_kws:
            name_q |= Q(name__icontains=kw)
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


def _resolve_park_base(target_base: int, num_games: int) -> int:
    """Pick a parking range that's guaranteed past every target number we'll
    write. Parking + writing happens in one transaction within our own group,
    so we only need to clear our own target range; +1000 slack keeps the
    parking range comfortably out of any plausible target_base growth."""
    return target_base + max(num_games, 0) + 1000


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
        # Cache files predating this signal default to 0.0 / [] — graceful
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
# 1 isn't enough — normal weekly scheduling routinely puts midweek games at
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

    DO NOT use `min(n%10, 3)` to index the suffixes — that clamps 4..9 to
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

    Format: "<home> <pos>, <pts> pts. <away> <pos>, <pts> pts — <gap>."
    where <gap> is computed for the away team relative to the home team.

    Returns None if there's no standings table (knockout / non-soccer), if
    neither playing team appears in the table (e.g. cold-start with
    promoted teams), or if essential fields are missing.

    Surfaces the position+points data the soccer source already cached
    under extra.standings_table — see #10.
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
    # so they match byte-for-byte — no fuzzy matching needed.
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

    # Both teams in the table — add a gap descriptor for the away team
    # relative to the home team. Reads naturally: "...69 pts — 1 pt behind."
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
        # Tied on points — goal difference is the actual league
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

    return f"{home_str}. {away_str} — {gap}."


def _build_description(
    g: Dict[str, Any],
    tagline: str,
    placeholder: bool,
) -> str:
    """Build the EPG ProgramData description in natural-language form.

    Layout (each block separated by a blank line):
      1. Placeholder note (only if EPG hasn't matched a source yet)
      2. Headline: tagline + spread descriptor ("A title race — toss-up.")
      3. Matchday + league boundary summary (where applicable)
      4. Standings posture line (league fixtures only — see #10)
      5. Favorite-impact narratives (rooting framing, both deltas)
      6. "Favorite is your team" line if the favorite is playing this game
      7. Source channel line (only if matched)

    Deliberately dropped: kickoff time (already shown by EPG client time
    blocks), score breakdown (already in channel name as ★X.X), spread's
    raw line value (just say "toss-up"), late-season multiplier
    annotation (uniform across all current league games — adds no signal).
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
        sections.append(" — ".join(headline_parts) + ".")

    # 3. Matchday line + league boundary reminder. Both are league-based
    # ("why is this a race"). Matchday tells you where in the season we
    # are; boundary_summary explains what positions get what.
    league_ctx = _league_context_for(g)
    matchday = extra.get("matchday")
    matchdays_total = extra.get("matchdays_total") or (
        league_ctx.matchdays_total if league_ctx else None
    )
    matchday_line_parts: List[str] = []
    if matchday and matchdays_total:
        # "Catch-up matchday X of Y" when fixture is meaningfully behind
        # the league's current pacing — see _is_catchup_matchday and #3.
        # Without the label, an end-of-season "Matchday 40 of 46" reads as
        # if the team has 6 games left when really it's a postponement
        # being replayed late and they have 1.
        label = "Catch-up matchday" if _is_catchup_matchday(g) else "Matchday"
        matchday_line_parts.append(f"{label} {matchday} of {matchdays_total}.")
    if league_ctx and league_ctx.boundary_summary:
        matchday_line_parts.append(league_ctx.boundary_summary + ".")
    if matchday_line_parts:
        sections.append(" ".join(matchday_line_parts))

    # 4. Standings posture (league fixtures only — knockout cups have no
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
         (source_type=xmltv, is_active=False — we write programs directly,
         and we mustn't be source_type=dummy or Dispatcharr's UI overlays
         joke-filler descriptions on top of ours).
         Then replace its ProgramData for the game's airtime with title=matchup
         and description=WHY breakdown.

    The description shows up natively in TiviMate/Plex/Jellyfin guides.
    Source channels are never touched. Stale virtual channels (game no longer
    in cache) are deleted along with their EPG entries.
    """
    from apps.channels.models import Channel, ChannelGroup, ChannelStream
    from apps.epg.models import EPGSource, EPGData, ProgramData
    from django.db import transaction

    from .scoring import format_channel_name

    cache = _read_cache()
    games = cache.get("games", [])
    if not games:
        return {"status": "ok", "message": "Cache empty; run refresh first."}

    group_name = settings.get("channel_profile_name", "Top Matchups")
    dry_run = bool(settings.get("dry_run", True))

    # 1. Ensure target ChannelGroup. Also detect any old groups/sources we own
    # (from a previous target_group_name) and clean them up — fixes the case
    # where the user renames "Top Matchups" → "!Top Matchups" between runs.
    target_group = ChannelGroup.objects.filter(name=group_name).first()

    # Find any other groups containing channels we own. Helper covers both the
    # current TVG_ID_PREFIX scheme and any legacy markers from earlier plugin
    # versions (a prefix-only check would miss leftovers on rename).
    foreign_owned_groups = list(
        ChannelGroup.objects.exclude(name=group_name)
        .filter(_owned_tvg_id_q("channels__"))
        .distinct()
    )
    foreign_epg_sources = list(
        EPGSource.objects.exclude(name=group_name)
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
    # place. DO NOT delete + recreate — Channel.id is the stable handle that
    # ChannelProfileMembership, IPTV-client playlist caches, and the user's
    # pinned-channel state all key off. A delete-then-create cycle silently
    # orphans every one of those: profile memberships are gone (Dispatcharr
    # auto-adds new channels to existing profiles ONLY at profile-creation
    # time, never on channel-creation), and IPTV clients that cached the old
    # tvg-id render an empty slot for the renamed channel until the user
    # manually refreshes — exactly what bit us during the #1 live-verify.
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
    park_base = _resolve_park_base(virtual_base, len(games))
    logger.info(
        "[ranked_matchups] virtual_base=%d (highest_other=%s, setting=%r), park_base=%d",
        virtual_base, highest_other,
        settings.get("virtual_channel_base", DEFAULT_VIRTUAL_CHANNEL_BASE),
        park_base,
    )

    created = 0
    updated = 0
    deleted_stale = 0
    skipped_unmatched = 0
    seen_markers = set()

    placeholder_threshold = float(settings.get("placeholder_min_score", 5.0))
    placeholder_channels_created = 0

    # Optional Claude-rewritten EPG descriptions. Default off; when on, prose
    # replaces the deterministic `_build_description` output for non-placeholder
    # games. Failures fall back silently. cache.json (scores, breakdown,
    # score_notes) is untouched — only ProgramData.description changes.
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
    # logo — preserving the v0 behavior for the long tail. Per-marker thumb
    # URLs cached on disk to keep API hits to once per fixture per ~14 days.
    from . import logos as matchup_logos
    matchup_logos_enabled = bool(settings.get("enable_matchup_logos", True))
    sportsdb_api_key = SPORTSDB_DEFAULT_KEY
    thumb_cache: Optional[matchup_logos.ThumbCache] = None
    matchup_logos_used = 0
    matchup_logos_fallback = 0
    if matchup_logos_enabled and not dry_run:
        sportsdb_api_key = (
            _resolve_key(settings, "sportsdb_api_key", SPORTSDB_KEY_PATH)
            or SPORTSDB_DEFAULT_KEY
        )
        thumb_cache = matchup_logos.ThumbCache(SPORTSDB_THUMB_CACHE_PATH)

    def _resolve_matchup_logo_id(
        game: Dict[str, Any], marker: str, source,
    ) -> Tuple[Optional[int], bool]:
        """Returns (logo_id, hit_via_sportsdb).

        Tries SportsDB lookup → local download → Logo.get_or_create. Falls back
        to source.logo_id (current v0 behavior) on any miss; the bool tells the
        caller which path was taken so it can update the per-apply counters
        without recomputing the fallback id. Dry_run and feature-disabled
        paths short-circuit to the fallback before any HTTP.
        """
        fallback_id = source.logo_id if source else None
        if not matchup_logos_enabled or dry_run or thumb_cache is None:
            return fallback_id, False
        fresh, cached_url = thumb_cache.get(marker)
        thumb_url = cached_url
        if not fresh:
            start_dt = parse_iso_utc(game.get("start_time_utc"))
            if start_dt is None:
                return fallback_id, False
            thumb_url = matchup_logos.resolve_thumb_url(
                home=game.get("home", ""),
                away=game.get("away", ""),
                expected_dt=start_dt,
                sport_prefix=game.get("sport_prefix"),
                api_key=sportsdb_api_key,
            )
            thumb_cache.put(marker, thumb_url)
        if not thumb_url:
            return fallback_id, False
        # Ensure /data/logos/ exists (Dispatcharr creates it lazily on first
        # upload via the UI; we might race a fresh install).
        try:
            os.makedirs(matchup_logos.LOGO_DIR, exist_ok=True)
        except OSError as e:
            logger.warning("[ranked_matchups] cannot mkdir %s: %s", matchup_logos.LOGO_DIR, e)
            return fallback_id, False
        local_path = os.path.join(
            matchup_logos.LOGO_DIR, matchup_logos.marker_to_filename(marker),
        )
        if not os.path.exists(local_path):
            if not matchup_logos.download_thumb(thumb_url, local_path):
                return fallback_id, False
        from apps.channels.models import Logo
        logo_obj, _ = Logo.objects.get_or_create(
            url=local_path,
            defaults={"name": f"Top Matchup: {game.get('home','?')} vs {game.get('away','?')}"},
        )
        return logo_obj.id, True

    with transaction.atomic():
        # Phase 0: park existing virtual channels in a high temporary number
        # range so we can renumber based on cache order without colliding with
        # the unique (channel_group, channel_number) constraint. park_base is
        # guaranteed to be past every target number we're about to write.
        if not dry_run and existing_virtuals:
            for i, ch in enumerate(existing_virtuals.values()):
                ch.channel_number = float(park_base + i)
            for ch in existing_virtuals.values():
                ch.save(update_fields=["channel_number"])

        for cache_idx, g in enumerate(games):
            # channel_ids is the full list of matched provider channels (e.g.
            # multiple regional/quality variants of the same fixture). Falls
            # back to the single-channel `channel_id` key for cache entries
            # written by an older plugin version. Primary (first) drives
            # the channel logo and EPG context.
            source_ids = list(g.get("channel_ids") or [])
            if not source_ids:
                primary_id = g.get("channel_id")
                if primary_id:
                    source_ids = [primary_id]
            sources = list(Channel.objects.filter(id__in=source_ids))
            # Preserve the matcher-given order (channel_ids is primary-first).
            sources_by_id = {c.id: c for c in sources}
            sources = [sources_by_id[sid] for sid in source_ids if sid in sources_by_id]
            source = sources[0] if sources else None

            placeholder = False
            if not source:
                score_val = float(g.get("score", 0.0))
                if score_val >= placeholder_threshold:
                    placeholder = True
                    placeholder_channels_created += 1
                else:
                    skipped_unmatched += 1
                    continue

            # Target channel number = base + cache index. Today's games are at
            # the front of the cache, so they get the lowest numbers (TiviMate
            # and other IPTV clients sort by channel number → today's games
            # appear first in the user's Top Matchups group).
            target_chnum = float(virtual_base + cache_idx)

            marker = _build_marker_key(g)
            seen_markers.add(marker)

            from .scoring import pick_tagline
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
            new_name = format_channel_name(
                g["sport_prefix"], signals, score, g["home"], g["away"], tagline=tagline,
            )

            start_dt = parse_iso_utc(g.get("start_time_utc"))
            if start_dt is None:
                logger.warning("[ranked_matchups] bad start_time_utc on %s", marker)
                continue
            prog_start = start_dt - timedelta(minutes=EPG_PRE_MIN)
            prog_end = start_dt + timedelta(hours=EPG_POST_HOURS)

            description = _build_description(
                g=g,
                tagline=tagline,
                placeholder=placeholder,
            )

            # If LLM-rewritten descriptions are enabled and this isn't a
            # placeholder (placeholders have no EPG match yet, so the "channel
            # match pending" note matters more than prose), try the call and
            # fall back to `description` on any failure.
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

            # Gather streams from EVERY matched source channel and rank by
            # composite quality key: valid ffprobe data (height/bitrate)
            # ranks first, then name-keyword fallback, then probe-failed
            # streams last. Stable secondary sort by source-channel order
            # so equal-quality streams preserve the matcher's primary-first
            # ordering.
            stream_pool = []  # list of (quality_key, src_order, stream_id)
            seen_stream_ids = set()
            for src_order, src in enumerate(sources):
                for s in src.streams.all().only("id", "name", "stream_stats"):
                    if s.id in seen_stream_ids:
                        continue
                    seen_stream_ids.add(s.id)
                    key = _stream_sort_key(s.stream_stats, s.name or src.name or "")
                    stream_pool.append((key, src_order, s.id))
            stream_pool.sort()
            source_streams = [sid for _, _, sid in stream_pool]
            existing = existing_virtuals.get(marker)

            resolved_logo_id, used_sportsdb = _resolve_matchup_logo_id(g, marker, source)
            if matchup_logos_enabled and not dry_run:
                if used_sportsdb:
                    matchup_logos_used += 1
                else:
                    matchup_logos_fallback += 1

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
                    # Compare by ORDERED list, not set — when our sort key
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
                    # DO NOT use vc.save(update_fields=["epg_data"]) —
                    # apps/channels/signals.py post_save fires
                    # parse_programs_for_tvg_id which unconditionally deletes
                    # ProgramData for the tvg_id (apps/epg/tasks.py:1308) before
                    # attempting an EPG-source refetch. Our EPGSource has no
                    # URL/file (we write programs directly), so the refetch
                    # fails and the rows stay deleted until the next plugin
                    # tick — wiping the EPG grid for 0–3 minutes per new
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
                # name carries — the EPG entry should read like a program,
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
                # Past slot — bridges game-end to the next scheduled refresh
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

        # 5. Delete stale virtual channels (not seen this refresh)
        stale = [ch for marker, ch in existing_virtuals.items() if marker not in seen_markers]
        if stale:
            if not dry_run:
                stale_ids = [c.id for c in stale]
                stale_markers = [c.tvg_id for c in stale]
                ChannelStream.objects.filter(channel_id__in=stale_ids).delete()
                Channel.objects.filter(id__in=stale_ids).delete()
                if epg_source is not None:
                    EPGData.objects.filter(
                        epg_source=epg_source, tvg_id__in=stale_markers,
                    ).delete()
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

    # Persist the LLM-description cache (prune entries whose marker is no
    # longer in this refresh; keep file bounded to live games). Save outside
    # the atomic block — sidecar JSON file is independent of the DB.
    if llm_enabled and not dry_run:
        pruned = llm_descriptions.prune_cache(llm_cache, seen_markers)
        llm_descriptions.write_cache(LLM_DESCRIPTIONS_CACHE_PATH, pruned)

    # Persist the SportsDB thumb-URL cache and sweep stale matchup logo files
    # from /data/logos/. Both prune to the live marker set so disk usage
    # doesn't grow unbounded across many refresh cycles. Logo rows pointing
    # at deleted files are left for Dispatcharr's own cleanup_unused_logos
    # endpoint — deleting them here would race with concurrent UI reads.
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
    # `placeholders` is a *subset* of (created + updated) — placeholder games
    # go through the same upsert path as matched ones, so they're already
    # counted there. Report as "(placeholders=N included)" to avoid the
    # "10 created + 3 placeholders == 13?" misread.
    llm_msg = ""
    if llm_enabled:
        llm_msg = f" LLM descriptions: {llm_used} written, {llm_failed} fell back to deterministic."
    logo_msg = ""
    if matchup_logos_enabled and not dry_run:
        logo_msg = (
            f" Matchup logos: {matchup_logos_used} resolved via SportsDB, "
            f"{matchup_logos_fallback} fell back to source-channel logo, "
            f"{stale_logo_files_swept} stale file(s) swept."
        )
    msg = (
        f"{prefix}Group {group_name!r}: created={created}, updated={updated} "
        f"(placeholders={placeholder_channels_created} included), "
        f"stale_deleted={deleted_stale}, "
        f"orphan_epg_deleted={orphan_epg_deleted if 'orphan_epg_deleted' in locals() else 0}, "
        f"unmatched_skipped={skipped_unmatched}.{rename_msg}{llm_msg}{logo_msg} "
        f"WHY descriptions written to EPG source."
    )
    return {"status": "ok", "message": msg}


# ---------- auto pipeline ----------

def _action_auto_pipeline(settings: Dict[str, Any]) -> Dict[str, Any]:
    r1 = _action_refresh(settings)
    if r1.get("status") != "ok":
        return r1
    r2 = _action_apply(settings)
    return {
        "status": r2.get("status", "ok"),
        "message": f"refresh: {r1.get('message')} | apply: {r2.get('message')}",
    }


# ---------- show status ----------

def _action_show_status(settings: Dict[str, Any]) -> Dict[str, Any]:
    del settings  # interface-required (Plugin.run dispatch), not read here
    cache = _read_cache()
    games = cache.get("games", [])
    if not games:
        return {"status": "ok", "message": "Cache empty. Run refresh."}
    lines = [
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


# ---------- scheduler ----------

_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop = threading.Event()


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


def _scheduler_loop(plugin_ref):
    """Auto-refresh + apply at every time listed in scheduled_times."""
    while not _scheduler_stop.is_set():
        try:
            settings = plugin_ref.get_current_settings()
            if not settings.get("auto_refresh_enabled", False):
                _scheduler_stop.wait(timeout=300)
                continue
            tz = _resolve_tz(settings.get("local_timezone", "UTC"))
            times = _parse_scheduled_times(settings.get("scheduled_times", "0400"))
            if not times:
                logger.warning("[ranked_matchups] no valid scheduled_times; idling 5m")
                _scheduler_stop.wait(timeout=300)
                continue
            target = _next_fire_time(times, tz)
            if target is None:
                _scheduler_stop.wait(timeout=300)
                continue
            sleep_s = (target - datetime.now(tz)).total_seconds()
            logger.info(
                "[ranked_matchups] scheduler sleeping %.0fs until %s (next of %s)",
                sleep_s, target.isoformat(), times,
            )
            if _scheduler_stop.wait(timeout=sleep_s):
                return
            if not _try_acquire_scheduler_lock():
                logger.info("[ranked_matchups] another worker holds the lock; skipping")
                continue
            try:
                logger.info("[ranked_matchups] scheduler firing auto_pipeline")
                result = _action_auto_pipeline(settings)
                logger.info("[ranked_matchups] auto_pipeline: %s", result.get("message"))
            finally:
                _release_scheduler_lock()
        except Exception:
            logger.exception("[ranked_matchups] scheduler loop crashed; sleeping 10m")
            _scheduler_stop.wait(timeout=600)


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

class Plugin:
    name = "Ranked Matchups (Top Games)"
    version = "0.1.0"

    def __init__(self):
        # The scheduler reads settings live from the DB on each tick rather than
        # relying on stale init-time settings.
        global _scheduler_thread
        if _scheduler_thread is None or not _scheduler_thread.is_alive():
            _scheduler_stop.clear()
            t = threading.Thread(target=_scheduler_loop, args=(self,), daemon=True,
                                 name="ranked_matchups-scheduler")
            t.start()
            _scheduler_thread = t
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

    def run(self, action: Optional[str] = None,
            params: Optional[Dict[str, Any]] = None,
            context: Optional[Dict[str, Any]] = None):
        ctx = context or {}
        settings = dict(ctx.get("settings") or {})
        if params:
            settings.update(params)
        try:
            if action == "refresh":
                return _action_refresh(settings)
            if action == "apply":
                return _action_apply(settings)
            if action == "auto_pipeline":
                return _action_auto_pipeline(settings)
            if action == "show_status":
                return _action_show_status(settings)
            return {"status": "error", "message": f"Unknown action: {action!r}"}
        except Exception as e:
            logger.exception("[ranked_matchups] action %r failed", action)
            return {"status": "error", "message": f"{type(e).__name__}: {e}"}
