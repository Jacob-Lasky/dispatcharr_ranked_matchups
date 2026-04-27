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
  - cfbd_api_key:         CFBD/CBB-Data bearer token (chmod 600)
  - football_data_api_key: Football-Data.org token (chmod 600)
  - odds_api_key:         The Odds API token (chmod 600)
  - anthropic_api_key:    Claude key (chmod 600), only needed for LLM EPG
                          matching or the optional narrative signal.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # py < 3.9 fallback (won't hit on Dispatcharr's Python 3.13)
    ZoneInfo = None  # type: ignore

from ._util import parse_iso_utc, stable_hash_int

# Derived from the package directory so the loader, logger, and PluginConfig
# row all stay in sync if the directory is ever renamed.
PLUGIN_KEY = __package__ or "dispatcharr_ranked_matchups"

logger = logging.getLogger(f"plugins.{PLUGIN_KEY}")

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(PLUGIN_DIR, "cache.json")
CFBD_KEY_PATH = os.path.join(PLUGIN_DIR, "cfbd_api_key")
FD_KEY_PATH = os.path.join(PLUGIN_DIR, "football_data_api_key")
ODDS_KEY_PATH = os.path.join(PLUGIN_DIR, "odds_api_key")
ANTHROPIC_KEY_PATH = os.path.join(PLUGIN_DIR, "anthropic_api_key")

# Window relative to game start in which the EPG should show the game.
EPG_PRE_MIN = 30      # 30 min before game starts
EPG_POST_HOURS = 4    # 4 hours after game starts (covers OT)

# Marker we put in tvg_id of cloned channels so we can find/clean them up later
# without needing a custom_properties field on the Channel model.
TVG_ID_PREFIX = "ranked_matchups:"

# Default starting channel number when the user hasn't configured one. Sentinel
# 0 means "auto" — pick the first channel number after the highest existing
# non-virtual channel, so we slot in cleanly without colliding with real
# channels.
DEFAULT_VIRTUAL_CHANNEL_BASE = 0

# Default fallback when DEFAULT_VIRTUAL_CHANNEL_BASE is sentinel-0 AND there
# are zero existing channels (fresh install) — picked high enough not to
# collide with auto-channel-sync ranges.
_AUTO_BASE_FALLBACK = 9000


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


def _build_weights(settings: Dict[str, Any]):
    from .scoring import Weights
    return Weights(
        rank=float(settings.get("weight_rank", 1.0)),
        spread=float(settings.get("weight_spread", 0.5)),
        favorite=float(settings.get("weight_favorite", 4.0)),
        rivalry=float(settings.get("weight_rivalry", 2.0)),
        stakes=float(settings.get("weight_stakes", 2.0)),
        tournament=float(settings.get("weight_tournament", 1.5)),
        impact_favorite=float(settings.get("weight_impact_favorite", 1.0)),
        narrative=float(settings.get("weight_narrative", 0.0)),
    )


def _build_sources(settings: Dict[str, Any]):
    from .sources import NcaafSource, NcaamSource, SoccerSource
    sources = []
    cfbd_key = _resolve_key(settings, "cfbd_api_key", CFBD_KEY_PATH)
    fd_key = _resolve_key(settings, "football_data_api_key", FD_KEY_PATH)
    odds_key = _resolve_key(settings, "odds_api_key", ODDS_KEY_PATH)
    if settings.get("enable_ncaaf", False) and cfbd_key:
        sources.append(NcaafSource(api_key=cfbd_key))
    # NCAAM uses the same CFBD/CBB-Data Bearer token as NCAAF.
    if settings.get("enable_ncaam", False) and cfbd_key:
        sources.append(NcaamSource(api_key=cfbd_key))
    if settings.get("enable_epl", False) and fd_key:
        sources.append(SoccerSource("epl", fd_api_key=fd_key, odds_api_key=odds_key))
    if settings.get("enable_championship", False) and fd_key:
        sources.append(SoccerSource("championship", fd_api_key=fd_key, odds_api_key=odds_key))
    if settings.get("enable_ucl", False) and fd_key:
        sources.append(SoccerSource("ucl", fd_api_key=fd_key, odds_api_key=odds_key))
    return sources


# ---------- refresh ----------

def _action_refresh(settings: Dict[str, Any]) -> Dict[str, Any]:
    from .scoring import GameSignals, score_game
    from .matcher import match_games_to_channels

    favorites = _parse_favorites(settings.get("favorites", ""))
    weights = _build_weights(settings)
    lookahead = int(settings.get("lookahead_days", 7))
    max_games = int(settings.get("max_games", 25))

    sources = _build_sources(settings)
    if not sources:
        return {"status": "error", "message": "No sport sources enabled."}

    # 1. Fetch
    all_games = []
    src_summary = []
    for src in sources:
        try:
            games = src.fetch_upcoming(days_ahead=lookahead)
        except Exception as e:
            logger.exception("[ranked_matchups] source %s fetch failed", src.sport_label)
            src_summary.append(f"{src.sport_label}: error ({e})")
            continue
        all_games.extend(games)
        src_summary.append(f"{src.sport_label}: {len(games)} games")
        logger.info("[ranked_matchups] %s: pulled %d games", src.sport_label, len(games))

    if not all_games:
        msg = "No games found in lookahead window. " + " | ".join(src_summary)
        logger.info("[ranked_matchups] %s", msg)
        cache = {"games": [], "refreshed_at": datetime.now(timezone.utc).isoformat(),
                 "summary": src_summary}
        _write_cache(cache)
        return {"status": "ok", "message": msg}

    # 2. Score (with Phase 3 standings/tournament/impact signals)
    from .scoring import (
        match_favorites, LEAGUE_CONTEXTS, compute_team_stakes,
        compute_impact_on_favorites, build_impact_narratives,
    )
    scored: List[Tuple[Any, GameSignals, Any]] = []
    for g in all_games:
        extra = g.extra or {}
        comp_code = extra.get("fd_competition_code")
        league_ctx = LEAGUE_CONTEXTS.get(comp_code) if comp_code else None

        # Stakes per team (proximity to a meaningful league threshold)
        stakes_a, hits_a = (0.0, [])
        stakes_b, hits_b = (0.0, [])
        if league_ctx:
            stakes_a, hits_a = compute_team_stakes(g.rank_home, league_ctx.thresholds)
            stakes_b, hits_b = compute_team_stakes(g.rank_away, league_ctx.thresholds)
        thresholds_hit = list(dict.fromkeys(hits_a + hits_b))

        # Impact on favorites: non-favorite games that move a favorite's table.
        # Build the rich version (with points) for narrative rendering, plus
        # the plain version (name+position) for the score signal.
        favs_with_standings: List[Dict[str, Any]] = []
        standings_table = extra.get("standings_table") or []
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
        favs_in_league: List[Tuple[str, int]] = [
            (f["name"], f["position"]) for f in favs_with_standings
        ]
        impact_favs = compute_impact_on_favorites(
            g.rank_home, g.rank_away, g.home, g.away, favs_in_league,
        )
        # Pre-render the natural-language impact narrative now and stash on
        # the row so it survives the post-score cap/resort. Apply reads it
        # straight from the cache without redoing the standings lookup.
        g.extra["impact_narratives"] = build_impact_narratives(
            g.rank_home, g.rank_away, g.home, g.away,
            favs_with_standings, standings_table,
        )

        signals = GameSignals(
            rank_a=g.rank_home,
            rank_b=g.rank_away,
            team_a=g.home,
            team_b=g.away,
            favorite_match=match_favorites(g.home, g.away, favorites),
            spread=g.spread,
            stakes_a=stakes_a,
            stakes_b=stakes_b,
            stakes_thresholds_hit=thresholds_hit,
            season_progress=float(extra.get("season_progress") or 0.0),
            tournament_stage=extra.get("stage"),
            impact_on_favorites=impact_favs,
        )
        score = score_game(signals, weights)
        scored.append((g, signals, score))

    # Sort: today's games first (0 before 1), then 0-10 score desc, then raw as
    # tiebreak, then start_time ascending. So a game today with ★7 outranks a
    # game next week with ★9.5.
    tz_local = _resolve_tz(settings.get("local_timezone", "UTC"))

    def _sort_key(item):
        game, _signals, score = item
        return (
            0 if _is_today_local(game.start_time, tz_local) else 1,
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
            "score": score.final,           # 0-10 (display-facing)
            "score_raw": score.raw,         # unbounded sum (sort tiebreak)
            "score_breakdown": score.breakdown,
            "score_notes": score.notes,
            "favorites_matched": signals.favorite_match,
            "stakes_thresholds_hit": signals.stakes_thresholds_hit,
            "season_progress": signals.season_progress,
            "tournament_stage": signals.tournament_stage,
            "impact_on_favorites": signals.impact_on_favorites,
            "channel_id": match.channel_id,
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

def _build_epg_lookup(exclude_group_name: Optional[str] = None):
    """Return a callable: GameRow -> List[ChannelCandidate]. Closure over ORM.

    Excludes any channel that is one of OUR virtual channels (tvg_id starts with
    TVG_ID_PREFIX) — covers the configured target group AND any old groups left
    over from a prior target_group_name. Without this, the matcher self-matches
    against our prior-run channels because their EPG titles literally contain
    the team names.
    """
    from .matcher import ChannelCandidate
    from apps.channels.models import Channel
    from apps.epg.models import ProgramData

    def lookup(game) -> List[ChannelCandidate]:
        window_start = game.start_time - timedelta(minutes=EPG_PRE_MIN)
        window_end = game.start_time + timedelta(hours=EPG_POST_HOURS)
        progs = (
            ProgramData.objects
            .filter(start_time__lt=window_end, end_time__gt=window_start)
            .select_related("epg")
            .only("id", "title", "start_time", "end_time", "epg_id", "epg__tvg_id")[:2000]
        )
        if not progs:
            return []
        epg_ids = {p.epg_id for p in progs if p.epg_id}
        if not epg_ids:
            return []
        # Exclude all our virtual channels (current target + any orphans)
        chan_qs = Channel.objects.filter(epg_data_id__in=epg_ids).exclude(
            tvg_id__startswith=TVG_ID_PREFIX,
        )
        chans = chan_qs.only("id", "name", "epg_data_id")
        chan_by_epg = {}
        for c in chans:
            chan_by_epg.setdefault(c.epg_data_id, []).append(c)
        out: List[ChannelCandidate] = []
        for p in progs:
            for c in chan_by_epg.get(p.epg_id, []):
                out.append(ChannelCandidate(
                    channel_id=c.id,
                    channel_name=c.name,
                    program_title=p.title or "",
                    program_start=p.start_time,
                    program_end=p.end_time,
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
      - Positive int → use that as the base (legacy: 9000)
      - 0 (sentinel) → auto: pick (highest existing non-virtual channel) + 1,
        so virtuals slot in just after the user's real channels.
      - Anything unparseable → treat as auto.

    `highest_non_virtual` is the max channel_number across all channels that
    are NOT ours (excluding tvg_id__startswith=TVG_ID_PREFIX). Caller passes 0
    if there are no other channels.

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
    so we only need to clear our own target range — +1000 slack is safety
    margin against future-us reusing the parking range for something else."""
    return target_base + max(num_games, 0) + 1000


def _build_signals_score_from_payload(g: Dict[str, Any]):
    """Reconstruct GameSignals + GameScore from cache.json payload."""
    from .scoring import GameSignals, GameScore
    signals = GameSignals(
        rank_a=g.get("rank_home"),
        rank_b=g.get("rank_away"),
        team_a=g.get("home", ""),
        team_b=g.get("away", ""),
        favorite_match=g.get("favorites_matched", []),
        spread=g.get("spread"),
        stakes_thresholds_hit=g.get("stakes_thresholds_hit") or [],
        season_progress=g.get("season_progress") or 0.0,
        tournament_stage=g.get("tournament_stage"),
        impact_on_favorites=g.get("impact_on_favorites") or [],
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


def _build_description(
    g: Dict[str, Any],
    tagline: str,
    why: str,
    placeholder: bool,
) -> str:
    """Build the EPG ProgramData description in natural-language form.

    Layout (in order, each block separated by a blank line):
      1. kickoff line + optional "today" marker, plus a placeholder note
         if no EPG match was found
      2. Headline: tagline + spread descriptor in one sentence
      3. Pre-rendered favorite-impact narrative(s) from the cache
      4. Optional "favorite is your team" line if the favorite is playing
      5. Score breakdown one-liner
      6. Source channel line (only if matched)

    No more `Matchup:` / `Sport:` / `(raw X.X)` lines — they're already in
    the channel name or are debug noise.
    """
    score_final = float(g.get("score", 0.0))
    extra = g.get("extra") or {}
    spread = g.get("spread")
    favorites_matched = g.get("favorites_matched") or []
    impact_narratives = (
        extra.get("impact_narratives")
        or g.get("impact_narratives")  # fallback if older cache shape
        or []
    )

    sections: List[str] = []

    # 1. Kickoff line + placeholder note
    kickoff_line = g.get("kickoff_local") or g.get("start_time_utc", "")
    today_marker = " 🔴" if g.get("is_today") else ""
    opener = f"{kickoff_line}{today_marker}"
    if placeholder:
        opener += (
            "\n_Channel match pending: broadcaster's EPG hasn't published "
            "this fixture yet. Will activate on the next refresh once it "
            "appears._"
        )
    sections.append(opener)

    # 2. Headline sentence: combines tagline + spread descriptor.
    headline_parts = []
    if tagline:
        article = "An" if tagline[:1].lower() in "aeiou" else "A"
        headline_parts.append(f"{article} {tagline}")
    if spread is not None and spread <= 3:
        headline_parts.append(f"toss-up (line {spread:+.1f})")
    if headline_parts:
        sections.append(" — ".join(headline_parts) + ".")

    # 3. Pre-rendered impact narratives. One per affected favorite.
    for narrative in impact_narratives:
        sections.append(narrative)

    # 4. If the favorite is in the game, call it out (impact-narrative path
    #    skips this case).
    if favorites_matched:
        labels = ", ".join(favorites_matched)
        if len(favorites_matched) == 1:
            sections.append(f"{labels} is your favorite.")
        else:
            sections.append(f"Your favorites: {labels}.")

    # 5. Score breakdown one-liner.
    sections.append(f"Score ★{score_final:.1f} — {why}.")

    # 6. Source channel.
    src_name = g.get("channel_name_current")
    if src_name:
        sections.append(f"Source: {src_name}.")

    return "\n\n".join(sections)


def _action_apply(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Clone-into-group + dummy-EPG strategy:

    For each scored+matched game we:
      1. Get-or-create a virtual Channel in 'Top Matchups' ChannelGroup,
         linked to the same streams as the source channel (so playback works).
      2. Get-or-create an EPGData entry on our dummy 'Top Matchups' EPGSource,
         and replace its ProgramData for the game's airtime with title=matchup
         and description=WHY breakdown.

    The description shows up natively in TiviMate/Plex/Jellyfin guides.
    Source channels are never touched. Stale virtual channels (game no longer
    in cache) are deleted along with their EPG entries.
    """
    from apps.channels.models import Channel, ChannelGroup, ChannelStream
    from apps.epg.models import EPGSource, EPGData, ProgramData
    from django.db import transaction

    from .scoring import format_channel_name, build_why_text

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

    # Find any other groups containing channels we own (tvg_id starts with our prefix)
    foreign_owned_groups = list(
        ChannelGroup.objects.exclude(name=group_name)
        .filter(channels__tvg_id__startswith=TVG_ID_PREFIX)
        .distinct()
    )
    foreign_epg_sources = list(
        EPGSource.objects.exclude(name=group_name)
        .filter(epgs__tvg_id__startswith=TVG_ID_PREFIX)
        .distinct()
    )

    if not target_group:
        if dry_run:
            return {
                "status": "ok",
                "message": (
                    f"[dry] Would create ChannelGroup {group_name!r} + dummy EPGSource and clone "
                    f"{sum(1 for g in games if g.get('channel_id'))} matched games into it. "
                    f"Would also clean up {len(foreign_owned_groups)} stale group(s) and "
                    f"{len(foreign_epg_sources)} stale EPGSource(s) from prior target names."
                ),
            }
        target_group = ChannelGroup.objects.create(name=group_name)
        logger.info("[ranked_matchups] created ChannelGroup id=%s name=%r",
                    target_group.id, group_name)

    # Migrate / clean up any virtual channels in old groups
    migrated_from_old_group = 0
    deleted_old_groups = 0
    if not dry_run and foreign_owned_groups:
        for old_g in foreign_owned_groups:
            old_chans = Channel.objects.filter(
                channel_group=old_g, tvg_id__startswith=TVG_ID_PREFIX,
            )
            n = old_chans.count()
            # We re-create everything fresh anyway (cache index drives target chnum),
            # so just delete the old virtual channels here. Their stream + EPG
            # links cascade.
            ChannelStream.objects.filter(channel__in=old_chans).delete()
            old_chans.delete()
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
            ours = EPGData.objects.filter(
                epg_source=old_src, tvg_id__startswith=TVG_ID_PREFIX,
            ).count()
            if total > 0 and total == ours:
                old_src.delete()  # cascades EPGData + ProgramData
                deleted_old_sources += 1
                logger.info("[ranked_matchups] deleted old EPGSource %r (%d entries)",
                            old_src.name, total)
            else:
                # Mixed: just nuke our entries, leave the source alone
                EPGData.objects.filter(
                    epg_source=old_src, tvg_id__startswith=TVG_ID_PREFIX,
                ).delete()

    # 2. Ensure our dummy EPGSource (same pattern as event_channel_managarr's)
    epg_source = EPGSource.objects.filter(name=group_name).first()
    if not epg_source:
        if dry_run:
            logger.info("[ranked_matchups] [dry] would create EPGSource name=%r type=dummy",
                        group_name)
        else:
            epg_source = EPGSource.objects.create(
                name=group_name,
                source_type="dummy",
                is_active=True,
                refresh_interval=0,
            )
            logger.info("[ranked_matchups] created EPGSource id=%s name=%r",
                        epg_source.id, group_name)

    # 3. Existing virtual channels we'll update or delete
    existing_virtuals = {
        ch.tvg_id: ch for ch in Channel.objects.filter(
            channel_group=target_group, tvg_id__startswith=TVG_ID_PREFIX,
        )
    }

    # Resolve the virtual channel base. In auto mode we slot in just after
    # the user's highest real channel (excluding our own virtuals) so we
    # don't squat on prime numbers like 1-100.
    from django.db.models import Max as _Max
    highest_other = (
        Channel.objects.exclude(tvg_id__startswith=TVG_ID_PREFIX)
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
            source_id = g.get("channel_id")
            source = Channel.objects.filter(id=source_id).first() if source_id else None

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
                stakes_thresholds=g.get("stakes_thresholds_hit") or [],
                tournament_stage=g.get("tournament_stage"),
                season_progress=g.get("season_progress"),
                rank_a=g.get("rank_home"),
                rank_b=g.get("rank_away"),
                rank_source=rank_source,
            )
            new_name = format_channel_name(
                g["sport_prefix"], signals, score, g["home"], g["away"], tagline=tagline,
            )
            why = build_why_text(
                rank_home=g.get("rank_home"),
                rank_away=g.get("rank_away"),
                favorites_matched=g.get("favorites_matched", []),
                score_breakdown=g.get("score_breakdown", {}),
                spread=g.get("spread"),
                stakes_thresholds=g.get("stakes_thresholds_hit") or [],
                tournament_stage=g.get("tournament_stage"),
                impact_on_favorites=g.get("impact_on_favorites") or [],
                season_progress=g.get("season_progress"),
                rank_source=rank_source,
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
                why=why,
                placeholder=placeholder,
            )

            source_streams = (
                list(source.streams.all().values_list("id", flat=True))
                if source else []
            )
            existing = existing_virtuals.get(marker)

            if existing:
                changed = False
                if existing.name != new_name:
                    existing.name = new_name
                    changed = True
                source_logo_id = source.logo_id if source else None
                if existing.logo_id != source_logo_id:
                    existing.logo_id = source_logo_id
                    changed = True
                if existing.channel_number != target_chnum:
                    existing.channel_number = target_chnum
                    changed = True
                if changed and not dry_run:
                    existing.save(update_fields=["name", "logo", "channel_number"])
                if not dry_run:
                    current = set(existing.streams.values_list("id", flat=True))
                    if current != set(source_streams):
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
                        logo=(source.logo if source else None),
                        auto_created=False,
                    )
                    for order, sid in enumerate(source_streams):
                        ChannelStream.objects.create(
                            channel=vc, stream_id=sid, order=order,
                        )
                    created += 1

            # 4. EPG: get-or-create EPGData + replace ProgramData
            if not dry_run and epg_source is not None and vc is not None:
                epg_data, _ = EPGData.objects.get_or_create(
                    epg_source=epg_source,
                    tvg_id=marker,
                    defaults={"name": new_name},
                )
                if epg_data.name != new_name:
                    epg_data.name = new_name
                    epg_data.save(update_fields=["name"])
                # Link the virtual channel to this EPGData
                if vc.epg_data_id != epg_data.id:
                    vc.epg_data_id = epg_data.id
                    vc.save(update_fields=["epg_data"])
                # Replace ProgramData for this game (delete old, insert new)
                ProgramData.objects.filter(epg=epg_data).delete()
                ProgramData.objects.create(
                    epg=epg_data,
                    start_time=prog_start,
                    end_time=prog_end,
                    title=new_name,
                    sub_title=f"{g['sport_label']} — score {score.raw:.1f}/10",
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
                    channel_group=target_group, tvg_id__startswith=TVG_ID_PREFIX,
                )
            }
            orphans = EPGData.objects.filter(
                epg_source=epg_source, tvg_id__startswith=TVG_ID_PREFIX,
            ).exclude(tvg_id__in=kept_markers)
            orphan_epg_deleted, _ = orphans.delete()

    prefix = "[dry] " if dry_run else ""
    rename_msg = ""
    if migrated_from_old_group or deleted_old_groups or deleted_old_sources:
        rename_msg = (
            f" Migrated from old target: {migrated_from_old_group} channel(s) "
            f"removed from {deleted_old_groups} old group(s), {deleted_old_sources} "
            f"old EPGSource(s) deleted."
        )
    # `placeholders` is a *subset* of (created + updated) — placeholder games
    # go through the same upsert path as matched ones, so they're already
    # counted there. Report as "(placeholders=N included)" to avoid the
    # "10 created + 3 placeholders == 13?" misread.
    msg = (
        f"{prefix}Group {group_name!r}: created={created}, updated={updated} "
        f"(placeholders={placeholder_channels_created} included), "
        f"stale_deleted={deleted_stale}, "
        f"orphan_epg_deleted={orphan_epg_deleted if 'orphan_epg_deleted' in locals() else 0}, "
        f"unmatched_skipped={skipped_unmatched}.{rename_msg} "
        f"WHY descriptions written to dummy EPG source."
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
