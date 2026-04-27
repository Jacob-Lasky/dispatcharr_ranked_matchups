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
  - cache.json:        last refresh result with per-game score breakdowns
  - cfbd_api_key:      CFBD bearer token (chmod 600)
  - anthropic_api_key: Claude key (chmod 600). Falls back to symlinking the sports
                       filter's key if not present.
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

logger = logging.getLogger("plugins.dispatcharr_ranked_matchups")

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

# Channel-number range for our virtual channels. Picked high so we don't collide
# with real auto-channel-sync numbers.
VIRTUAL_CHANNEL_BASE = 9000


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
    from .sources import NcaafSource, SoccerSource
    sources = []
    if settings.get("enable_ncaaf", True):
        cfbd_key = _read_key(CFBD_KEY_PATH)
        sources.append(NcaafSource(api_key=cfbd_key))
    fd_key = _read_key(FD_KEY_PATH)
    odds_key = _read_key(ODDS_KEY_PATH)
    if settings.get("enable_epl", False) and fd_key:
        sources.append(SoccerSource("epl", fd_api_key=fd_key, odds_api_key=odds_key))
    if settings.get("enable_championship", False) and fd_key:
        sources.append(SoccerSource("championship", fd_api_key=fd_key, odds_api_key=odds_key))
    if settings.get("enable_ucl", False) and fd_key:
        sources.append(SoccerSource("ucl", fd_api_key=fd_key, odds_api_key=odds_key))
    # Phase 2: NCAAM, baseball
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
        compute_impact_on_favorites,
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

        # Impact on favorites: non-favorite games that move a favorite's table
        favs_in_league: List[Tuple[str, int]] = []
        standings_table = extra.get("standings_table") or []
        if standings_table:
            for fav in favorites:
                fav_lc = fav.lower()
                for entry in standings_table:
                    name = entry.get("name", "")
                    if fav_lc in name.lower():
                        favs_in_league.append((name, entry["position"]))
                        break  # one match per favorite per competition
        impact_favs = compute_impact_on_favorites(
            g.rank_home, g.rank_away, g.home, g.away, favs_in_league,
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
    tz_local = _resolve_tz(settings.get("local_timezone", "America/Chicago"))
    scored.sort(key=lambda x: (
        0 if _is_today_local(x[0].start_time, tz_local) else 1,
        -x[2].final,
        -x[2].raw,
        x[0].start_time,
    ))

    # 3. Cap to max_games but always include favorites
    if len(scored) > max_games:
        favs = [s for s in scored if s[1].favorite_match]
        non_favs = [s for s in scored if not s[1].favorite_match]
        keep_non_favs = non_favs[: max(0, max_games - len(favs))]
        # Re-sort the kept set by score
        scored = sorted(favs + keep_non_favs, key=lambda x: (-x[2].raw, x[0].start_time))

    # 4. EPG match each game to a Dispatcharr channel.
    # _build_epg_lookup excludes ALL our virtual channels by tvg_id prefix —
    # covers both the current target group and any orphans from a renamed group.
    epg_lookup = _build_epg_lookup()
    api_key = _read_key(ANTHROPIC_KEY_PATH) or _read_key(
        os.path.join(os.path.dirname(PLUGIN_DIR), "dispatcharr_sports_filter", "anthropic_api_key")
    )
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
    """
    sport = game.get("sport_prefix", "?")
    extra = game.get("extra") or {}
    cfbd_id = extra.get("cfbd_id")
    if cfbd_id:
        return f"{TVG_ID_PREFIX}{sport}:{cfbd_id}"
    # Fallback: hash of teams + start time
    fallback = f"{game.get('away','')}|{game.get('home','')}|{game.get('start_time_utc','')}"
    return f"{TVG_ID_PREFIX}{sport}:{abs(hash(fallback))}"


def _next_virtual_channel_number(used: set) -> float:
    """Pick the next available number in our virtual range."""
    n = float(VIRTUAL_CHANNEL_BASE)
    while n in used:
        n += 1
    used.add(n)
    return n


# When we need to renumber existing channels to match cache order, shift them
# into this temporary range first to avoid colliding with the target numbers.
_RENUMBER_PARK_BASE = 19000


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
    score = GameScore(
        raw=g.get("score_raw", g.get("score", 0.0)),
        final=g.get("score", 0.0),
        breakdown=g.get("score_breakdown", {}),
        notes=g.get("score_notes", []),
    )
    return signals, score


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
    from datetime import datetime, timedelta, timezone as _tz

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
    used_numbers = set(
        Channel.objects.filter(channel_number__gte=VIRTUAL_CHANNEL_BASE)
        .values_list("channel_number", flat=True)
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
        # the unique (channel_group, channel_number) constraint. After parking,
        # we'll assign each surviving channel a target number 9000+idx based on
        # its position in the (today-first, score-desc) cache.
        if not dry_run and existing_virtuals:
            for i, ch in enumerate(existing_virtuals.values()):
                ch.channel_number = float(_RENUMBER_PARK_BASE + i)
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
            target_chnum = float(VIRTUAL_CHANNEL_BASE + cache_idx)

            marker = _build_marker_key(g)
            seen_markers.add(marker)

            signals, score = _build_signals_score_from_payload(g)
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
            )
            new_name = format_channel_name(
                g["sport_prefix"], signals, score, g["home"], g["away"], why=why,
            )

            # Parse start time for EPG window
            try:
                start_dt = datetime.fromisoformat(g["start_time_utc"])
            except Exception:
                logger.warning("[ranked_matchups] bad start_time_utc on %s", marker)
                continue
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=_tz.utc)
            # Pad: program shown 30min before kickoff, runs 4h
            prog_start = start_dt - timedelta(minutes=30)
            prog_end = start_dt + timedelta(hours=4)

            # EPG description body — full transparency on why this game made the list
            score_lines = [f"  {k}: +{v}" for k, v in (g.get("score_breakdown") or {}).items()]
            kickoff_line = g.get("kickoff_local") or g.get("start_time_utc", "")
            today_marker = " 🔴 TODAY" if g.get("is_today") else ""
            placeholder_note = (
                "\n[NOTE] No EPG match found yet — this is a placeholder channel. "
                "Provider EPG hasn't published this game's broadcast info; "
                "channel will activate (streams added) on the next refresh once EPG appears.\n"
                if placeholder else ""
            )
            description = (
                f"{why}.{placeholder_note}\n"
                f"Kickoff: {kickoff_line}{today_marker}\n"
                f"Matchup: {g.get('away')} @ {g.get('home')}\n"
                f"Sport: {g.get('sport_label', g.get('sport_prefix'))}\n"
                f"Score: {score.final:.1f}/10  (raw {score.raw:.1f})\n"
                f"Score breakdown:\n" + "\n".join(score_lines) + "\n\n"
                f"Source channel: {g.get('channel_name_current') or '(none — placeholder)'}\n"
                f"EPG title at airtime: {g.get('program_title') or '(none)'}"
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
    msg = (
        f"{prefix}Group {group_name!r}: created={created}, updated={updated}, "
        f"placeholders={placeholder_channels_created}, stale_deleted={deleted_stale}, "
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
            tz = _resolve_tz(settings.get("local_timezone", "America/Chicago"))
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


def _try_acquire_scheduler_lock() -> bool:
    """Cross-worker mutex via Redis. ttl 30 min so a crashed run releases."""
    try:
        from core.utils import RedisClient
        r = RedisClient.get_client()
        return bool(r.set("plugins:ranked_matchups:scheduler:lock", "1", nx=True, ex=1800))
    except Exception as e:
        logger.warning("[ranked_matchups] redis lock failed (%s); proceeding without lock", e)
        return True


def _release_scheduler_lock() -> None:
    try:
        from core.utils import RedisClient
        RedisClient.get_client().delete("plugins:ranked_matchups:scheduler:lock")
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
            pc = PluginConfig.objects.filter(key="dispatcharr_ranked_matchups").first()
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
