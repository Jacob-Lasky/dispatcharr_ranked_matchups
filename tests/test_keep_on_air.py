"""A refresh must not take a game off air (#240).

2026-10-10, Georgia at Alabama (CFB, 19:30 ET):

1. At 19:00 the CFBD /rankings call returned 525. ncaaf.py degrades a failed
   request to an empty list, the refresh rebuilt the slate without any CFB
   game, and apply deleted every CFB channel half an hour before kickoff.
2. At 20:26 the M3U rematch ran a refresh with CFBD healthy again, but
   ncaaf.py lists only games that have not kicked off, so the live game was
   still missing and nothing recreated its channel.

These tests drive both paths: games already started, and games whose source
came back empty, are kept (and re-matched) until the reaper would remove them.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest

UTC = timezone.utc


def _row(plugin, home, away, start, *, label="NCAA Football", prefix="CFB",
         cfbd_id=None, channel_id=None):
    row = {
        "sport_prefix": prefix,
        "sport_label": label,
        "home": home,
        "away": away,
        "rank_home": 6,
        "rank_away": 2,
        "start_time_utc": start.isoformat(),
        "score": 9.1,
        "score_raw": 12.0,
        "channel_id": channel_id,
        "channel_ids": [channel_id] if channel_id else [],
        "stream_ids": [],
        "match_method": "regex_strict" if channel_id else None,
        "extra": {"cfbd_id": cfbd_id} if cfbd_id else {},
    }
    return row


# ---------------------------------------------------------------------------
# _rows_to_hold
# ---------------------------------------------------------------------------

class TestRowsToHold:
    NOW = datetime(2026, 10, 11, 0, 30, tzinfo=UTC)  # 20:30 ET

    def _hold(self, plugin, rows, empty=()):
        return plugin._rows_to_hold(rows, self.NOW, 30, {}, set(empty))

    def test_game_that_kicked_off_is_held(self, plugin):
        live = _row(plugin, "Alabama", "Georgia", self.NOW - timedelta(minutes=53), cfbd_id=401)
        held = self._hold(plugin, [live])
        assert list(held) == [plugin._build_marker_key(live)]

    def test_upcoming_game_from_a_healthy_source_is_not_held(self, plugin):
        later = _row(plugin, "Fresno State", "Boise State", self.NOW + timedelta(hours=2), cfbd_id=402)
        assert self._hold(plugin, [later]) == {}

    def test_upcoming_game_from_an_empty_source_is_held(self, plugin):
        later = _row(plugin, "Alabama", "Georgia", self.NOW + timedelta(minutes=30), cfbd_id=401)
        held = self._hold(plugin, [later], empty={"NCAA Football"})
        assert list(held) == [plugin._build_marker_key(later)]

    def test_empty_source_holds_only_its_own_games(self, plugin):
        soccer = _row(plugin, "Liverpool", "Man City", self.NOW + timedelta(hours=12),
                      label="English Premier League", prefix="EPL")
        assert self._hold(plugin, [soccer], empty={"NCAA Football"}) == {}

    def test_finished_game_is_not_held(self, plugin):
        # CFB's post-window is several hours; a game from yesterday is done.
        old = _row(plugin, "Alabama", "Georgia", self.NOW - timedelta(hours=20), cfbd_id=401)
        assert self._hold(plugin, [old], empty={"NCAA Football"}) == {}

    def test_undatable_game_is_not_held(self, plugin):
        bad = _row(plugin, "Alabama", "Georgia", self.NOW, cfbd_id=401)
        bad["start_time_utc"] = None
        assert self._hold(plugin, [bad], empty={"NCAA Football"}) == {}


# ---------------------------------------------------------------------------
# Identity: a carried row must collide with the same game in a later fetch
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cfbd_id", [401520413, None])
def test_gamerow_marker_matches_cache_row_marker(plugin, cfbd_id):
    """If these disagree, a held game that the fetch DOES list is carried as
    well, and apply sees two games with one channel (or two channels)."""
    row = _row(plugin, "Alabama", "Georgia", datetime(2026, 10, 10, 23, 30, tzinfo=UTC),
               cfbd_id=cfbd_id)
    game = plugin._gamerow_from_row(row)
    assert plugin._gamerow_marker(game) == plugin._build_marker_key(row)


# ---------------------------------------------------------------------------
# End to end through _action_refresh
# ---------------------------------------------------------------------------

class _Source:
    def __init__(self, label, prefix, games=(), exc=None):
        self.sport_label = label
        self.sport_prefix = prefix
        self._games = list(games)
        self._exc = exc

    def fetch_upcoming(self, days_ahead=7):
        if self._exc:
            raise self._exc
        return list(self._games)


def _game(home, away, start, cfbd_id):
    from dispatcharr_ranked_matchups.sources.base import GameRow
    return GameRow(sport_prefix="CFB", sport_label="NCAA Football", home=home, away=away,
                   rank_home=None, rank_away=None, start_time=start,
                   extra={"cfbd_id": cfbd_id, "game_id": cfbd_id})


def test_failed_source_keeps_its_games_and_rematches_them(plugin, refresh_env):
    """The 19:00 failure: CFBD down 30 min before kickoff."""
    write_prior, run, read = refresh_env
    kickoff = datetime.now(UTC) + timedelta(minutes=30)
    write_prior([_row(plugin, "Alabama", "Georgia", kickoff, cfbd_id=401, channel_id=5)])

    result = run([_Source("NCAA Football", "CFB", games=[])])

    assert result["status"] == "ok"
    games = read()["games"]
    assert [(g["home"], g["away"]) for g in games] == [("Alabama", "Georgia")]
    # Re-matched against the current feeds, not copied from the prior cache.
    assert games[0]["channel_id"] == 777
    assert games[0]["score"] == 9.1  # scoring kept from the refresh that scored it


def test_source_that_raises_keeps_its_games(plugin, refresh_env):
    write_prior, run, read = refresh_env
    kickoff = datetime.now(UTC) + timedelta(hours=3)
    write_prior([_row(plugin, "Alabama", "Georgia", kickoff, cfbd_id=401, channel_id=5)])

    run([_Source("NCAA Football", "CFB", exc=RuntimeError("525"))])

    assert [g["home"] for g in read()["games"]] == ["Alabama"]


def test_live_game_missing_from_a_healthy_fetch_is_kept(plugin, refresh_env):
    """The 20:26 failure: CFBD healthy, but it no longer lists the live game."""
    write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    write_prior([_row(plugin, "Alabama", "Georgia", now - timedelta(minutes=53),
                      cfbd_id=401, channel_id=5)])
    later = _game("Fresno State", "Boise State", now + timedelta(hours=2), 402)

    run([_Source("NCAA Football", "CFB", games=[later])])

    homes = sorted(g["home"] for g in read()["games"])
    assert homes == ["Alabama", "Fresno State"]


def test_upcoming_game_dropped_by_a_healthy_source_is_not_kept(plugin, refresh_env):
    """Guard: the hold must not freeze the slate. A not-yet-started game the
    source stopped listing (rescheduled out of the window, curated out) goes."""
    write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    write_prior([_row(plugin, "Alabama", "Georgia", now + timedelta(hours=1),
                      cfbd_id=401, channel_id=5)])
    other = _game("Fresno State", "Boise State", now + timedelta(hours=2), 402)

    run([_Source("NCAA Football", "CFB", games=[other])])

    assert [g["home"] for g in read()["games"]] == ["Fresno State"]


def test_live_game_still_listed_is_not_duplicated(plugin, refresh_env):
    """Sources that DO list in-progress games (the ESPN family) must not get the
    game twice: once fetched, once carried."""
    write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    start = now - timedelta(minutes=53)
    write_prior([_row(plugin, "Alabama", "Georgia", start, cfbd_id=401, channel_id=5)])

    run([_Source("NCAA Football", "CFB", games=[_game("Alabama", "Georgia", start, 401)])])

    assert [g["home"] for g in read()["games"]] == ["Alabama"]


def test_live_game_is_never_benched_by_the_cap(plugin, refresh_env):
    """A listed live game that sorts below max_games stays applied.

    The other games are Favorites, so they sort ahead of it and are all
    forced onto the list; with max_games=1 the live game lands at index 3,
    which _split_applied would bench (and apply would delete) without the hold.
    They kick off five minutes before it so the today-first key ties."""
    write_prior, run, read = refresh_env
    start = datetime.now(UTC) - timedelta(minutes=53)
    write_prior([_row(plugin, "Alabama", "Georgia", start, cfbd_id=401, channel_id=5)])
    others = [_game(f"Home{i}", f"Away{i}", start - timedelta(minutes=5), 500 + i)
              for i in range(3)]

    run([_Source("NCAA Football", "CFB",
                 games=others + [_game("Alabama", "Georgia", start, 401)])],
        max_games=1, curation_preset="manual", favorites="Home0, Home1, Home2")

    cache = read()
    assert "Alabama" in [g["home"] for g in cache["games"]]
    assert "Alabama" not in [g["home"] for g in cache.get("bench") or []]


def test_next_game_of_a_series_waits_while_the_current_one_is_on_air(plugin, refresh_env):
    """One channel per matchup (_dedup_series_games). Game 1 is live and held;
    the source now lists only Game 2 (tomorrow). Game 2 must not get a second
    channel until Game 1 has been reaped."""
    write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    write_prior([_row(plugin, "Alabama", "Georgia", now - timedelta(minutes=53),
                      cfbd_id=401, channel_id=5)])
    game2 = _game("Georgia", "Alabama", now + timedelta(days=1), 403)  # home side swapped

    run([_Source("NCAA Football", "CFB", games=[game2])])

    games = read()["games"]
    assert [(g["home"], g["extra"]["cfbd_id"]) for g in games] == [("Alabama", 401)]


def test_empty_rematch_keeps_the_previous_match(plugin, refresh_env, monkeypatch):
    """If the carried game's re-match finds nothing, the old match stays: an
    unmatched low-score row is skipped by apply and its channel deleted."""
    import importlib
    matcher = importlib.import_module("dispatcharr_ranked_matchups.matcher")
    write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    write_prior([_row(plugin, "Alabama", "Georgia", now - timedelta(minutes=53),
                      cfbd_id=401, channel_id=5)])
    monkeypatch.setattr(matcher, "match_games_to_channels",
                        lambda scored, *_a, **_kw: [matcher.MatchResult(game_index=i)
                                                    for i in range(len(scored))])

    run([_Source("NCAA Football", "CFB", games=[])])

    (game,) = read()["games"]
    assert game["channel_id"] == 5 and game["channel_ids"] == [5]


def test_raised_source_is_an_outage_even_beside_a_healthy_sibling(plugin, refresh_env):
    """Two sources share one label; one raises, one returns games. The failed
    one's upcoming row is kept."""
    write_prior, run, read = refresh_env
    now = datetime.now(UTC)
    write_prior([_row(plugin, "Alabama", "Georgia", now + timedelta(hours=3),
                      cfbd_id=401, channel_id=5)])
    sibling = _game("Fresno State", "Boise State", now + timedelta(hours=2), 402)

    run([_Source("NCAA Football", "CFB", games=[sibling]),
         _Source("NCAA Football", "CFB", exc=RuntimeError("525"))])

    assert sorted(g["home"] for g in read()["games"]) == ["Alabama", "Fresno State"]


class TestReapDoesNotBackfillHeldRows:
    """A held row sat outside max_games; its expiry frees no slot."""

    def _reap(self, plugin, monkeypatch, tmp_path, games, bench):
        monkeypatch.setattr(plugin, "CACHE_PATH", str(tmp_path / "cache.json"))
        (tmp_path / "cache.json").write_text(json.dumps({"games": games, "bench": bench}))
        monkeypatch.setattr(plugin, "_action_apply", lambda _s: {"status": "ok"})
        plugin._action_reap({"remove_finished_after_minutes": 30})
        return json.loads((tmp_path / "cache.json").read_text())

    def _rows(self, plugin, held):
        now = datetime.now(UTC)
        done = _row(plugin, "Alabama", "Georgia", now - timedelta(hours=20), cfbd_id=401)
        if held:
            done[plugin._HELD_ON_AIR] = True
        bench = [_row(plugin, "Bench", "Team", now + timedelta(hours=2), cfbd_id=499, channel_id=9)]
        return [done], bench

    def test_held_row_expiry_promotes_nothing(self, plugin, monkeypatch, tmp_path):
        games, bench = self._rows(plugin, held=True)
        cache = self._reap(plugin, monkeypatch, tmp_path, games, bench)
        assert cache["games"] == [] and len(cache["bench"]) == 1

    def test_capped_row_expiry_still_promotes(self, plugin, monkeypatch, tmp_path):
        games, bench = self._rows(plugin, held=False)
        cache = self._reap(plugin, monkeypatch, tmp_path, games, bench)
        assert [g["home"] for g in cache["games"]] == ["Bench"]
