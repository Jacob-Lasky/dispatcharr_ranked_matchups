"""UEFA EURO group -> knockout cross-source chain (#78).

Mirrors the WC 2026 chain (#53) for EURO 2028: 24 teams, six groups of
four, top two plus the four best third-placed teams into a round of 16.

Every bracket expectation below is a LITERAL copied from the governing
document or from FD.org's record of EURO 2024, never read back from the
constants under test:

  * Regulations of the UEFA European Football Championship 2026-28,
    Article 21 (paragraphs 21.03-21.07), in force 29 July 2026.
  * football-data.org `GET /v4/competitions/EC/matches?season=2024`,
    fetched read-only on 2026-10-11 and trimmed into
    tests/fixtures/fd_ec_2024_matches.json. EURO 2024 used the same
    format and bracket, so its real group results must seed its real
    round of 16, and its real knockout results must walk the real
    quarter-finals, semi-finals and final.
"""
import copy
import itertools
import json
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import pytest

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "fd_ec_2024_matches.json")


def _ec_2024_matches() -> List[Dict[str, Any]]:
    with open(FIXTURE) as fh:
        return json.load(fh)["matches"]


def _euro_pair(matches):
    from dispatcharr_ranked_matchups.sources.soccer import (
        GroupStageSoccerSource, KnockoutSoccerSource,
    )
    groups = GroupStageSoccerSource("euros", fd_api_key="x", odds_api_key="")
    knockout = KnockoutSoccerSource("euros", fd_api_key="x", odds_api_key="")
    groups._all_matches_cache = matches
    knockout._all_matches_cache = matches
    return groups, knockout


def _reopen_matchday_3(matches, start: datetime):
    """EURO 2024 frozen just before its final group matchday: every MD3
    group game back to SCHEDULED with no score, kicking off at `start`.
    The knockout fixtures keep their real team names, which is the
    partially-filled bracket FD.org serves on the last group matchday."""
    out = copy.deepcopy(matches)
    for m in out:
        if m["stage"] == "GROUP_STAGE" and m["matchday"] == 3:
            m["status"] = "SCHEDULED"
            m["score"] = {"winner": None, "duration": "REGULAR",
                          "fullTime": {"home": None, "away": None}}
            m["utcDate"] = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    return out


# Article 21.03, verbatim slots ("W" group winner, "R" runner-up,
# "3" third-placed team), Match 1 .. Match 8.
ARTICLE_21_03 = [
    ("WB", "3A/D/E/F"),
    ("WA", "RC"),
    ("WF", "3A/B/C"),
    ("RD", "RE"),
    ("WE", "3A/B/C/D"),
    ("WD", "RF"),
    ("WC", "3D/E/F"),
    ("RA", "RB"),
]

# Article 21.04, every row as printed: qualifying third-place groups ->
# opponents of WB (Match 1), WC (Match 7), WE (Match 5), WF (Match 3).
ARTICLE_21_04 = {
    "ABCD": ("3A", "3D", "3B", "3C"),
    "ABCE": ("3A", "3E", "3B", "3C"),
    "ABCF": ("3A", "3F", "3B", "3C"),
    "ABDE": ("3D", "3E", "3A", "3B"),
    "ABDF": ("3D", "3F", "3A", "3B"),
    "ABEF": ("3E", "3F", "3B", "3A"),
    "ACDE": ("3E", "3D", "3C", "3A"),
    "ACDF": ("3F", "3D", "3C", "3A"),
    "ACEF": ("3E", "3F", "3C", "3A"),
    "ADEF": ("3E", "3F", "3D", "3A"),
    "BCDE": ("3E", "3D", "3B", "3C"),
    "BCDF": ("3F", "3D", "3C", "3B"),
    "BCEF": ("3F", "3E", "3C", "3B"),
    "BDEF": ("3F", "3E", "3D", "3B"),
    "CDEF": ("3F", "3E", "3D", "3C"),
}


def _slot_text(slot) -> str:
    kind, payload = slot
    if kind == "group_winner":
        return f"W{payload}"
    if kind == "group_runner_up":
        return f"R{payload}"
    return "3" + "/".join(sorted(payload))


class TestEuroBracketData:
    def test_round_of_16_matches_article_21_03(self):
        from dispatcharr_ranked_matchups.sources.soccer import _CHAIN_BRACKETS
        ec = _CHAIN_BRACKETS["EC"]
        assert ec.entry_stage == "LAST_16"
        assert ec.groups == frozenset("ABCDEF")
        got = [(_slot_text(h), _slot_text(a)) for h, a in ec.entry_pairings]
        assert got == ARTICLE_21_03

    def test_later_rounds_match_article_21_05_to_21_07(self):
        # QF n = winners of R16 matches 2n-1 and 2n; SF n = winners of
        # QF 2n-1 and 2n; final = winners of SF 1 and 2 (0-based below).
        from dispatcharr_ranked_matchups.sources.soccer import _CHAIN_BRACKETS
        assert _CHAIN_BRACKETS["EC"].downstream == (
            ("QUARTER_FINALS", [(0, 1), (2, 3), (4, 5), (6, 7)]),
            ("SEMI_FINALS", [(0, 1), (2, 3)]),
            ("FINAL", [(0, 1)]),
        )

    def test_third_placer_table_matches_article_21_04_row_for_row(self):
        from dispatcharr_ranked_matchups.sources.soccer import _CHAIN_BRACKETS
        table = _CHAIN_BRACKETS["EC"].third_placer_table
        assert set(table) == {frozenset(c) for c in itertools.combinations("ABCDEF", 4)}
        assert len(table) == 15
        # Article 21.04 column -> 0-based R16 match index (Match 1, 7, 5, 3).
        column_match = (0, 6, 4, 2)
        for groups, opponents in ARTICLE_21_04.items():
            expected = {(m, "away", o[1]) for m, o in zip(column_match, opponents)}
            assert set(table[frozenset(groups)]) == expected, groups

    def test_every_third_placer_row_respects_its_slot(self):
        # Independent of the transcription: each assigned team's group is
        # one Article 21.03 allows in that match, and each qualifying group
        # is used exactly once.
        from dispatcharr_ranked_matchups.sources.soccer import _CHAIN_BRACKETS
        ec = _CHAIN_BRACKETS["EC"]
        allowed = {
            i: away[1] for i, (_home, away) in enumerate(ec.entry_pairings)
            if away[0] == "best_third"
        }
        for key, row in ec.third_placer_table.items():
            assert sorted(letter for _, _, letter in row) == sorted(key)
            assert {idx for idx, _, _ in row} == set(allowed)
            for idx, side, letter in row:
                assert side == "away"
                assert letter in allowed[idx]

    @pytest.mark.parametrize("code", ["WC", "EC"])
    def test_bracket_shape_is_consistent_with_its_group_context(self, code):
        # Each group's winner and runner-up appear exactly once, the number
        # of third-place slots equals the context's best_third_place_count,
        # and every later round halves the one before it.
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        from dispatcharr_ranked_matchups.sources.soccer import _CHAIN_BRACKETS
        b = _CHAIN_BRACKETS[code]
        slots = [s for pair in b.entry_pairings for s in pair]
        fixed = [(k, p) for k, p in slots if k != "best_third"]
        assert sorted(fixed) == sorted(
            [("group_winner", g) for g in b.groups]
            + [("group_runner_up", g) for g in b.groups]
        )
        n_third = sum(1 for k, _ in slots if k == "best_third")
        assert n_third == LEAGUE_CONTEXTS[f"{code}_GS"].best_third_place_count
        prev = len(b.entry_pairings)
        for _stage, pairings in b.downstream:
            assert len(pairings) * 2 == prev
            assert sorted(i for pair in pairings for i in pair) == list(range(prev))
            prev = len(pairings)
        assert prev == 1


def _uefa_final_state(groups):
    """EURO 2024's group stage as UEFA ranked it. Denmark and Slovenia
    finished Group C level on points, goal difference, goals scored and
    their 1-1 head-to-head; UEFA put Denmark second on disciplinary points
    (Article 20.01 g), which FD.org's results cannot express. The
    production tiebreaker (points, GD, GF) leaves them level and keeps
    insertion order, so list Denmark first to apply UEFA's decision."""
    state = dict(groups.initial_state())
    teams = dict(state["_teams"])
    order = sorted(teams, key=lambda t: (t != "Denmark", t != "Slovenia"))
    state["_teams"] = {t: teams[t] for t in order}
    return state


class TestEuro2024Backtest:
    """EURO 2024's real group results, through the production standings
    code, must seed EURO 2024's real round of 16."""

    # FD.org's EURO 2024 LAST_16 fixtures, (homeTeam, awayTeam), in
    # Article 21.03 match order.
    REAL_R16 = [
        ("Spain", "Georgia"),          # WB vs 3F
        ("Germany", "Denmark"),        # WA vs RC
        ("Portugal", "Slovenia"),      # WF vs 3C
        ("France", "Belgium"),         # RD vs RE
        ("Romania", "Netherlands"),    # WE vs 3D
        ("Austria", "Turkey"),         # WD vs RF
        ("England", "Slovakia"),       # WC vs 3E
        ("Switzerland", "Italy"),      # RA vs RB
    ]

    def test_seed_reproduces_the_real_round_of_16(self):
        matches = _ec_2024_matches()
        groups, _ = _euro_pair(matches)
        seed = groups._build_bracket_seed(_uefa_final_state(groups))
        entry = seed["stages"][0]
        assert entry["stage"] == "LAST_16"
        assert [(t["home"], t["away"]) for t in entry["ties"]] == self.REAL_R16
        fd_r16 = {
            (m["homeTeam"]["name"], m["awayTeam"]["name"])
            for m in matches if m["stage"] == "LAST_16"
        }
        assert set(self.REAL_R16) == fd_r16

    def test_results_alone_differ_only_by_the_disciplinary_tiebreak(self):
        # Without UEFA's fair-play decision the only possible difference is
        # Denmark and Slovenia trading places: same four third-place groups
        # (C D E F), so the rest of the bracket is untouched.
        groups, _ = _euro_pair(_ec_2024_matches())
        seed = groups._build_bracket_seed(groups.initial_state())
        got = [(t["home"], t["away"]) for t in seed["stages"][0]["ties"]]
        swap = {"Denmark": "Slovenia", "Slovenia": "Denmark"}
        swapped = [(swap.get(h, h), swap.get(a, a)) for h, a in self.REAL_R16]
        assert got in (self.REAL_R16, swapped)

    def test_real_results_walk_the_real_later_rounds(self):
        # Resolve every placeholder with EURO 2024's actual winners and
        # compare each round's pairings to FD.org's fixtures for it.
        matches = _ec_2024_matches()
        groups, _ = _euro_pair(matches)
        seed = groups._build_bracket_seed(_uefa_final_state(groups))

        def winner(m):
            return m["homeTeam"]["name"] if m["score"]["winner"] == "HOME_TEAM" \
                else m["awayTeam"]["name"]

        winners_by_pair = {
            frozenset({m["homeTeam"]["name"], m["awayTeam"]["name"]}): winner(m)
            for m in matches if m["stage"] != "GROUP_STAGE"
        }
        resolved: Dict[str, List[Tuple[str, str]]] = {
            "LAST_16": [(t["home"], t["away"]) for t in seed["stages"][0]["ties"]],
        }
        for stage_spec in seed["stages"][1:]:
            pairs = []
            for tie in stage_spec["ties"]:
                sides = []
                for side in (tie["home"], tie["away"]):
                    feeder_stage, idx = tie["feeds_from"][side]
                    sides.append(winners_by_pair[frozenset(resolved[feeder_stage][idx])])
                pairs.append(tuple(sides))
            resolved[stage_spec["stage"]] = pairs

        for stage in ("QUARTER_FINALS", "SEMI_FINALS", "FINAL"):
            fd = {
                frozenset({m["homeTeam"]["name"], m["awayTeam"]["name"]})
                for m in matches if m["stage"] == stage
            }
            assert {frozenset(p) for p in resolved[stage]} == fd, stage
        assert resolved["FINAL"] == [("Spain", "England")]


class TestEuroChainToggle:
    def test_active_on_final_matchday_with_partly_filled_bracket(self):
        # The pre-#78 toggle switched the chain off as soon as FD.org had
        # ANY knockout fixture with real teams, which it has on the last
        # group matchday (2A v 2B is known while groups C-F still play).
        matches = _reopen_matchday_3(
            _ec_2024_matches(), datetime(2024, 6, 25, 16, tzinfo=timezone.utc),
        )
        groups, knockout = _euro_pair(matches)
        assert knockout._fetch_bracket_games(), "fixture should carry real R16 teams"
        groups.set_paired_knockout_source(knockout)
        chain = groups.cross_source_chain()
        assert chain is not None
        assert chain[0] is knockout

    def test_off_once_every_group_game_is_finished(self):
        # Between tournaments FD.org serves the completed EURO 2024 season.
        groups, knockout = _euro_pair(_ec_2024_matches())
        groups.set_paired_knockout_source(knockout)
        assert groups.cross_source_chain() is None

    def test_off_when_unpaired(self):
        matches = _reopen_matchday_3(
            _ec_2024_matches(), datetime(2024, 6, 25, 16, tzinfo=timezone.utc),
        )
        groups, _ = _euro_pair(matches)
        assert groups.cross_source_chain() is None

    def test_off_when_the_season_is_not_the_brackets_format(self):
        # Six EURO groups fed to the World Cup source: the 12-group WC 2026
        # bracket must not be seeded from them.
        from dispatcharr_ranked_matchups.sources.soccer import (
            GroupStageSoccerSource, KnockoutSoccerSource,
        )
        matches = _reopen_matchday_3(
            _ec_2024_matches(), datetime(2024, 6, 25, 16, tzinfo=timezone.utc),
        )
        wc_groups = GroupStageSoccerSource("world_cup", fd_api_key="x", odds_api_key="")
        wc_ko = KnockoutSoccerSource("world_cup", fd_api_key="x", odds_api_key="")
        wc_groups._all_matches_cache = matches
        wc_ko._all_matches_cache = matches
        wc_groups.set_paired_knockout_source(wc_ko)
        assert wc_groups.cross_source_chain() is None

    def test_off_before_the_group_draw(self):
        # FD.org lists group fixtures with null teams before the draw.
        matches = _reopen_matchday_3(
            _ec_2024_matches(), datetime(2028, 6, 20, 16, tzinfo=timezone.utc),
        )
        for m in matches:
            m["homeTeam"] = {"name": None}
            m["awayTeam"] = {"name": None}
        groups, knockout = _euro_pair(matches)
        groups.set_paired_knockout_source(knockout)
        assert groups.cross_source_chain() is None


class TestSeedNeverMisalignsFeeders:
    def test_unfillable_entry_slot_raises_instead_of_dropping_the_tie(self):
        # A dropped entry tie would shift every later feeds_from index onto
        # the wrong match. Group A with one known team cannot supply a
        # runner-up for Match 8 (RA vs RB).
        matches = _ec_2024_matches()
        groups, _ = _euro_pair(matches)
        state = groups.initial_state()
        state = dict(state)
        state["_team_group"] = {
            t: g for t, g in state["_team_group"].items()
            if g != "GROUP_A" or t == "Germany"
        }
        with pytest.raises(ValueError, match="LAST_16 tie 8"):
            groups._build_bracket_seed(state)


class TestEcGsCarriesKnockoutCascade:
    def test_ec_gs_bands(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        labels = [lbl for _, lbl, _ in LEAGUE_CONTEXTS["EC_GS"].thresholds]
        assert labels == [
            "advance", "round_of_16", "quarterfinal", "semifinal", "final", "winner",
        ]

    def test_ec_and_ec_gs_share_knockout_weights(self):
        from dispatcharr_ranked_matchups.scoring import LEAGUE_CONTEXTS
        ko = {lbl: w for _, lbl, w in LEAGUE_CONTEXTS["EC"].thresholds}
        gs = {lbl: w for _, lbl, w in LEAGUE_CONTEXTS["EC_GS"].thresholds if lbl != "advance"}
        assert ko == gs == {
            "round_of_16": 1.0, "quarterfinal": 2.5, "semifinal": 4.0,
            "final": 6.0, "winner": 10.0,
        }


class TestEuroGroupGameGetsKnockoutLeverage:
    """Acceptance for #78: a EURO group game, built the way production
    builds it (GroupStageSoccerSource.fetch_upcoming), scores nonzero
    leverage on R16+ bands when the chain is active."""

    KNOCKOUT_LABELS = ("round_of_16", "quarterfinal", "semifinal", "final", "winner")

    def _md3_row(self, monkeypatch, groups, matches, home, away):
        fixture = next(
            m for m in matches
            if m["stage"] == "GROUP_STAGE" and m["homeTeam"]["name"] == home
            and m["awayTeam"]["name"] == away
        )
        monkeypatch.setattr(groups, "_fetch_fixtures", lambda days_ahead: [fixture])
        monkeypatch.setattr(groups, "_fetch_closeness", lambda: {})
        rows = groups.fetch_upcoming(days_ahead=7)
        assert len(rows) == 1
        assert rows[0].extra["fd_competition_code"] == "EC_GS"
        return rows[0]

    def _score(self, monkeypatch, paired: bool):
        from dispatcharr_ranked_matchups.scoring import (
            LEAGUE_CONTEXTS, compute_match_importance,
        )
        start = datetime.now(timezone.utc) + timedelta(days=1)
        matches = _reopen_matchday_3(_ec_2024_matches(), start)
        groups, knockout = _euro_pair(matches)
        if paired:
            groups.set_paired_knockout_source(knockout)
        # Group E at EURO 2024 finished with all four teams on 4 points:
        # its MD3 game decides who wins the group, so who meets whom.
        row = self._md3_row(monkeypatch, groups, matches, "Slovakia", "Romania")
        return compute_match_importance(
            groups, row, LEAGUE_CONTEXTS["EC_GS"], n_sims=400,
            rng=random.Random(78),
        )

    def test_chain_active_gives_nonzero_knockout_leverage(self, monkeypatch):
        _pts, notes, hit = self._score(monkeypatch, paired=True)
        knockout_hit = [lbl for lbl in hit if lbl in self.KNOCKOUT_LABELS]
        assert {"round_of_16", "quarterfinal", "semifinal", "final", "winner"} <= set(
            knockout_hit), (notes, hit)
        assert any("quarterfinal:" in n for n in notes), notes

    def test_unpaired_source_scores_advance_only(self, monkeypatch):
        _pts, _notes, hit = self._score(monkeypatch, paired=False)
        assert not [lbl for lbl in hit if lbl in self.KNOCKOUT_LABELS]
        assert "advance" in hit


class TestPluginWiresEuroChain:
    @pytest.mark.parametrize("setting, fd_code", [
        ("enable_euros", "EC"),
        ("enable_world_cup", "WC"),
    ])
    def test_toggle_pairs_group_and_knockout_sources(self, plugin, setting, fd_code):
        sources = plugin._build_sources({setting: True, "football_data_api_key": "x"})
        comp = [s for s in sources if getattr(getattr(s, "config", None), "fd_code", None) == fd_code]
        ko = [s for s in comp if type(s).__name__ == "KnockoutSoccerSource"]
        gs = [s for s in comp if type(s).__name__ == "GroupStageSoccerSource"]
        assert len(ko) == 1 and len(gs) == 1
        assert getattr(gs[0], "_paired_knockout", None) is ko[0]


def _state_from_results(src, groups: Dict[str, List[Tuple[str, str, int, int]]], order=None):
    """A GroupStageState built the way initial_state builds one: zero rows
    for every team, then each result folded in through `_mutate_apply` and
    recorded in `_results`. `order` fixes `_teams` insertion order (the
    last-resort tiebreak) so a test can make the OLD ranking pick wrong."""
    team_group: Dict[str, str] = {}
    for grp, games in groups.items():
        for home, away, _hg, _ag in games:
            team_group.setdefault(home, grp)
            team_group.setdefault(away, grp)
    names = order or list(team_group)
    state: Dict[str, Any] = {
        "_applied": frozenset(),
        "_team_group": team_group,
        "_teams": {t: {"played": 0, "points": 0, "gf": 0, "ga": 0} for t in names},
        "_results": tuple(r for games in groups.values() for r in games),
    }
    for home, away, hg, ag in state["_results"]:
        src._mutate_apply(state, home, away, hg, ag)
    return state


def _euro_groups_source():
    from dispatcharr_ranked_matchups.sources.soccer import GroupStageSoccerSource
    return GroupStageSoccerSource("euros", fd_api_key="x", odds_api_key="")


def _table(src, state, group="GROUP_A"):
    return [t for t, _row in src._compute_group_standings(state).by_group[group]]


class TestHeadToHeadTiebreakers:
    """UEFA EURO 2026-28 regulations Article 20.01: teams level on points
    are separated by their matches against each other before overall goal
    difference. The pre-#78 ranking went straight to overall GD / GF."""

    def test_head_to_head_beats_better_overall_goal_difference(self):
        src = _euro_groups_source()
        state = _state_from_results(src, {"GROUP_A": [
            ("X", "Y", 1, 0), ("Z", "X", 1, 0), ("X", "W", 1, 0),
            ("Y", "Z", 3, 0), ("Y", "W", 3, 0), ("Z", "W", 0, 0),
        ]})
        # X and Y both 6 points; Y +5 overall, X +1, but X won their game.
        assert _table(src, state) == ["X", "Y", "Z", "W"]

    def test_three_way_tie_uses_the_mini_table(self):
        # A, B, C each beat D and beat one another in a circle. Among the
        # three: A +2, C 0, B -2. Overall GD is +3 for all and B scores the
        # most goals, so the old ranking put B first.
        src = _euro_groups_source()
        state = _state_from_results(src, {"GROUP_A": [
            ("A", "B", 3, 0), ("B", "C", 1, 0), ("C", "A", 1, 0),
            ("A", "D", 1, 0), ("B", "D", 5, 0), ("C", "D", 3, 0),
        ]})
        assert _table(src, state) == ["A", "C", "B", "D"]

    def test_still_level_teams_are_decided_by_their_own_match(self):
        # Criterion d: all four on 4 points; the four-team mini table (all
        # group games) leaves A and C level on every count, and reapplying
        # it to the A-C match alone puts A (1-0 winner) ahead. C is listed
        # first so the old insertion-order fallback would pick C.
        src = _euro_groups_source()
        state = _state_from_results(src, {"GROUP_A": [
            ("A", "B", 0, 0), ("A", "C", 1, 0), ("A", "D", 0, 1),
            ("B", "C", 0, 1), ("B", "D", 2, 0), ("C", "D", 0, 0),
        ]}, order=["C", "B", "A", "D"])
        assert _table(src, state) == ["B", "A", "C", "D"]

    def test_unbreakable_tie_falls_back_to_overall_goals(self):
        # Criteria e-f: A, B, C level on points and on every head-to-head
        # count; overall goal difference then decides (D's games differ).
        src = _euro_groups_source()
        state = _state_from_results(src, {"GROUP_A": [
            ("A", "B", 1, 1), ("B", "C", 1, 1), ("C", "A", 1, 1),
            ("A", "D", 3, 0), ("B", "D", 2, 0), ("C", "D", 1, 0),
        ]}, order=["C", "B", "A", "D"])
        assert _table(src, state) == ["A", "B", "C", "D"]

    def test_simulated_results_feed_the_tiebreak(self):
        # apply_result must record what it applies, or every simulated
        # season would rank on overall GD again.
        from dispatcharr_ranked_matchups.sources.base import GameRow, MatchResult
        src = _euro_groups_source()
        state = _state_from_results(src, {"GROUP_A": [
            ("Z", "X", 1, 0), ("X", "W", 1, 0),
            ("Y", "Z", 3, 0), ("Y", "W", 3, 0), ("Z", "W", 0, 0),
        ]})
        row = GameRow(sport_prefix="EURO", sport_label="UEFA European Championship",
                      home="X", away="Y", rank_home=None, rank_away=None,
                      start_time=None, extra={"fd_id": 99})
        after = src.apply_result(state, row, MatchResult(home_goals=1, away_goals=0))
        assert after["_results"][-1] == ("X", "Y", 1, 0)
        assert state["_results"] == after["_results"][:-1]  # input untouched
        assert _table(src, after) == ["X", "Y", "Z", "W"]


class TestBestThirdRanking:
    # Two third-placers level on points (3), goal difference (0) and goals
    # (2): "Aaa" drew three times, "Zed" won once. Article 19.04 d ranks
    # by wins next, so Zed first; the name fallback would pick Aaa.
    GROUPS = {
        "GROUP_A": [
            ("A1", "A2", 1, 0), ("A1", "Aaa", 1, 1), ("A1", "A4", 1, 0),
            ("A2", "Aaa", 1, 1), ("A2", "A4", 1, 0), ("Aaa", "A4", 0, 0),
        ],
        "GROUP_B": [
            ("B1", "B2", 1, 0), ("B1", "Zed", 1, 0), ("B1", "B4", 1, 0),
            ("B2", "Zed", 1, 0), ("B2", "B4", 1, 0), ("Zed", "B4", 2, 0),
        ],
    }

    def test_euro_ranks_level_third_placers_by_wins(self):
        src = _euro_groups_source()
        standings = src._compute_group_standings(_state_from_results(src, self.GROUPS))
        assert [t for t, _ in standings.best_third_order] == ["Zed", "Aaa"]

    def test_world_cup_does_not_count_wins(self):
        from dispatcharr_ranked_matchups.sources.soccer import GroupStageSoccerSource
        src = GroupStageSoccerSource("world_cup", fd_api_key="x", odds_api_key="")
        standings = src._compute_group_standings(_state_from_results(src, self.GROUPS))
        assert [t for t, _ in standings.best_third_order] == ["Aaa", "Zed"]


class TestChainNeedsASeedableFeed:
    def test_group_with_one_named_team_keeps_the_chain_off(self, caplog):
        # Codex review, round 1: letters A-F all present, but Group A has a
        # single named team, so no runner-up for Match 8. The chain must stay
        # off (group games keep `advance` leverage) rather than switch on and
        # raise inside every simulated season.
        matches = _reopen_matchday_3(
            _ec_2024_matches(), datetime(2024, 6, 25, 16, tzinfo=timezone.utc),
        )
        for m in matches:
            if m.get("group") == "GROUP_A":
                for side in ("homeTeam", "awayTeam"):
                    if m[side]["name"] != "Germany":
                        m[side] = {"name": None}
        groups, knockout = _euro_pair(matches)
        groups.set_paired_knockout_source(knockout)
        assert groups.cross_source_chain() is None
        assert groups.cross_source_chain() is None
        warnings = [r for r in caplog.records if "knockout leverage is off" in r.getMessage()]
        assert len(warnings) == 1

    def test_group_with_a_tbd_team_keeps_the_chain_off(self):
        # Codex review, round 2: three named teams per group are enough to
        # fill every slot, but a three-team group simulated against full
        # ones skews both the table and the best-third race.
        matches = _reopen_matchday_3(
            _ec_2024_matches(), datetime(2024, 6, 25, 16, tzinfo=timezone.utc),
        )
        for m in matches:
            for side in ("homeTeam", "awayTeam"):
                if m[side]["name"] == "Scotland":
                    m[side] = {"name": None}
        groups, knockout = _euro_pair(matches)
        groups.set_paired_knockout_source(knockout)
        assert groups.cross_source_chain() is None


def _play_out(knockout, state, winner_is_home=True):
    """Drain a seeded bracket with 1-0 results; return games played per stage."""
    from dispatcharr_ranked_matchups.sources.base import MatchResult
    played: Dict[str, int] = {}
    result = MatchResult(home_goals=1, away_goals=0) if winner_is_home \
        else MatchResult(home_goals=0, away_goals=1)
    while True:
        rem = knockout.remaining_matches(state)
        if not rem:
            break
        for m in rem:
            played[m.extra["stage"]] = played.get(m.extra["stage"], 0) + 1
            state = knockout.apply_result(state, m, result)
    return played, state


class TestSeededBracketPlaysToTheFinal:
    """Codex review, round 2: a tie created mid-simulation got no leg count,
    defaulted to two legs, and never completed, so a seeded bracket stopped
    one round after its entry round (EURO: no semi-final, final or winner;
    WC 2026: nothing after the round of 16)."""

    def test_euro_seed_plays_every_round(self):
        matches = _ec_2024_matches()
        groups, knockout = _euro_pair(matches)
        state = knockout.seed_from_chain(groups._build_bracket_seed(_uefa_final_state(groups)))
        played, end = _play_out(knockout, state)
        assert played == {"LAST_16": 8, "QUARTER_FINALS": 4, "SEMI_FINALS": 2, "FINAL": 1}
        # Home side wins every game: Match 1's home team (Spain, WB) wins
        # QF1, SF1 and the final.
        assert knockout.terminal_outcomes(end)["Spain"] == [
            "round_of_16", "quarterfinal", "semifinal", "final", "winner",
        ]

    def test_world_cup_seed_plays_every_round(self):
        from dispatcharr_ranked_matchups.sources.soccer import (
            GroupStageSoccerSource, KnockoutSoccerSource,
        )
        groups = GroupStageSoccerSource("world_cup", fd_api_key="x", odds_api_key="")
        knockout = KnockoutSoccerSource("world_cup", fd_api_key="x", odds_api_key="")
        letters = "ABCDEFGHIJKL"
        team_group = {f"G{g}{n}": f"GROUP_{g}" for g in letters for n in (1, 2, 3, 4)}
        teams = {}
        for g in letters:
            for n, pts in ((1, 9), (2, 6), (3, 3 + letters.index(g) % 2), (4, 0)):
                teams[f"G{g}{n}"] = {"played": 3, "points": pts, "gf": pts, "ga": 0}
        state = {"_applied": frozenset(), "_team_group": team_group, "_teams": teams,
                 "_results": ()}
        seeded = knockout.seed_from_chain(groups._build_bracket_seed(state))
        played, _end = _play_out(knockout, seeded, winner_is_home=False)
        assert played == {"LAST_32": 16, "LAST_16": 8, "QUARTER_FINALS": 4,
                          "SEMI_FINALS": 2, "FINAL": 1}
