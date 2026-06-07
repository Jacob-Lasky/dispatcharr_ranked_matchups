"""Tests for scoring.py: pure logic, no Django, no network."""

from dispatcharr_ranked_matchups.scoring import (
    GameSignals,
    Weights,
    LEAGUE_CONTEXTS,
    LeagueContext,
    TEAM_QUALIFIER_TOKENS,
    TEAM_SUFFIX_TOKENS,
    _compress_to_10,
    build_impact_narratives,
    compute_match_importance,
    format_channel_name,
    match_favorites,
    pick_tagline,
    render_favorite_impact,
    score_game,
    strip_team_suffix,
)


class TestCompressTo10:
    """Anchor values from the docstring: if these drift, the score everyone
    sees in the UI shifts. Pin them."""

    def test_zero_or_negative(self):
        assert _compress_to_10(0) == 0.0
        assert _compress_to_10(-5) == 0.0

    def test_known_anchors(self):
        # Knee = 16.0 → 10 * tanh(raw/16). Tight tolerances to catch any
        # silent change to _FINAL_KNEE. Anchors doubled vs the knee=8.0
        # era so a "typical good game" (raw 20-30) lands at 7.7-8.6
        # instead of saturating to 9.5+.
        assert round(_compress_to_10(2), 2) == 1.24
        assert round(_compress_to_10(4), 2) == 2.45
        assert round(_compress_to_10(8), 2) == 4.62
        assert round(_compress_to_10(16), 2) == 7.62
        assert round(_compress_to_10(24), 2) == 9.05
        assert round(_compress_to_10(32), 2) == 9.64

    def test_asymptotes_at_10_for_large_input(self):
        # tanh saturates to 1.0 in float64 well before raw=1000: that's
        # fine; the score is "10/10" in display either way.
        assert _compress_to_10(100) <= 10.0
        assert _compress_to_10(1000) <= 10.0
        # But a small raw should be well below 10.
        assert _compress_to_10(2) < 5.0


class TestMatchFavorites:
    def test_simple_match(self):
        assert match_favorites("Wrexham AFC", "Hull City AFC", ["Wrexham"]) == ["Wrexham"]

    def test_word_boundary_blocks_substring(self):
        # "Hull" must NOT match "Hull City" if we're searching for "Hull"
        # actually it should: "Hull City" has "City" as a TEAM_QUALIFIER_TOKEN
        assert match_favorites("Hull City AFC", "QPR", ["Hull"]) == ["Hull"]

    def test_qualifier_required_after_partial(self):
        # "UNC" should NOT match "UNC Pembroke" because Pembroke isn't a
        # qualifier token: it's a different school.
        assert match_favorites("UNC Pembroke", "Wofford", ["UNC"]) == []

    def test_qualifier_required_for_compound_name(self):
        # "North Carolina" inside "North Carolina A&T": A&T is the second
        # word, "&" is a qualifier token, so this matches A&T as the
        # trailing capitalized word's first char.
        # (Documenting actual behavior: "A&T" starts with "A", a single
        # capitalized letter that doesn't match the qualifier list, so the
        # match is rejected.)
        assert match_favorites(
            "North Carolina A&T", "Howard", ["North Carolina"]
        ) == []

    def test_full_team_name(self):
        # If the favorite IS the full name, no trailing word check applies.
        assert match_favorites("Wrexham AFC", "Hull City", ["Wrexham AFC"]) == ["Wrexham AFC"]

    def test_empty_favorites_returns_empty(self):
        assert match_favorites("Anyone", "Else", []) == []

    def test_case_insensitive(self):
        assert match_favorites("WREXHAM AFC", "Hull", ["wrexham"]) == ["wrexham"]


class TestSuffixTokens:
    def test_suffix_tokens_subset_of_qualifier_tokens(self):
        # The strip-set must be a subset of the qualifier set; otherwise
        # soccer's _lookup_odds can strip a suffix that the favorite-matcher
        # would have kept and trusted as a real second word.
        for tok in TEAM_SUFFIX_TOKENS:
            assert tok in TEAM_QUALIFIER_TOKENS, f"{tok!r} drifted"


class TestScoreGame:
    def test_both_top_5(self):
        signals = GameSignals(rank_a=1, rank_b=5)
        s = score_game(signals, Weights())
        assert "rank_pair" in s.breakdown
        assert s.breakdown["rank_pair"] > 0
        assert s.final > 0

    def test_favorite_flat_boost(self):
        # Track the default Weights.favorite value. Bumped to 6.0 in
        # the Phase A tuning bundle to surface favorite-involved games
        # ahead of title-race contenders.
        sig = GameSignals(favorite_match=["Wrexham"])
        s = score_game(sig, Weights())
        assert s.breakdown["favorite"] == 6.0

    def test_close_spread(self):
        sig = GameSignals(spread=0.5)
        s = score_game(sig, Weights())
        assert "close_game" in s.breakdown

    def test_unranked_vs_unranked_no_signal(self):
        # No rank, no favorite, no importance → empty breakdown, score 0.
        sig = GameSignals()
        s = score_game(sig, Weights())
        assert s.raw == 0.0
        assert s.final == 0.0

    def test_breakdown_sums_to_raw(self):
        sig = GameSignals(
            rank_a=2, rank_b=3,
            favorite_match=["Wrexham"],
            spread=2.0,
            importance_points=4.0,
        )
        s = score_game(sig, Weights())
        # raw must equal sum of breakdown contributions to within rounding
        breakdown_sum = round(sum(s.breakdown.values()), 2)
        assert abs(s.raw - breakdown_sum) < 0.01


class TestEffectiveCloseness:
    """B.3: the score_game helper that unifies the closeness (B.3 soccer
    path) and spread (NCAAF/NCAAM legacy path) signals into a single
    [0, 1] coinflip-ness measure."""

    def test_closeness_preferred_when_present(self):
        from dispatcharr_ranked_matchups.scoring import _effective_closeness
        # closeness=0.85 takes precedence over a contradicting spread.
        assert _effective_closeness(0.85, 10.0) == 0.85

    def test_closeness_clamped_to_one(self):
        from dispatcharr_ranked_matchups.scoring import _effective_closeness
        # Defensive: a bookmaker oddity producing closeness > 1 must clamp.
        assert _effective_closeness(1.5, None) == 1.0

    def test_closeness_clamped_to_zero(self):
        from dispatcharr_ranked_matchups.scoring import _effective_closeness
        assert _effective_closeness(-0.2, None) == 0.0

    def test_spread_fallback_when_closeness_none(self):
        from dispatcharr_ranked_matchups.scoring import _effective_closeness
        # spread=0 (perfect coinflip) → closeness 1.0; spread=14 → 0.0.
        assert _effective_closeness(None, 0.0) == 1.0
        assert _effective_closeness(None, 14.0) == 0.0
        # spread=7 (NFL "strong favorite") → 0.5.
        assert _effective_closeness(None, 7.0) == 0.5

    def test_negative_spread_returns_none(self):
        from dispatcharr_ranked_matchups.scoring import _effective_closeness
        # A negative spread is upstream-malformed; treat as no signal.
        assert _effective_closeness(None, -1.0) is None

    def test_both_none_returns_none(self):
        from dispatcharr_ranked_matchups.scoring import _effective_closeness
        assert _effective_closeness(None, None) is None


class TestScoreGameCloseness:
    """B.3: the close_game contribution from the closeness signal."""

    def test_closeness_one_gives_full_weight(self):
        # Perfect coinflip → closeness*weight = 1 * 3.0 = 3.0 raw.
        sig = GameSignals(closeness=1.0)
        s = score_game(sig, Weights())
        assert s.breakdown["close_game"] == 3.0

    def test_closeness_zero_silent(self):
        # Blowout → 0 contribution → not in breakdown (silent signal).
        sig = GameSignals(closeness=0.0)
        s = score_game(sig, Weights())
        assert "close_game" not in s.breakdown

    def test_closeness_midrange(self):
        # 0.5 coinflip-ness × 3.0 weight = 1.5.
        sig = GameSignals(closeness=0.5)
        s = score_game(sig, Weights())
        assert s.breakdown["close_game"] == 1.5

    def test_closeness_note_distinguishes_from_spread_path(self):
        # The note format reveals which path fired: closeness path says
        # "implied coinflip-ness", spread path says "betting spread".
        sig = GameSignals(closeness=0.8)
        s = score_game(sig, Weights())
        close_notes = [n for n in s.notes if "coinflip" in n.lower()]
        assert len(close_notes) == 1
        assert "0.80" in close_notes[0]

    def test_spread_fallback_still_works(self):
        # Pre-B.3 path: closeness=None, spread=2.0 (NCAAF tight game).
        # Normalized closeness = (14-2)/14 = 0.857; × 3.0 = 2.57.
        sig = GameSignals(spread=2.0)
        s = score_game(sig, Weights())
        assert round(s.breakdown["close_game"], 2) == 2.57

    def test_closeness_wins_over_spread(self):
        # Both populated (shouldn't happen in production but test defensively):
        # closeness wins. closeness=0.2 × 3.0 = 0.6, ignoring spread=0.
        sig = GameSignals(closeness=0.2, spread=0.0)
        s = score_game(sig, Weights())
        assert round(s.breakdown["close_game"], 2) == 0.6


class TestFormatChannelName:
    def test_inline_ranks_attach_to_each_team(self):
        # New default (#99): ranks render inline after the team they belong to,
        # not as a compact "NvN" prefix. rank_a/team_a is HOME, rank_b/team_b
        # is AWAY, and the matchup renders "away at home".
        sig = GameSignals(rank_a=9, rank_b=3)  # home rank 9, away rank 3
        score = score_game(GameSignals(rank_a=9, rank_b=3), Weights())
        name = format_channel_name("CFB", sig, score, "Penn State", "Ohio State")
        assert "Ohio State (3) at Penn State (9)" in name
        # the retired "NvN" prefix must be gone
        assert "3v9" not in name
        assert "9v3" not in name

    def test_truncates_to_250(self):
        sig = GameSignals(rank_a=1, rank_b=2)
        score = score_game(sig, Weights())
        long_tag = "x" * 500
        name = format_channel_name("EPL", sig, score, "A" * 30, "B" * 30, tagline=long_tag)
        assert len(name) <= 250
        assert name.endswith("...")

    def test_strips_team_suffix(self):
        sig = GameSignals(rank_a=3, rank_b=9)
        score = score_game(sig, Weights())
        name = format_channel_name("EPL", sig, score, "Manchester United FC", "Brentford FC")
        # "FC" must NOT appear in the rendered matchup
        assert "FC" not in name
        # away (Brentford, rank_b=9) at home (Man United, rank_a=3), suffixes gone
        assert "Brentford (9) at Manchester United (3)" in name

    def test_b_format_separator(self):
        sig = GameSignals(rank_a=3, rank_b=9)
        score = score_game(sig, Weights())
        name = format_channel_name(
            "EPL", sig, score, "Manchester United FC", "Brentford FC",
            tagline="title race",
        )
        # B-format uses '·' separators between segments
        assert "·" in name
        assert "title race" in name
        assert ":" not in name  # legacy format used ':'

    def test_favorite_emoji(self):
        sig = GameSignals(favorite_match=["Wrexham"])
        score = score_game(sig, Weights())
        name = format_channel_name("EPL", sig, score, "Wrexham AFC", "Hull")
        assert "⭐" in name

    def test_one_ranked(self):
        # Only the ranked team gets an inline rank; the unranked opponent stays
        # bare (no "vUR" marker, no empty parens) thanks to the {group} collapse.
        sig = GameSignals(rank_a=5, rank_b=None)  # home Texas ranked, away Oklahoma not
        score = score_game(sig, Weights())
        name = format_channel_name("CFB", sig, score, "Texas", "Oklahoma")
        assert "Oklahoma at Texas (5)" in name
        assert "vUR" not in name
        assert "()" not in name


class TestStripTeamSuffix:
    def test_strips_fc(self):
        assert strip_team_suffix("Brentford FC") == "Brentford"

    def test_strips_afc(self):
        assert strip_team_suffix("Wrexham AFC") == "Wrexham"

    def test_idempotent(self):
        assert strip_team_suffix("Brentford") == "Brentford"

    def test_keeps_compound_when_not_suffix(self):
        # "City" is a qualifier (Hull City is its own team), not a strippable
        # club-tag: must NOT be stripped.
        assert strip_team_suffix("Hull City") == "Hull City"

    def test_strips_only_trailing(self):
        # Don't strip mid-word matches.
        assert strip_team_suffix("FC Bayern München") == "FC Bayern München"


class TestRenderFavoriteImpact:
    """New format is action-oriented:
        '{Fav} fans: rooting against {Nearby} ({spots} and {pts} {dir}).
         [outcome clause when gap is interesting].'
    """

    def test_flip_clause_when_win_erasable(self):
        out = render_favorite_impact(
            "Manchester City FC", 2, 78,
            "Manchester United FC", 3, 75,
        )
        assert out.startswith("Manchester City fans: rooting against Manchester United")
        assert "1 spot and 3 pts back" in out
        # 3-pt lead is win-erasable → flip clause fires
        assert "Manchester United win flips them past you" in out

    def test_narrows_clause_for_medium_gap(self):
        out = render_favorite_impact("City", 2, 78, "United", 3, 72)
        assert "1 spot and 6 pts back" in out
        # 6-pt lead, win narrows to 3
        assert "narrows the gap to 3 pts" in out

    def test_no_outcome_clause_when_huge_gap(self):
        out = render_favorite_impact("City", 2, 78, "Liverpool", 4, 60)
        assert "2 spots and 18 pts back" in out
        # 18-pt lead is too far to narrate the outcome
        assert "narrows" not in out
        assert "flips" not in out

    def test_fav_chasing_narrate_loss(self):
        out = render_favorite_impact("City", 5, 60, "United", 3, 66)
        assert "2 spots and 6 pts ahead" in out
        # 6 pts behind → loss could close to 3
        assert "United loss could narrow the gap to 3 pts" in out

    def test_fav_chasing_could_put_level(self):
        out = render_favorite_impact("City", 5, 67, "United", 3, 70)
        # 3 pts back, "could put you level"
        assert "United loss could put you level" in out

    def test_fav_chasing_no_outcome_when_far(self):
        out = render_favorite_impact("City", 5, 50, "United", 3, 70)
        assert "behind" not in out  # spelled as "ahead" since United is above
        assert "2 spots and 20 pts ahead" in out
        # 20 pts back, no outcome narration
        assert "could" not in out

    def test_strips_suffixes_in_output(self):
        out = render_favorite_impact(
            "Manchester City FC", 2, 78,
            "Manchester United FC", 3, 75,
        )
        assert "FC" not in out

    def test_uses_rooting_against_framing(self):
        out = render_favorite_impact("City", 2, 78, "United", 3, 75)
        assert "rooting against" in out

    def test_no_points_falls_back_to_spots_only(self):
        out = render_favorite_impact("City", 2, None, "United", 3, None)
        assert "(1 spot back)" in out
        assert "pts" not in out
        assert "flips" not in out
        assert "narrows" not in out


class TestBuildImpactNarratives:
    def test_skips_when_favorite_is_playing(self):
        # Wrexham IS in this game → favorite signal handles it, skip impact narrative.
        narratives = build_impact_narratives(
            rank_home=4, rank_away=6,
            home="Middlesbrough FC", away="Wrexham AFC",
            favorites_with_standings=[
                {"name": "Wrexham AFC", "position": 6, "points": 70}
            ],
            standings_table=[
                {"name": "Middlesbrough FC", "position": 4, "points": 73},
                {"name": "Wrexham AFC", "position": 6, "points": 70},
            ],
        )
        assert narratives == []

    def test_picks_closest_team(self):
        narratives = build_impact_narratives(
            rank_home=3, rank_away=9,
            home="Manchester United FC", away="Brentford FC",
            favorites_with_standings=[
                {"name": "Manchester City FC", "position": 2, "points": 78}
            ],
            standings_table=[
                {"name": "Manchester City FC", "position": 2, "points": 78},
                {"name": "Manchester United FC", "position": 3, "points": 75},
                {"name": "Brentford FC", "position": 9, "points": 50},
            ],
        )
        assert len(narratives) == 1
        # Man United is closer (1 spot away): narrative should reference it
        assert "Manchester United" in narratives[0]
        assert "Brentford" not in narratives[0]

    def test_skips_when_neither_team_close(self):
        narratives = build_impact_narratives(
            rank_home=18, rank_away=19,
            home="Bottom Team", away="Other Bottom",
            favorites_with_standings=[
                {"name": "Top Team", "position": 1, "points": 90}
            ],
            standings_table=[],
        )
        assert narratives == []


class TestPickTagline:
    def test_tournament_stage_wins(self):
        tag = pick_tagline(
            score_breakdown={"tournament_stage": 5.0},
            favorites_matched=[],
            spread=None,
            importance_thresholds=["title"],
            tournament_stage="FINAL",
            rank_a=1, rank_b=2, rank_source="poll",
        )
        assert tag == "Final"

    def test_importance_picks_label(self):
        # Phase C.4: tagline derives from importance breakdown + thresholds_hit.
        tag = pick_tagline(
            score_breakdown={"importance": 8.0},
            favorites_matched=[],
            spread=None,
            importance_thresholds=["title", "UCL"],
            tournament_stage=None,
            rank_a=3, rank_b=9, rank_source="standings",
        )
        assert tag == "title / UCL race"

    def test_poll_rank_pair(self):
        tag = pick_tagline(
            score_breakdown={"rank_pair": 5.0},
            favorites_matched=[],
            spread=None,
            importance_thresholds=[],
            tournament_stage=None,
            rank_a=2, rank_b=4, rank_source="poll",
        )
        assert tag == "top-5 showdown"

    def test_standings_rank_pair_dropped(self):
        # For league standings (every team is "ranked"), rank-pair tagline
        # must be dropped: it's noise. Should fall through to other signals.
        tag = pick_tagline(
            score_breakdown={"rank_pair": 5.0},
            favorites_matched=[],
            spread=None,
            importance_thresholds=[],
            tournament_stage=None,
            rank_a=2, rank_b=4, rank_source="standings",
        )
        assert tag == ""  # nothing else fired

    def test_falls_back_to_toss_up(self):
        tag = pick_tagline(
            score_breakdown={"close_game": 3.0},
            favorites_matched=[],
            spread=1.5,
            importance_thresholds=[],
            tournament_stage=None,
            rank_a=None, rank_b=None, rank_source="poll",
        )
        assert tag == "toss-up"


class TestLeagueContexts:
    """Pin the threshold lists so a typo in scoring.py doesn't silently change
    every game's stakes signal."""

    def test_pl_thresholds(self):
        pl = LEAGUE_CONTEXTS["PL"]
        assert pl.matchdays_total == 38
        positions = [p for p, _, _ in pl.thresholds]
        assert 1 in positions       # title
        assert 4 in positions       # UCL
        assert 17 in positions      # relegation

    def test_elc_thresholds(self):
        elc = LEAGUE_CONTEXTS["ELC"]
        assert elc.matchdays_total == 46
        positions = [p for p, _, _ in elc.thresholds]
        assert 2 in positions       # auto-promotion
        assert 6 in positions       # playoff
        assert 21 in positions      # relegation

    def test_pl_threshold_weights(self):
        # Phase A.6: relegation and title should weight equally high (5.0);
        # UCL slightly less (4.0); Europa lowest (2.0). If anyone bumps
        # any of these, they should bump intentionally and update the
        # acceptance numbers downstream.
        pl = LEAGUE_CONTEXTS["PL"]
        weights = {label: w for _, label, w in pl.thresholds}
        assert weights["title"] == 5.0
        assert weights["UCL"] == 4.0
        assert weights["Europa/Conference"] == 2.0
        assert weights["relegation"] == 5.0

    def test_elc_threshold_weights(self):
        elc = LEAGUE_CONTEXTS["ELC"]
        weights = {label: w for _, label, w in elc.thresholds}
        assert weights["auto-promotion"] == 4.5
        assert weights["playoff"] == 3.0
        assert weights["relegation"] == 4.0

    def test_pl_has_boundary_summary(self):
        # The boundary_summary is rendered in the EPG description, so a
        # missing/typo'd value would silently degrade UX.
        assert "UCL" in LEAGUE_CONTEXTS["PL"].boundary_summary
        assert "relegation" in LEAGUE_CONTEXTS["PL"].boundary_summary

    def test_elc_has_boundary_summary(self):
        assert "auto-promotion" in LEAGUE_CONTEXTS["ELC"].boundary_summary
        assert "playoff" in LEAGUE_CONTEXTS["ELC"].boundary_summary
        assert "relegation" in LEAGUE_CONTEXTS["ELC"].boundary_summary


# ---------- Phase C: compute_match_importance + score_game importance branch ----------

class _FakeMatch:
    """Minimal GameRow-shaped object for testing compute_match_importance."""
    def __init__(self, home: str, away: str):
        self.home = home
        self.away = away
        self.extra = {"fd_id": 999}


class _FakeImportanceSource:
    """A SportSource stand-in that returns deterministic per-(team, label)
    leverages. Lets compute_match_importance be tested without running the
    actual Monte Carlo simulator: that's tested separately in
    test_simulation.py.
    """
    def __init__(self, leverages):
        # leverages is {(team, label): float in [0,1]}
        self.leverages = leverages
        self.supports_importance = True

    def __getattr__(self, name):
        # Other SportSource methods are unused on the compute_match_importance
        # path (which delegates straight to monte_carlo_importance_batch: we
        # monkeypatch the simulator below for tests).
        raise AttributeError(name)


class TestComputeMatchImportance:
    def _epl_ctx(self):
        return LEAGUE_CONTEXTS["PL"]

    def test_unsupported_source_returns_zero(self):
        # Simulator never gets called when supports_importance is False.
        class NoImportance:
            supports_importance = False
        raw, notes, _hit = compute_match_importance(
            NoImportance(), _FakeMatch("A", "B"), self._epl_ctx(), n_sims=10,
        )
        assert raw == 0.0
        assert notes == []

    def test_empty_thresholds_returns_zero(self):
        empty_ctx = LeagueContext(code="EMPTY", matchdays_total=10, thresholds=[])
        class S:
            supports_importance = True
        raw, notes, _hit = compute_match_importance(
            S(), _FakeMatch("A", "B"), empty_ctx, n_sims=10,
        )
        assert raw == 0.0
        assert notes == []

    def test_aggregates_leverage_times_weight(self, monkeypatch):
        """Heart of the function: sum (team, outcome) → leverage × weight."""
        # Hand-craft leverages: Tottenham strongly leverages relegation;
        # Everton has zero leverage anywhere.
        # EPL thresholds: title (5.0), UCL (4.0), Europa/Conference (2.0),
        # relegation (5.0).
        leverages = {
            ("Tottenham", "title"): 0.0,
            ("Tottenham", "UCL"): 0.0,
            ("Tottenham", "Europa/Conference"): 0.0,
            ("Tottenham", "relegation"): 0.5,   # → 0.5 × 5 = 2.5
            ("Everton", "title"): 0.0,
            ("Everton", "UCL"): 0.0,
            ("Everton", "Europa/Conference"): 0.0,
            ("Everton", "relegation"): 0.0,
        }
        # Patch monte_carlo_importance_batch to return our fixed leverages.
        from dispatcharr_ranked_matchups import simulation
        monkeypatch.setattr(
            simulation, "monte_carlo_importance_batch",
            lambda source, match, queries, n_sims, rng=None: {
                q: leverages.get(q, 0.0) for q in queries
            },
        )
        class S:
            supports_importance = True
        raw, notes, _hit = compute_match_importance(
            S(), _FakeMatch("Tottenham", "Everton"),
            self._epl_ctx(), n_sims=10,
        )
        assert raw == 2.5
        # Only nonzero contributions appear in notes.
        assert len(notes) == 1
        assert "Tottenham" in notes[0]
        assert "relegation" in notes[0]
        assert "0.50 leverage" in notes[0]
        assert "5.0" in notes[0]
        assert "2.50" in notes[0]

    def test_multiple_bands_per_team_all_contribute(self, monkeypatch):
        """A team can leverage multiple bands (champion AND UCL); both
        contribute to the raw total (sum aggregation, per the open-question
        recommendation in TUNING_REPORT).
        """
        leverages = {
            ("Arsenal", "title"): 0.6,             # 0.6 × 5.0 = 3.0
            ("Arsenal", "UCL"): 0.4,               # 0.4 × 4.0 = 1.6
            ("Arsenal", "Europa/Conference"): 0.2, # 0.2 × 2.0 = 0.4
            ("Arsenal", "relegation"): 0.0,
            ("Crystal Palace", "title"): 0.0,
            ("Crystal Palace", "UCL"): 0.0,
            ("Crystal Palace", "Europa/Conference"): 0.0,
            ("Crystal Palace", "relegation"): 0.0,
        }
        from dispatcharr_ranked_matchups import simulation
        monkeypatch.setattr(
            simulation, "monte_carlo_importance_batch",
            lambda source, match, queries, n_sims, rng=None: {
                q: leverages.get(q, 0.0) for q in queries
            },
        )
        class S:
            supports_importance = True
        raw, notes, _hit = compute_match_importance(
            S(), _FakeMatch("Arsenal", "Crystal Palace"),
            self._epl_ctx(), n_sims=10,
        )
        # 3.0 + 1.6 + 0.4 = 5.0
        assert raw == 5.0
        # Three nonzero notes, sorted by descending contribution.
        assert len(notes) == 3
        # First note should be the biggest contributor (title at 3.00).
        assert "title" in notes[0]
        assert "3.00" in notes[0]
        # Last note should be the smallest contributor (Europa at 0.40).
        assert "Europa" in notes[-1]
        assert "0.40" in notes[-1]

    def test_locked_outcomes_contribute_zero(self, monkeypatch):
        """When the simulator returns leverage=0 for every (team, outcome)
       : i.e., locked seasons: the raw points are 0 and there are no notes.
        This is the structural fix Phase C delivers: mathematically locked
        teams stop polluting the importance signal.
        """
        from dispatcharr_ranked_matchups import simulation
        monkeypatch.setattr(
            simulation, "monte_carlo_importance_batch",
            lambda source, match, queries, n_sims, rng=None: {q: 0.0 for q in queries},
        )
        class S:
            supports_importance = True
        raw, notes, _hit = compute_match_importance(
            S(), _FakeMatch("Burnley", "Wolves"),
            self._epl_ctx(), n_sims=10,
        )
        assert raw == 0.0
        assert notes == []

    def test_notes_sorted_by_descending_contribution(self, monkeypatch):
        """Notes order matters: the top-3 contributors are surfaced to
        cache.json, and the user reads the BIG signals first."""
        leverages = {
            ("Home", "title"): 0.1,                # 0.1 × 5.0 = 0.5
            ("Home", "UCL"): 0.0,
            ("Home", "Europa/Conference"): 0.4,    # 0.4 × 2.0 = 0.8
            ("Home", "relegation"): 0.3,           # 0.3 × 5.0 = 1.5
            ("Away", "title"): 0.0,
            ("Away", "UCL"): 0.0,
            ("Away", "Europa/Conference"): 0.0,
            ("Away", "relegation"): 0.0,
        }
        from dispatcharr_ranked_matchups import simulation
        monkeypatch.setattr(
            simulation, "monte_carlo_importance_batch",
            lambda source, match, queries, n_sims, rng=None: {
                q: leverages.get(q, 0.0) for q in queries
            },
        )
        class S:
            supports_importance = True
        raw, notes, _hit = compute_match_importance(
            S(), _FakeMatch("Home", "Away"),
            LEAGUE_CONTEXTS["PL"], n_sims=10,
        )
        # 0.5 + 0.8 + 1.5 = 2.8
        assert raw == 2.8
        # Notes should be sorted highest-first.
        assert "relegation" in notes[0]   # 1.50
        assert "Europa" in notes[1]       # 0.80
        assert "title" in notes[2]        # 0.50


class TestComputeMatchImportanceChainRouting:
    """#53: when the source declares a non-None `cross_source_chain`,
    compute_match_importance routes to `monte_carlo_importance_batch_
    chain` instead of the regular batch. WC_GS LEAGUE_CONTEXT now
    carries downstream cascade labels (R16/QF/SF/F/winner), so the
    chain's per-team merged outcomes get correct weights applied.
    """

    def _wc_gs_ctx(self):
        return LEAGUE_CONTEXTS["WC_GS"]

    def test_wc_gs_thresholds_carry_downstream_cascade(self):
        # Pin the data-model assumption that powers the chain weighting:
        # WC_GS must include the downstream knockout labels.
        ctx = self._wc_gs_ctx()
        labels = {label for _, label, _ in ctx.thresholds}
        assert "advance" in labels
        assert {"last_32", "round_of_16", "quarterfinal",
                "semifinal", "final", "winner"} <= labels

    def test_routes_to_chain_when_cross_source_chain_returns_tuple(
        self, monkeypatch,
    ):
        # Patch BOTH simulator entry points. Confirm the chain function
        # fires (not the regular batch) AND that the queries passed in
        # include the downstream-cascade labels: otherwise WC_GS
        # could silently drop the cascade labels and this routing test
        # would still pass while production produces 0 contribution
        # on R16+ bands.
        from dispatcharr_ranked_matchups import simulation
        called: Dict[str, int] = {"batch": 0, "chain": 0}
        captured_queries: List[List[Tuple[str, str]]] = []

        def fake_batch(source, match, queries, n_sims, rng=None):
            called["batch"] += 1
            return {q: 0.0 for q in queries}

        def fake_chain(source, match, queries, downstream, seed_fn,
                       n_sims, rng=None):
            called["chain"] += 1
            captured_queries.append(list(queries))
            return {q: 0.0 for q in queries}

        monkeypatch.setattr(simulation, "monte_carlo_importance_batch", fake_batch)
        monkeypatch.setattr(
            simulation, "monte_carlo_importance_batch_chain", fake_chain,
        )

        class FakeDownstream:
            supports_importance = True

        class FakeGroupSource:
            supports_importance = True

            def cross_source_chain(self):
                return (FakeDownstream(), lambda state: state)

        compute_match_importance(
            FakeGroupSource(), _FakeMatch("Mexico", "South Africa"),
            self._wc_gs_ctx(), n_sims=10,
        )
        assert called["chain"] == 1
        assert called["batch"] == 0
        # Each playing team queried against EVERY threshold label,
        # including the downstream cascade. If anyone strips cascade
        # labels from WC_GS, this assertion catches it.
        seen_labels = {label for _team, label in captured_queries[0]}
        assert "advance" in seen_labels
        assert "round_of_16" in seen_labels
        assert "quarterfinal" in seen_labels
        assert "semifinal" in seen_labels
        assert "final" in seen_labels
        assert "winner" in seen_labels

    def test_routes_to_batch_when_cross_source_chain_returns_none(
        self, monkeypatch,
    ):
        # Same shape as above but cross_source_chain returns None
        # (chain inactive). Regular batch must fire.
        from dispatcharr_ranked_matchups import simulation
        called: Dict[str, int] = {"batch": 0, "chain": 0}

        def fake_batch(source, match, queries, n_sims, rng=None):
            called["batch"] += 1
            return {q: 0.0 for q in queries}

        def fake_chain(*args, **kwargs):
            called["chain"] += 1
            return {q: 0.0 for q in kwargs.get("queries", [])}

        monkeypatch.setattr(simulation, "monte_carlo_importance_batch", fake_batch)
        monkeypatch.setattr(
            simulation, "monte_carlo_importance_batch_chain", fake_chain,
        )

        class FakeSource:
            supports_importance = True

            def cross_source_chain(self):
                return None

        compute_match_importance(
            FakeSource(), _FakeMatch("Mexico", "South Africa"),
            self._wc_gs_ctx(), n_sims=10,
        )
        assert called["batch"] == 1
        assert called["chain"] == 0

    def test_routes_to_batch_when_source_lacks_cross_source_chain(
        self, monkeypatch,
    ):
        # Sources without the cross_source_chain method (EPL, NCAAF,
        # etc.) take the regular batch path unconditionally.
        from dispatcharr_ranked_matchups import simulation
        called: Dict[str, int] = {"batch": 0, "chain": 0}

        def fake_batch(source, match, queries, n_sims, rng=None):
            called["batch"] += 1
            return {q: 0.0 for q in queries}

        def fake_chain(*args, **kwargs):
            called["chain"] += 1
            return {q: 0.0 for q in kwargs.get("queries", [])}

        monkeypatch.setattr(simulation, "monte_carlo_importance_batch", fake_batch)
        monkeypatch.setattr(
            simulation, "monte_carlo_importance_batch_chain", fake_chain,
        )

        class NoChainSource:
            supports_importance = True
            # No cross_source_chain method at all.

        compute_match_importance(
            NoChainSource(), _FakeMatch("Wrexham", "Hull"),
            LEAGUE_CONTEXTS["ELC"], n_sims=10,
        )
        assert called["batch"] == 1
        assert called["chain"] == 0


class TestScoreGameImportanceBranch:
    def test_importance_points_zero_no_breakdown_entry(self):
        s = GameSignals(rank_a=1, rank_b=2, importance_points=0.0)
        score = score_game(s, Weights())
        assert "importance" not in score.breakdown

    def test_importance_points_contributes_weighted(self):
        s = GameSignals(rank_a=1, rank_b=2, importance_points=4.2)
        score = score_game(s, Weights(importance=2.0))
        # 4.2 raw × 2.0 weight = 8.4
        assert score.breakdown["importance"] == 8.4

    def test_importance_notes_surfaced_in_breakdown_notes(self):
        notes = [
            "Tottenham relegation: 0.50 leverage × 5.0 = 2.50",
            "Everton relegation: 0.30 leverage × 5.0 = 1.50",
        ]
        s = GameSignals(rank_a=1, rank_b=2,
                        importance_points=4.0, importance_notes=notes)
        score = score_game(s, Weights())
        importance_lines = [n for n in score.notes if n.startswith("importance:")]
        assert len(importance_lines) == 2
        assert "Tottenham" in importance_lines[0]

    def test_importance_notes_capped_at_three(self):
        notes = [
            "Team A title: 0.50 leverage × 5.0 = 2.50",
            "Team B UCL: 0.40 leverage × 4.0 = 1.60",
            "Team C Europa: 0.30 leverage × 2.0 = 0.60",
            "Team D relegation: 0.20 leverage × 5.0 = 1.00",
            "Team E something: 0.10 leverage × 1.0 = 0.10",
        ]
        s = GameSignals(rank_a=1, rank_b=2,
                        importance_points=5.8, importance_notes=notes)
        score = score_game(s, Weights())
        importance_lines = [n for n in score.notes if n.startswith("importance:")]
        # Cap = 3 in score_game so cache.json stays readable.
        assert len(importance_lines) == 3

    def test_zero_weight_disables_branch(self):
        s = GameSignals(rank_a=1, rank_b=2, importance_points=4.0,
                        importance_notes=["X: 0.50 leverage × 5.0 = 2.50"])
        score = score_game(s, Weights(importance=0.0))
        # weight=0 → zero contribution AND no breakdown entry: same as
        # importance_points=0. Other signals (rank_pair) still fire.
        assert "importance" not in score.breakdown


class TestAdaptiveCompress:
    """#7: season-relative score normalization via batch-median scaling."""

    def test_falls_back_to_absolute_on_small_batch(self):
        from dispatcharr_ranked_matchups.scoring import adaptive_compress, _compress_to_10
        # 4 samples < _ADAPTIVE_MIN_SAMPLES=5 → absolute compression.
        raws = [2.0, 4.0, 8.0, 12.0]
        out = adaptive_compress(raws)
        expected = [_compress_to_10(r) for r in raws]
        assert out == expected

    def test_scales_against_median_when_above_threshold(self):
        from dispatcharr_ranked_matchups.scoring import adaptive_compress
        # All-low batch: median 4 → scale=4. The top entry (8) is 2×
        # median → should compress to a high but non-saturated value.
        out = adaptive_compress([2.0, 3.0, 4.0, 5.0, 8.0])
        # Median = 4. Each entry's tanh(r / (4 × 1.6)) = tanh(r/6.4).
        # 4.0 (median) → 10 × tanh(4/6.4) = 10 × tanh(0.625) ≈ 5.54.
        # 8.0 (2× median) → 10 × tanh(8/6.4) = 10 × tanh(1.25) ≈ 8.48.
        assert 5.4 <= out[2] <= 5.6, f"median should map near 5.55, got {out[2]:.2f}"
        assert 8.0 <= out[4] <= 8.7, f"2× median should map near 8.48, got {out[4]:.2f}"

    def test_zero_compresses_to_zero(self):
        from dispatcharr_ranked_matchups.scoring import adaptive_compress
        out = adaptive_compress([0.0, 4.0, 8.0, 12.0, 16.0, 20.0])
        assert out[0] == 0.0

    def test_negative_raw_treated_as_zero(self):
        from dispatcharr_ranked_matchups.scoring import adaptive_compress
        out = adaptive_compress([-1.0, 4.0, 8.0, 12.0, 16.0, 20.0])
        assert out[0] == 0.0

    def test_uniformly_low_batch_doesnt_saturate(self):
        # If every game is low (median 0.5), scale floors at 1.0 so a
        # raw=0.5 doesn't compress to ★10 via division-by-near-zero.
        from dispatcharr_ranked_matchups.scoring import adaptive_compress
        out = adaptive_compress([0.5] * 5)
        # All identical inputs map to the same output. With scale=1.0
        # (floor): tanh(0.5/(1×1.6)) = tanh(0.3125) ≈ 0.303 → 3.03.
        for v in out:
            assert v < 4.0, f"low-uniform batch should NOT saturate, got {v:.2f}"

    def test_late_season_inflated_batch_compresses(self):
        # Late-season simulation: half the batch is high-stakes (raw 20+),
        # median pushes high → high-raw scores no longer saturate at ★10.
        from dispatcharr_ranked_matchups.scoring import adaptive_compress
        out = adaptive_compress([4.0, 8.0, 12.0, 16.0, 20.0, 24.0, 28.0])
        # Median = 16; routine games (raw=4-8, half the median) compress
        # to mid-range; top (28 = ~1.75× median) still leaves headroom for the
        # rare ★10. Top output well below 10 → curve doesn't saturate.
        assert out[-1] < 10.0
        # Spread: top of batch is meaningfully higher than median.
        assert out[-1] - out[3] > 1.0

    def test_preserves_ordering(self):
        from dispatcharr_ranked_matchups.scoring import adaptive_compress
        raws = [1.0, 5.0, 3.0, 9.0, 7.0, 12.0]
        out = adaptive_compress(raws)
        # Same order after normalization as raw order.
        pairs_in = sorted(zip(raws, range(len(raws))))
        pairs_out = sorted(zip(out, range(len(out))))
        in_order = [i for _, i in pairs_in]
        out_order = [i for _, i in pairs_out]
        assert in_order == out_order
