"""Tests for scoring.py: pure logic, no Django, no network."""

from dispatcharr_ranked_matchups.scoring import (
    GameSignals,
    Weights,
    LEAGUE_CONTEXTS,
    TEAM_QUALIFIER_TOKENS,
    TEAM_SUFFIX_TOKENS,
    _compress_to_10,
    build_why_text,
    compute_impact_on_favorites,
    compute_team_stakes,
    format_channel_name,
    match_favorites,
    score_game,
)


class TestCompressTo10:
    """Anchor values from the docstring — if these drift, the score everyone
    sees in the UI shifts. Pin them."""

    def test_zero_or_negative(self):
        assert _compress_to_10(0) == 0.0
        assert _compress_to_10(-5) == 0.0

    def test_known_anchors(self):
        # Knee = 8.0 → 10 * tanh(raw/8). Tight tolerances to catch any silent
        # change to _FINAL_KNEE.
        assert round(_compress_to_10(2), 2) == 2.45
        assert round(_compress_to_10(4), 2) == 4.62
        assert round(_compress_to_10(8), 2) == 7.62
        assert round(_compress_to_10(12), 2) == 9.05
        assert round(_compress_to_10(16), 2) == 9.64

    def test_asymptotes_at_10_for_large_input(self):
        # tanh saturates to 1.0 in float64 well before raw=1000 — that's
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
        # actually it should — "Hull City" has "City" as a TEAM_QUALIFIER_TOKEN
        assert match_favorites("Hull City AFC", "QPR", ["Hull"]) == ["Hull"]

    def test_qualifier_required_after_partial(self):
        # "UNC" should NOT match "UNC Pembroke" because Pembroke isn't a
        # qualifier token — it's a different school.
        assert match_favorites("UNC Pembroke", "Wofford", ["UNC"]) == []

    def test_qualifier_required_for_compound_name(self):
        # "North Carolina" inside "North Carolina A&T" — A&T is the second
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
        # soccer's _lookup_spread can strip a suffix that the favorite-matcher
        # would have kept and trusted as a real second word.
        for tok in TEAM_SUFFIX_TOKENS:
            assert tok in TEAM_QUALIFIER_TOKENS, f"{tok!r} drifted"


class TestComputeTeamStakes:
    def test_none_position(self):
        assert compute_team_stakes(None, [(1, "title")]) == (0.0, [])

    def test_exact_threshold(self):
        # PL: 4th = UCL line. Default proximity=2 → exact match worth 3 pts.
        thresholds = [(1, "title"), (4, "UCL"), (17, "relegation")]
        pts, hits = compute_team_stakes(4, thresholds)
        assert pts == 3.0
        assert "UCL" in hits

    def test_adjacent(self):
        thresholds = [(4, "UCL")]
        pts, hits = compute_team_stakes(5, thresholds)
        assert pts == 2.0  # ±1 → proximity (2)

    def test_two_away(self):
        thresholds = [(4, "UCL")]
        pts, hits = compute_team_stakes(6, thresholds)
        assert pts == 1.0  # ±2 → proximity-1 (1)

    def test_far_away_no_points(self):
        thresholds = [(4, "UCL")]
        pts, hits = compute_team_stakes(15, thresholds)
        assert pts == 0.0
        assert hits == []

    def test_stacks_across_thresholds(self):
        # 4th place = at UCL line AND 3 away from title. With proximity=2, only
        # UCL fires.
        thresholds = [(1, "title"), (4, "UCL")]
        pts, hits = compute_team_stakes(4, thresholds)
        assert "UCL" in hits
        assert "title" not in hits
        assert pts == 3.0


class TestComputeImpactOnFavorites:
    def test_skips_when_favorite_is_playing(self):
        affected = compute_impact_on_favorites(
            rank_a=4, rank_b=5,
            team_a="Wrexham AFC", team_b="Hull City AFC",
            favorites_in_league=[("Wrexham AFC", 4)],
        )
        # Wrexham IS playing → skip; impact-on-favorite is for OTHER games.
        assert affected == []

    def test_picks_up_neighbor_game(self):
        affected = compute_impact_on_favorites(
            rank_a=5, rank_b=7,
            team_a="Hull City AFC", team_b="Watford FC",
            favorites_in_league=[("Wrexham AFC", 4)],
            proximity=3,
        )
        assert "Wrexham AFC" in affected

    def test_far_away_no_impact(self):
        affected = compute_impact_on_favorites(
            rank_a=20, rank_b=21,
            team_a="Cardiff City", team_b="Bristol City",
            favorites_in_league=[("Wrexham AFC", 4)],
            proximity=3,
        )
        assert affected == []


class TestScoreGame:
    def test_both_top_5(self):
        signals = GameSignals(rank_a=1, rank_b=5)
        s = score_game(signals, Weights())
        assert "rank_pair" in s.breakdown
        assert s.breakdown["rank_pair"] > 0
        assert s.final > 0

    def test_favorite_flat_boost(self):
        sig = GameSignals(favorite_match=["Wrexham"])
        s = score_game(sig, Weights())
        assert s.breakdown["favorite"] == 4.0

    def test_late_season_doubles_stakes(self):
        sig = GameSignals(stakes_a=2.0, season_progress=0.90)
        s = score_game(sig, Weights())
        # 2.0 raw × 2x late mult × 2.0 weight = 8.0
        assert s.breakdown["stakes"] == 8.0

    def test_mid_season_no_late_mult(self):
        sig = GameSignals(stakes_a=2.0, season_progress=0.5)
        s = score_game(sig, Weights())
        assert s.breakdown["stakes"] == 4.0  # 2.0 × 1.0 × 2.0

    def test_close_spread(self):
        sig = GameSignals(spread=0.5)
        s = score_game(sig, Weights())
        assert "close_game" in s.breakdown

    def test_unranked_vs_unranked_no_signal(self):
        # No rank, no favorite, no stakes → empty breakdown, score 0.
        sig = GameSignals()
        s = score_game(sig, Weights())
        assert s.raw == 0.0
        assert s.final == 0.0

    def test_breakdown_sums_to_raw(self):
        sig = GameSignals(
            rank_a=2, rank_b=3,
            favorite_match=["Wrexham"],
            spread=2.0,
            stakes_a=1.0, stakes_b=1.0, season_progress=0.85,
        )
        s = score_game(sig, Weights())
        # raw must equal sum of breakdown contributions to within rounding
        breakdown_sum = round(sum(s.breakdown.values()), 2)
        assert abs(s.raw - breakdown_sum) < 0.01


class TestFormatChannelName:
    def test_rank_pair_normalized_low_first(self):
        sig = GameSignals(rank_a=9, rank_b=3)
        score = score_game(GameSignals(rank_a=9, rank_b=3), Weights())
        name = format_channel_name("EPL", sig, score, "Manchester United", "Brentford FC")
        # Should render "3v9" not "9v3"
        assert " 3v9 " in name

    def test_truncates_to_250(self):
        sig = GameSignals(rank_a=1, rank_b=2)
        score = score_game(sig, Weights())
        long_why = "x" * 500
        name = format_channel_name("EPL", sig, score, "A" * 30, "B" * 30, why=long_why)
        assert len(name) <= 250
        assert name.endswith("...")

    def test_favorite_emoji(self):
        sig = GameSignals(favorite_match=["Wrexham"])
        score = score_game(sig, Weights())
        name = format_channel_name("EPL", sig, score, "Wrexham AFC", "Hull")
        assert "⭐" in name

    def test_one_ranked(self):
        sig = GameSignals(rank_a=5, rank_b=None)
        score = score_game(sig, Weights())
        name = format_channel_name("CFB", sig, score, "Texas", "Oklahoma")
        assert "5vUR" in name


class TestBuildWhyText:
    def test_empty_falls_back(self):
        why = build_why_text(None, None, [], {})
        assert why == "interesting matchup"

    def test_top_5_phrase(self):
        why = build_why_text(2, 5, [], {"rank_pair": 5.0})
        assert "top-5" in why

    def test_late_season_qualifier(self):
        why = build_why_text(
            None, None, [],
            {"stakes": 5.0},
            stakes_thresholds=["title", "UCL"],
            season_progress=0.90,
        )
        assert "final stretch" in why

    def test_toss_up_phrase(self):
        why = build_why_text(None, None, [], {"close_game": 3.0}, spread=0.5)
        assert "toss-up" in why


class TestLeagueContexts:
    """Pin the threshold lists so a typo in scoring.py doesn't silently change
    every game's stakes signal."""

    def test_pl_thresholds(self):
        pl = LEAGUE_CONTEXTS["PL"]
        assert pl.matchdays_total == 38
        positions = [p for p, _ in pl.thresholds]
        assert 1 in positions       # title
        assert 4 in positions       # UCL
        assert 17 in positions      # relegation

    def test_elc_thresholds(self):
        elc = LEAGUE_CONTEXTS["ELC"]
        assert elc.matchdays_total == 46
        positions = [p for p, _ in elc.thresholds]
        assert 2 in positions       # auto-promotion
        assert 6 in positions       # playoff
        assert 21 in positions      # relegation
