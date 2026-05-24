"""Tests for scoring.py: pure logic, no Django, no network."""

from dispatcharr_ranked_matchups.scoring import (
    GameSignals,
    Weights,
    LEAGUE_CONTEXTS,
    TEAM_QUALIFIER_TOKENS,
    TEAM_SUFFIX_TOKENS,
    _IMPACT_PROXIMITY_CAP,
    _compress_to_10,
    _late_season_multiplier,
    build_impact_narratives,
    build_why_text,
    compute_impact_on_favorites,
    compute_team_stakes,
    format_channel_name,
    match_favorites,
    pick_tagline,
    render_favorite_impact,
    score_game,
    strip_team_suffix,
)


class TestCompressTo10:
    """Anchor values from the docstring — if these drift, the score everyone
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
        # soccer's _lookup_odds can strip a suffix that the favorite-matcher
        # would have kept and trusted as a real second word.
        for tok in TEAM_SUFFIX_TOKENS:
            assert tok in TEAM_QUALIFIER_TOKENS, f"{tok!r} drifted"


class TestComputeTeamStakes:
    # Each threshold tuple is (position, label, consequence_weight) per
    # Phase A.5/A.6. weight=1.0 here so the proximity arithmetic in
    # these tests reproduces the pre-weighted behavior unchanged.

    def test_none_position(self):
        assert compute_team_stakes(None, [(1, "title", 1.0)]) == (0.0, [])

    # compute_team_stakes returns leverage_in_[0,1] × consequence_weight
    # per Phase A.5/A.6. With proximity=2:
    #   d=0 (exact)    → leverage 1.0   (3/3)
    #   d=1 (adjacent) → leverage ~0.67 (2/3)
    #   d=2 (two away) → leverage ~0.33 (1/3)
    # With weight=1.0 the leverage IS the score; with weight=5.0
    # (relegation/title) the score scales up by 5x.

    def test_exact_threshold(self):
        # PL: 4th = UCL line. d=0 → leverage 1.0 × weight 1.0 = 1.0 pt.
        thresholds = [(1, "title", 1.0), (4, "UCL", 1.0), (17, "relegation", 1.0)]
        pts, hits = compute_team_stakes(4, thresholds)
        assert pts == 1.0
        assert "UCL" in hits

    def test_adjacent(self):
        thresholds = [(4, "UCL", 1.0)]
        pts, hits = compute_team_stakes(5, thresholds)
        # d=1 → leverage 2/3 × weight 1.0
        assert round(pts, 4) == round(2.0 / 3.0, 4)
        assert hits == ["UCL"]

    def test_two_away(self):
        thresholds = [(4, "UCL", 1.0)]
        pts, hits = compute_team_stakes(6, thresholds)
        # d=2 → leverage 1/3 × weight 1.0
        assert round(pts, 4) == round(1.0 / 3.0, 4)
        assert hits == ["UCL"]

    def test_far_away_no_points(self):
        thresholds = [(4, "UCL", 1.0)]
        pts, hits = compute_team_stakes(15, thresholds)
        assert pts == 0.0
        assert hits == []

    def test_stacks_across_thresholds(self):
        # 4th place = at UCL line AND 3 away from title. With proximity=2,
        # only UCL fires.
        thresholds = [(1, "title", 1.0), (4, "UCL", 1.0)]
        pts, hits = compute_team_stakes(4, thresholds)
        assert "UCL" in hits
        assert "title" not in hits
        assert pts == 1.0  # only UCL, d=0, weight 1.0

    def test_weight_multiplies_into_score(self):
        # relegation weight 5.0, exact match (d=0, leverage 1.0) → 5.0 pts.
        thresholds = [(17, "relegation", 5.0)]
        pts, _ = compute_team_stakes(17, thresholds)
        assert pts == 5.0

    def test_weights_distinguish_consequences(self):
        # Title (5) and Europa (2) at the same proximity diverge cleanly.
        # The cross-sport calibration in microcosm.
        title = [(1, "title", 5.0)]
        europa = [(7, "Europa", 2.0)]
        title_pts, _ = compute_team_stakes(1, title)
        europa_pts, _ = compute_team_stakes(7, europa)
        assert title_pts == 5.0
        assert europa_pts == 2.0
        assert title_pts > europa_pts

    # ---- Elimination gating (A.5) ----

    def test_no_standings_no_gating(self):
        # Without standings_points_by_position, gating is OFF and
        # behavior reverts to pure proximity (the back-compat path
        # used by knockout comps with no league table).
        thresholds = [(4, "UCL", 1.0)]
        pts, _ = compute_team_stakes(4, thresholds)
        assert pts == 1.0  # d=0 → leverage 1.0 × weight 1.0
        # Explicit None.
        pts2, _ = compute_team_stakes(
            4, thresholds,
            team_points=20,
            matches_remaining=10,
            standings_points_by_position=None,
        )
        assert pts2 == 1.0

    def test_climber_locked_out_dropped(self):
        # Team at position 6 (d=2 from UCL cutoff at 4), proximity-eligible.
        # Has 40 points, 2 matches left, max reachable = 46.
        # Team at 4th has 60 points. 46 < 60 → mathematically can't catch.
        # Threshold should be dropped; pts=0.
        thresholds = [(4, "UCL", 1.0)]
        pts, hits = compute_team_stakes(
            6, thresholds,
            team_points=40, matches_remaining=2,
            standings_points_by_position={4: 60},
        )
        assert pts == 0.0
        assert hits == []

    def test_climber_within_reach_still_fires(self):
        # Same shape as above but with enough matches left to catch.
        # Team has 40, 10 left → max 70. 4th has 60 → catchable.
        thresholds = [(4, "UCL", 1.0)]
        pts, hits = compute_team_stakes(
            6, thresholds,
            team_points=40, matches_remaining=10,
            standings_points_by_position={4: 60},
        )
        # d=2 → leverage 1/3 × weight 1.0
        assert round(pts, 4) == round(1.0 / 3.0, 4)
        assert "UCL" in hits

    def test_defender_locked_in_dropped(self):
        # Team at position 3 (d=1 above UCL cutoff at 4). Defending.
        # The live opponent is at cutoff+1 (position 5) — they're the
        # chaser. With chaser at 30 pts + 5 matches × 3 = 45 max, and
        # defender at 60, chaser can't catch → locked in.
        thresholds = [(4, "UCL", 1.0)]
        pts, _ = compute_team_stakes(
            3, thresholds,
            team_points=60, matches_remaining=5,
            standings_points_by_position={5: 30},
        )
        assert pts == 0.0

    def test_defender_still_vulnerable_fires(self):
        # Chaser at position 5 has 50 points and 5 matches left
        # → max 65 > 60 (defender at #3). The race is live.
        thresholds = [(4, "UCL", 1.0)]
        pts, _ = compute_team_stakes(
            3, thresholds,
            team_points=60, matches_remaining=5,
            standings_points_by_position={5: 50},
        )
        # d=1 → leverage 2/3 × weight 1.0
        assert round(pts, 4) == round(2.0 / 3.0, 4)

    def test_at_threshold_treated_as_defender(self):
        # team_position == cutoff: the marginal winner, holding the
        # band. Live opponent is at cutoff+1 (the chaser). If the
        # chaser can catch, race is live; if not, locked in.
        thresholds = [(4, "UCL", 1.0)]
        # Chaser at #5 has 60 pts, 1 match left → max 63 > 99? No.
        # team_points=99 > chaser_max=63 → locked in.
        pts_locked, _ = compute_team_stakes(
            4, thresholds,
            team_points=99, matches_remaining=1,
            standings_points_by_position={5: 60},
        )
        assert pts_locked == 0.0
        # Same setup but chaser at 98 + 3 = 101 > 99 → live.
        pts_live, _ = compute_team_stakes(
            4, thresholds,
            team_points=99, matches_remaining=1,
            standings_points_by_position={5: 98},
        )
        assert pts_live == 1.0  # leverage 1.0 × weight 1.0

    def test_dead_rubber_relegation_drops_out(self):
        # The canonical Phase-A.5 acceptance test: Blackburn-Leicester
        # MD46 type scenario. Both teams in the bottom three, already
        # mathematically relegated. The relegation threshold should NOT
        # fire, even though they're close to position 21.
        thresholds = [(21, "relegation", 4.0)]
        # Team at position 22, 35 points, 1 match left (this matchday).
        # Position 21 has 45 points → max reachable 35+3=38 < 45.
        # Locked out from above; threshold drops.
        pts, _ = compute_team_stakes(
            22, thresholds,
            team_points=35, matches_remaining=1,
            standings_points_by_position={21: 45},
        )
        assert pts == 0.0

    def test_dead_rubber_season_over(self):
        # End-of-season check: matches_remaining=0 means the season is
        # done. With standings provided, the gate still activates and
        # any unreachable threshold drops cleanly. (Old contract
        # disabled gating at matches_remaining=0; the new one treats
        # current points as final.)
        thresholds = [(21, "relegation", 4.0)]
        pts, _ = compute_team_stakes(
            22, thresholds,
            team_points=35, matches_remaining=0,
            standings_points_by_position={21: 45},
        )
        assert pts == 0.0


class TestComputeImpactOnFavorites:
    def test_skips_when_favorite_is_playing(self):
        affected = compute_impact_on_favorites(
            rank_a=4, rank_b=5,
            team_a="Wrexham AFC", team_b="Hull City AFC",
            favorites_in_league=[("Wrexham AFC", 4, 5.0)],
        )
        # Wrexham IS playing → skip; impact-on-favorite is for OTHER games.
        assert affected == []

    def test_picks_up_neighbor_game(self):
        affected = compute_impact_on_favorites(
            rank_a=5, rank_b=7,
            team_a="Hull City AFC", team_b="Watford FC",
            favorites_in_league=[("Wrexham AFC", 4, 5.0)],
            proximity=3,
        )
        # Wrexham at pos 4; Hull at 5 = d=1; Watford at 7 = d=3; min(d)=1.
        assert affected == [("Wrexham AFC", 5.0, 1)]

    def test_far_away_no_impact(self):
        affected = compute_impact_on_favorites(
            rank_a=20, rank_b=21,
            team_a="Cardiff City", team_b="Bristol City",
            favorites_in_league=[("Wrexham AFC", 4, 5.0)],
            proximity=3,
        )
        assert affected == []

    def test_picks_closest_distance(self):
        # Both home and away within proximity — should report the min distance.
        affected = compute_impact_on_favorites(
            rank_a=4, rank_b=6,
            team_a="Hull City AFC", team_b="Watford FC",
            favorites_in_league=[("Wrexham AFC", 5, 5.0)],
            proximity=3,
        )
        # Wrexham at 5; Hull at 4 (d=1), Watford at 6 (d=1); min=1.
        assert affected == [("Wrexham AFC", 5.0, 1)]

    def test_carries_stakes_value_through(self):
        # Stakes pts passed in must be returned untouched — this function
        # is a carrier, not a recomputer.
        affected = compute_impact_on_favorites(
            rank_a=5, rank_b=7,
            team_a="Hull City AFC", team_b="Watford FC",
            favorites_in_league=[("Wrexham AFC", 4, 3.75)],
            proximity=3,
        )
        assert affected == [("Wrexham AFC", 3.75, 1)]

    def test_multiple_favorites_each_get_own_stakes(self):
        affected = compute_impact_on_favorites(
            rank_a=5, rank_b=18,
            team_a="Hull City AFC", team_b="Sunderland",
            favorites_in_league=[
                ("Wrexham AFC", 4, 5.0),
                ("Tottenham Hotspur FC", 17, 4.5),
            ],
            proximity=3,
        )
        # Hull (5) is d=1 from Wrexham (4); Sunderland (18) is d=1 from Tottenham (17).
        assert ("Wrexham AFC", 5.0, 1) in affected
        assert ("Tottenham Hotspur FC", 4.5, 1) in affected
        assert len(affected) == 2

    def test_zero_stakes_still_emitted(self):
        # A favorite with a locked-in/locked-out race (stakes=0 from
        # compute_team_stakes) should still emit a tuple — score_game's
        # contribution will be 0, no harm. The narrative path may want
        # to mention the proximity even when stakes have decided.
        affected = compute_impact_on_favorites(
            rank_a=5, rank_b=7,
            team_a="Hull City AFC", team_b="Watford FC",
            favorites_in_league=[("Wrexham AFC", 4, 0.0)],
            proximity=3,
        )
        assert affected == [("Wrexham AFC", 0.0, 1)]


class TestLateSeasonMultiplier:
    """Pinned because both score_game's stakes block AND its
    impact-on-favorite block depend on these exact thresholds. A drift
    here desynchronizes them, which was the original B.1 bug shape."""

    def test_early_season_is_1x(self):
        assert _late_season_multiplier(0.0) == 1.0
        assert _late_season_multiplier(0.5) == 1.0
        assert _late_season_multiplier(0.69) == 1.0

    def test_late_70pct_is_1_5x(self):
        assert _late_season_multiplier(0.70) == 1.5
        assert _late_season_multiplier(0.84) == 1.5

    def test_final_stretch_85pct_is_2x(self):
        assert _late_season_multiplier(0.85) == 2.0
        assert _late_season_multiplier(1.0) == 2.0

    def test_above_1_clamps_at_2x(self):
        # season_progress > 1.0 (e.g. extra-time tiebreakers) shouldn't
        # produce surprise multipliers; just stay at 2x.
        assert _late_season_multiplier(1.5) == 2.0


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

    def test_late_season_doubles_stakes(self):
        # Phase A bumped the weights.stakes default to 0.5 (was 2.0)
        # to compensate for compute_team_stakes returning
        # leverage_in_[0,1] × weight instead of the un-weighted
        # proximity points. Math: 2.0 raw × 2x late × 0.5 weight = 2.0.
        sig = GameSignals(stakes_a=2.0, season_progress=0.90)
        s = score_game(sig, Weights())
        assert s.breakdown["stakes"] == 2.0

    def test_mid_season_no_late_mult(self):
        # 2.0 × 1.0 × 0.5 = 1.0.
        sig = GameSignals(stakes_a=2.0, season_progress=0.5)
        s = score_game(sig, Weights())
        assert s.breakdown["stakes"] == 1.0

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


class TestScoreGameImpactInheritance:
    """B.1 / issue #11: impact-on-favorite inherits the favorite's own
    stakes magnitude, scaled by how close the game's teams sit to the
    favorite's slot, and amplified by the same late-season multiplier
    the favorite's own stakes would receive. The pre-B.1 flat +1 per
    favorite buried West Ham vs Leeds (Tottenham relegation pivot) at
    #8 — these tests prove the magnitude now tracks the urgency."""

    def test_distance_zero_full_inheritance(self):
        # d=0 means a game team sits exactly in the favorite's slot →
        # maximum swap risk → no proximity decay. With weights.impact_favorite
        # = 1.0 default and no late_mult, impact_pts = fav_stakes_raw exactly.
        sig = GameSignals(
            impact_on_favorites=[("Tottenham", 5.0, 0)],
            season_progress=0.0,
        )
        s = score_game(sig, Weights())
        assert s.breakdown["impact_on_favorite"] == 5.0

    def test_distance_decay_to_quarter_at_cap(self):
        # d == _IMPACT_PROXIMITY_CAP (3) → leverage = 1/(3+1) = 0.25.
        # 5.0 × 0.25 × 1.0 × 1.0 = 1.25.
        sig = GameSignals(
            impact_on_favorites=[("Tottenham", 5.0, _IMPACT_PROXIMITY_CAP)],
            season_progress=0.0,
        )
        s = score_game(sig, Weights())
        assert s.breakdown["impact_on_favorite"] == 1.25

    def test_late_season_doubles_impact(self):
        # 5.0 × 1.0 (d=0) × 2.0 (late) × 1.0 weight = 10.0.
        sig = GameSignals(
            impact_on_favorites=[("Tottenham", 5.0, 0)],
            season_progress=0.90,
        )
        s = score_game(sig, Weights())
        assert s.breakdown["impact_on_favorite"] == 10.0

    def test_impact_tracks_own_stakes_at_same_late_mult(self):
        # The whole point of B.1: a game IMPACTING a favorite at the
        # relegation cutoff should score the SAME stakes magnitude as
        # the favorite's own game at the same matchday (modulo proximity
        # decay and weights.impact_favorite vs weights.stakes).
        late = 0.90
        fav_stakes_raw = 5.0
        # Game where the favorite plays (their own stakes block fires).
        own_sig = GameSignals(stakes_a=fav_stakes_raw, season_progress=late)
        own = score_game(own_sig, Weights())
        # Other game that impacts the favorite at d=0 (a team sits in the
        # favorite's slot — maximal pivot risk).
        impact_sig = GameSignals(
            impact_on_favorites=[("Fav", fav_stakes_raw, 0)],
            season_progress=late,
        )
        impact = score_game(impact_sig, Weights())
        # Both apply the same late_mult. The remaining ratio is
        # weights.stakes vs weights.impact_favorite — both currently 0.5
        # and 1.0 respectively, so impact == 2 × stakes-contribution at d=0.
        # Stakes contribution: 5.0 × 2.0 × 0.5 = 5.0
        # Impact contribution: 5.0 × 1.0 × 2.0 × 1.0 = 10.0
        # If late_mult ever desyncs between blocks, this ratio breaks.
        ratio = impact.breakdown["impact_on_favorite"] / own.breakdown["stakes"]
        weights_ratio = Weights().impact_favorite / Weights().stakes
        assert abs(ratio - weights_ratio) < 0.01

    def test_multiple_affected_favorites_sum(self):
        # Two affected favorites at d=1 each, stakes 5.0 + 3.0.
        # leverage = (3+1-1)/(3+1) = 0.75. Contribution = (5.0 + 3.0) × 0.75 = 6.0.
        sig = GameSignals(
            impact_on_favorites=[("A", 5.0, 1), ("B", 3.0, 1)],
            season_progress=0.0,
        )
        s = score_game(sig, Weights())
        assert s.breakdown["impact_on_favorite"] == 6.0

    def test_zero_stakes_zero_contribution(self):
        # A favorite locked-in/out of every threshold (stakes_raw=0) and
        # still proximate → 0 inheritance. The signal is silent rather
        # than firing a flat +1 like pre-B.1.
        sig = GameSignals(
            impact_on_favorites=[("Tottenham", 0.0, 0)],
            season_progress=0.90,
        )
        s = score_game(sig, Weights())
        assert "impact_on_favorite" not in s.breakdown or s.breakdown["impact_on_favorite"] == 0.0

    def test_weight_multiplies_in(self):
        # weights.impact_favorite acts as a final user-facing scaler.
        # 5.0 × 1.0 × 1.0 × 2.5 = 12.5.
        sig = GameSignals(
            impact_on_favorites=[("Tottenham", 5.0, 0)],
            season_progress=0.0,
        )
        s = score_game(sig, Weights(impact_favorite=2.5))
        assert s.breakdown["impact_on_favorite"] == 12.5

    def test_notes_extract_names_from_tuples(self):
        # The post-B.1 notes formatting must unpack tuples; if score_game
        # ever regressed to joining tuples directly, the note would
        # become "affects favorite: ('Tottenham', 5.0, 0)".
        sig = GameSignals(
            impact_on_favorites=[("Tottenham Hotspur FC", 5.0, 0)],
            season_progress=0.0,
        )
        s = score_game(sig, Weights())
        # Find the impact note
        impact_notes = [n for n in s.notes if "affects favorite" in n]
        assert len(impact_notes) == 1
        assert "Tottenham Hotspur FC" in impact_notes[0]
        assert "(" not in impact_notes[0]  # no raw tuple repr


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
        # The note format reveals which path fired — closeness path says
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
    def test_rank_pair_normalized_low_first(self):
        sig = GameSignals(rank_a=9, rank_b=3)
        score = score_game(GameSignals(rank_a=9, rank_b=3), Weights())
        name = format_channel_name("EPL", sig, score, "Manchester United", "Brentford FC")
        # Should render "3v9" not "9v3"
        assert " 3v9 " in name

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
        assert "Brentford at Manchester United" in name

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


class TestStripTeamSuffix:
    def test_strips_fc(self):
        assert strip_team_suffix("Brentford FC") == "Brentford"

    def test_strips_afc(self):
        assert strip_team_suffix("Wrexham AFC") == "Wrexham"

    def test_idempotent(self):
        assert strip_team_suffix("Brentford") == "Brentford"

    def test_keeps_compound_when_not_suffix(self):
        # "City" is a qualifier (Hull City is its own team), not a strippable
        # club-tag — must NOT be stripped.
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
        # Man United is closer (1 spot away) — narrative should reference it
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
            stakes_thresholds=["title"],
            tournament_stage="FINAL",
            rank_a=1, rank_b=2, rank_source="poll",
        )
        assert tag == "Final"

    def test_stakes_picks_label(self):
        tag = pick_tagline(
            score_breakdown={"stakes": 8.0},
            favorites_matched=[],
            spread=None,
            stakes_thresholds=["title", "UCL"],
            tournament_stage=None,
            rank_a=3, rank_b=9, rank_source="standings",
        )
        assert tag == "title / UCL race"

    def test_poll_rank_pair(self):
        tag = pick_tagline(
            score_breakdown={"rank_pair": 5.0},
            favorites_matched=[],
            spread=None,
            stakes_thresholds=[],
            tournament_stage=None,
            rank_a=2, rank_b=4, rank_source="poll",
        )
        assert tag == "top-5 showdown"

    def test_standings_rank_pair_dropped(self):
        # For league standings (every team is "ranked"), rank-pair tagline
        # must be dropped — it's noise. Should fall through to other signals.
        tag = pick_tagline(
            score_breakdown={"rank_pair": 5.0},
            favorites_matched=[],
            spread=None,
            stakes_thresholds=[],
            tournament_stage=None,
            rank_a=2, rank_b=4, rank_source="standings",
        )
        assert tag == ""  # nothing else fired

    def test_falls_back_to_toss_up(self):
        tag = pick_tagline(
            score_breakdown={"close_game": 3.0},
            favorites_matched=[],
            spread=1.5,
            stakes_thresholds=[],
            tournament_stage=None,
            rank_a=None, rank_b=None, rank_source="poll",
        )
        assert tag == "toss-up"


class TestBuildWhyTextRankSource:
    def test_poll_keeps_top_n_label(self):
        why = build_why_text(
            rank_home=2, rank_away=4,
            favorites_matched=[], score_breakdown={"rank_pair": 5.0},
            rank_source="poll",
        )
        assert "top-5" in why

    def test_standings_drops_top_n_label(self):
        # For EPL etc, "both top-5" is meaningless when there are 20 teams.
        why = build_why_text(
            rank_home=2, rank_away=4,
            favorites_matched=[], score_breakdown={"rank_pair": 5.0},
            rank_source="standings",
        )
        assert "top-5" not in why
        assert "both ranked" not in why


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
