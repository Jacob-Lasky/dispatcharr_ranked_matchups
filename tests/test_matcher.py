"""Tests for matcher.py: pure-logic helpers (no Anthropic, no Django)."""

from datetime import datetime, timezone

import pytest

from dispatcharr_ranked_matchups.matcher import (
    ChannelCandidate,
    _extract_json,
    _is_preview_title,
    _kw_hit,
    _regex_filter,
    _regex_filter_channel_name,
    _strip_preview_titles,
    _strong_team_keywords,
    _team_keywords,
    both_teams_in_one_segment,
    match_games_to_channels,
    select_streams_for_game,
)


class TestTeamKeywords:
    def test_single_word(self):
        kws = _team_keywords("Wrexham")
        assert "Wrexham" in kws

    def test_two_words_appends_last(self):
        kws = _team_keywords("Notre Dame")
        assert "Notre Dame" in kws
        assert "Dame" in kws

    def test_state_suffix_skipped(self):
        kws = _team_keywords("Penn State")
        # "State" is too generic: must be excluded so we don't false-match
        # any other "Foo State" team.
        assert "State" not in kws
        assert "Penn State" in kws

    def test_college_suffix_skipped(self):
        kws = _team_keywords("Boston College")
        assert "College" not in kws

    def test_numeric_rematch_last_word_skipped(self):
        # Regression (UFC 329 wildcard): a field-event title ending in a rematch
        # number must NOT reduce to the bare number as a keyword. '2' is a
        # substring of a huge fraction of channel names/numbers, so it dragged
        # 'UFC 329: McGregor vs. Holloway 2' onto 8214 unrelated streams.
        kws = _team_keywords("UFC 329: McGregor vs. Holloway 2")
        assert "2" not in kws
        # The real discriminators survive.
        assert "UFC 329: McGregor vs. Holloway 2" in kws
        assert "UFC 329:" in kws

    def test_boxing_promo_number_last_word_skipped(self):
        # Same shape from the boxing feed: '... IBA Pro 19' -> '19'.
        kws = _team_keywords("Gassiev vs. Kadiru: IBA Pro 19")
        assert "19" not in kws

    def test_short_last_word_skipped(self):
        # A 1-2 char last-word token (e.g. a Roman-numeral rematch marker) is
        # also a wildcard and must not be emitted as a standalone keyword.
        kws = _team_keywords("Ali vs. Frazier II")
        assert "II" not in kws

    def test_first_two_words_added(self):
        kws = _team_keywords("North Carolina State")
        assert "North Carolina" in kws

    def test_two_word_with_club_suffix_strips(self):
        # Regression: 'Brentford FC' must produce 'Brentford' as a keyword
        # so it matches channel names like 'EPL01: ... Brentford 27/04'.
        # Previously, the first-two-words rule duplicated the full name and
        # the bare team name was never in the keyword list.
        kws = _team_keywords("Brentford FC")
        assert "Brentford" in kws
        assert "Brentford FC" in kws

    def test_three_word_with_club_suffix_strips(self):
        kws = _team_keywords("Manchester United FC")
        assert "Manchester United" in kws
        assert "Manchester United FC" in kws
        # 'United' alone is suppressed as a generic: see
        # test_generic_soccer_suffix_not_a_keyword.

    def test_afc_suffix_stripped(self):
        kws = _team_keywords("Wrexham AFC")
        assert "Wrexham" in kws

    def test_no_duplicates(self):
        # Dedupe rule applies regardless of which fallback rules fire.
        kws = _team_keywords("Brentford FC")
        assert len(kws) == len(set(kws))

    def test_generic_soccer_suffix_not_a_keyword(self):
        # Regression: 'Manchester United' must NOT reduce to 'United' as a
        # standalone keyword. False-matched 'Brentford v West Ham United'
        # before the fix.
        kws = _team_keywords("Manchester United FC")
        assert "United" not in kws
        assert "Manchester United" in kws

    def test_generic_city_not_a_keyword(self):
        # Same false-positive class for 'City' (Manchester/Leicester/Hull/
        # Cardiff/Swansea/etc).
        kws = _team_keywords("Manchester City FC")
        assert "City" not in kws
        assert "Manchester City" in kws

    def test_villa_and_hotspur_treated_as_generics(self):
        # 'Villa' and 'Hotspur' are listed as generic second-words even
        # though only one EPL club uses each. Reason: providers consistently
        # write the full 'Aston Villa' / 'Tottenham Hotspur' in channel
        # names, so we don't need the bare last-word fallback, and dropping
        # it avoids weird substring hits ("Hotspur Way", stadium names, etc).
        kws_villa = _team_keywords("Aston Villa FC")
        kws_spurs = _team_keywords("Tottenham Hotspur FC")
        assert "Aston Villa" in kws_villa
        assert "Villa" not in kws_villa
        assert "Tottenham Hotspur" in kws_spurs
        assert "Hotspur" not in kws_spurs

    def test_college_generics_still_skipped(self):
        # Existing college-football skips (state/college/university) preserved.
        assert "State" not in _team_keywords("Penn State")
        assert "College" not in _team_keywords("Boston College")

    @pytest.mark.parametrize(
        "team,city,nickname",
        [
            ("New York Yankees", "New York", "Yankees"),
            ("New York Giants", "New York", "Giants"),
            ("Los Angeles Lakers", "Los Angeles", "Lakers"),
            ("Tampa Bay Buccaneers", "Tampa Bay", "Buccaneers"),
            ("Kansas City Chiefs", "Kansas City", "Chiefs"),
            ("San Francisco Giants", "San Francisco", "Giants"),
            ("New England Patriots", "New England", "Patriots"),
        ],
    )
    def test_bare_city_not_a_keyword_when_nickname_survives(self, team, city, nickname):
        # Regression (#162): the first-two-words rule emitted the bare CITY for
        # every multi-word-city franchise, and Path A admits a candidate on any
        # ONE keyword, so 'New York' dragged the NFL Giants and Jets onto an MLB
        # Yankees game. The nickname is the discriminator; the city is a
        # wildcard across every franchise in town.
        # The city must not be able to admit a candidate on its OWN...
        assert city not in _strong_team_keywords(team)
        # ...but it stays available to the both-teams gates, where requiring the
        # other side too makes a city-only feed name a correct match.
        assert city in _team_keywords(team)
        # The real discriminators survive, so recall is unchanged.
        kws = _team_keywords(team)
        assert team in kws
        assert nickname in kws

    def test_geographic_prefix_kept_when_last_word_is_generic(self):
        # The mirror of the rule above, and why it is conditional rather than a
        # blanket drop: 'State' is suppressed as a generic, so 'North Carolina'
        # is the ONLY relaxed form left and must stay.
        kws = _team_keywords("North Carolina State")
        assert "North Carolina" in kws
        assert "State" not in kws
        # And it must be STRONG: with nothing else left, it has to be able to
        # admit a candidate on its own.
        assert "North Carolina" in _strong_team_keywords("North Carolina State")

    def test_bare_city_not_reintroduced_via_suffix_strip(self):
        # Second shape of the same #162 defect. 'New York City FC' strips its
        # club tag to 'New York City', whose 'City' is dropped as a generic,
        # which used to let the first-two-words rule re-emit the bare 'New York'
        # for a club sharing the metro with the Yankees, Mets, Giants and Jets.
        # The stripped form is already a relaxed keyword, so the prefix is not
        # needed.
        assert "New York" not in _strong_team_keywords("New York City FC")
        kws = _team_keywords("New York City FC")
        assert "New York City" in kws
        assert "New York City FC" in kws

    def test_place_last_word_suppressed_promotes_real_discriminator(self):
        # MLS writes one club as 'Red Bull New York'. 'York' is 4 chars so it
        # survived the weak-token guard, yet it substring-matches every New York
        # franchise in the guide (#162). Suppressing it promotes 'Red Bull',
        # which actually identifies the club.
        kws = _team_keywords("Red Bull New York")
        assert "York" not in kws
        assert "Red Bull" in kws
        assert "Red Bull New York" in kws

    def test_club_short_form_survives_prefix_drop(self):
        # The prefix rule must not cost a real short name: 'West Ham' comes from
        # the suffix-stripped form and the alias table, not the prefix rule.
        kws = _team_keywords("West Ham United FC")
        assert "West Ham" in kws
        assert "West Ham United" in kws

    def test_event_prefix_kept_when_last_word_is_weak(self):
        # Same conditionality for field events: the trailing rematch number is
        # dropped as weak, so the 'UFC 329:' prefix is the only relaxed form.
        kws = _team_keywords("UFC 329: McGregor vs. Holloway 2")
        assert "UFC 329:" in kws
        assert "2" not in kws


class TestWeakKeywordsStillMatchBothTeamsGates:
    """The recall half of #162: weak keywords must stay usable when BOTH sides hit.

    Caught by replaying the real slate: an earlier cut of this fix dropped the
    bare city everywhere, which lost a CORRECT Tier-1 match because providers do
    name feeds by city alone. Requiring both sides is what makes that safe.
    """

    def _chan(self, name):
        return ChannelCandidate(
            channel_id=1, channel_name=name, program_title="",
            program_start=datetime(2026, 8, 1, 19, 25, tzinfo=timezone.utc),
            program_end=datetime(2026, 8, 1, 21, 25, tzinfo=timezone.utc),
        )

    def test_city_only_feed_name_still_matches_both_teams_gate(self):
        # Real feed + real fixture from a live instance: the away side is only
        # ever written 'San Jose' here, so the weak keyword is the only way in.
        cands = [self._chan("(Apple) (MLS) 006 |  Cincinnati vs. San Jose  (2026-08-01 19:25:00)")]
        got = _regex_filter_channel_name(cands, "FC Cincinnati", "San Jose Earthquakes")
        assert len(got) == 1

    def test_city_only_name_does_not_match_one_sided(self):
        # The same weak keyword must NOT be enough on its own: a Yankees game
        # cannot claim a channel that merely says 'New York'.
        cands = [self._chan("New York Giants")]
        assert _regex_filter_channel_name(cands, "New York Yankees", "Chicago White Sox") == []
        # Single-sided (field-event) mode is a single-keyword admission, so it is
        # gated on strong keywords and must also refuse.
        assert _regex_filter_channel_name(cands, "New York Yankees", None) == []


class TestChannelNameSegmentGate:
    """#129 mode 2: a team alias sitting in a channel's FEED LABEL must not pair
    with an opponent named in the matchup body to fake a Tier-1 match.

    The reported false positive: the 'Australia at United States' game matched
    'USA Soccer07: Australia vs Turkey ( TSN1 Feed )', where 'USA' is the
    provider's feed label and the real fixture on that channel is
    Australia vs TURKEY. 1.8.0 built both_teams_in_one_segment for exactly this
    shape but wired it only into Path C (stream names), leaving the older
    channel-name path (Path B) ungated. This class pins the gate on BOTH.
    """

    def _chan(self, name, cid=1):
        return ChannelCandidate(
            channel_id=cid, channel_name=name, program_title="",
            program_start=datetime(2026, 6, 13, tzinfo=timezone.utc),
            program_end=datetime(2026, 6, 13, tzinfo=timezone.utc),
        )

    def test_feed_label_alias_does_not_pair_with_body_opponent(self):
        cands = [self._chan("USA Soccer07: Australia vs Turkey ( TSN1 Feed )")]
        assert _regex_filter_channel_name(cands, "United States", "Australia") == []

    def test_the_real_fixture_on_that_same_channel_still_matches(self):
        # The recall control for the test above: the channel is a legitimate
        # Australia vs Turkey feed and must keep matching THAT game.
        cands = [self._chan("USA Soccer07: Australia vs Turkey ( TSN1 Feed )")]
        assert len(_regex_filter_channel_name(cands, "Turkey", "Australia")) == 1

    def test_kickoff_time_is_not_a_segment_boundary(self):
        # The ':' in '02:00' must not split the matchup across segments.
        cands = [self._chan("FIFA World Cup 2026 06: USA 02:00 Paraguay")]
        assert len(_regex_filter_channel_name(cands, "United States", "Paraguay")) == 1

    def test_pipe_label_prefix_still_matches(self):
        cands = [self._chan("AU (STAN 01) | Manchester United v Brentford PL 2025/26")]
        out = _regex_filter_channel_name(cands, "Manchester United FC", "Brentford FC")
        assert len(out) == 1

    def test_channel_name_gate_agrees_with_path_c_gate(self):
        # Path B and Path C must not disagree about the same text. This is the
        # contract that was violated: the helper existed and one caller used it.
        name = "USA Soccer07: Australia vs Turkey ( TSN1 Feed )"
        home, away = _team_keywords("United States"), _team_keywords("Australia")
        assert both_teams_in_one_segment(name, home, away) is False
        assert _regex_filter_channel_name([self._chan(name)], "United States", "Australia") == []


class TestShortKeywordWordBoundaries:
    """#129 mode 1: short aliases are substring-matched into unrelated words.

    team_aliases.json carries 2-4 character abbreviations ('NE', 'OM', 'GB',
    'BOS', 'PIT', 'Real'), and _is_weak_last_word only screens the LAST-WORD
    fallback, so aliases reach _kw_hit unfiltered. As bare substrings they are
    close to wildcards: 'NE' is inside 'Sportsnet', 'Tennessee', and 'Network'.
    On Path A's single-keyword title pre-filter that hands Tier 3 an arbitrary
    candidate pool for every Patriots game, which is the shape that produced
    the reported 'SportsGrid' match.
    """

    @pytest.mark.parametrize("team,text,alias", [
        ("New England Patriots", "Sportsnet 1 HD", "NE"),
        ("New England Patriots", "Tennessee Titans at Buffalo", "NE"),
        ("Olympique de Marseille", "Roma vs Lazio", "OM"),
        ("Boston Celtics", "WC 12: Bosnia vs Morocco", "BOS"),
        ("Green Bay Packers", "GBN News HD", "GB"),
        ("Pittsburgh Steelers", "Capital Sports 4", "PIT"),
        ("Real Madrid", "CF Montreal vs Toronto", "Real"),
    ])
    def test_short_alias_does_not_match_inside_a_word(self, team, text, alias):
        assert alias in [k for k in _strong_team_keywords(team)], (
            f"fixture drift: {alias!r} is no longer an alias of {team!r}"
        )
        assert not _kw_hit(text, _strong_team_keywords(team))

    @pytest.mark.parametrize("team,text", [
        ("New England Patriots", "NFL 04 | NE at BUF"),
        ("Green Bay Packers", "NFL 11: GB vs CHI"),
        ("Boston Celtics", "NBA 03 - BOS @ LAL"),
        ("Olympique de Marseille", "Ligue 1: OM - PSG"),
        ("Real Madrid", "LaLiga: Real Madrid vs Sevilla"),
        ("Pittsburgh Steelers", "PIT/CLE Sunday Night"),
    ])
    def test_short_alias_still_matches_as_a_standalone_token(self, team, text):
        # The recall control: abbreviations are in the alias file because real
        # providers use them, so the boundary must admit them when they stand
        # alone, including against '/', '-', '@' and end-of-string.
        assert _kw_hit(text, _strong_team_keywords(team))

    @pytest.mark.parametrize("name,home,away,collides", [
        # Both names are verbatim from a 6588-channel / 16415-stream corpus, and
        # both were real Tier-1 matches before this fix. They are the whole
        # measured cost of the change: a differential over that corpus across 33
        # fixtures dropped exactly these and gained nothing.
        ("Tennis 35: WTA Memphis & ATP Los Cabos: Svrcina, Dalibor - Darderi, Luciano @ 30 Jul 12:00 AM ET",
         "Dallas Cowboys", "Philadelphia Eagles", "DAL inside 'Dalibor'"),
        ("MiLB 25: MiLB A 08: Clearwater Threshers at Jupiter Hammerheads 29 @ Jul 06:30 PM ET",
         "Pittsburgh Steelers", "Cleveland Browns", "PIT inside 'Jupiter', CLE inside 'Clearwater'"),
    ])
    def test_real_corpus_false_positives_are_gone(self, name, home, away, collides):
        cands = [ChannelCandidate(
            channel_id=1, channel_name=name, program_title="",
            program_start=datetime(2026, 7, 29, tzinfo=timezone.utc),
            program_end=datetime(2026, 7, 29, tzinfo=timezone.utc),
        )]
        assert _regex_filter_channel_name(cands, home, away) == [], collides

    def test_long_keywords_keep_substring_semantics(self):
        # Only SHORT keywords get the boundary. Long ones stay substrings so a
        # suffix-stripped or possessive form still hits.
        assert _kw_hit("Manchester Uniteds title race", _team_keywords("Manchester United FC"))
        assert _kw_hit("Yankees' bullpen", _strong_team_keywords("New York Yankees"))

    def test_boundary_applies_through_the_shared_gates(self):
        # _kw_hit is the single source of truth, so the fix must reach every
        # caller. If a gate ever grows its own matcher these break.
        # 'NE' is inside 'Tennessee', so before the fix this Titans-at-Bills feed
        # read as a Patriots-at-Bills feed. The away side genuinely hits here, so
        # the alias collision is the only thing deciding the verdict.
        assert both_teams_in_one_segment(
            "Sportsnet: Tennessee Titans at Buffalo Bills",
            _team_keywords("New England Patriots"),
            _team_keywords("Buffalo Bills"),
        ) is False
        assert select_streams_for_game(
            ["Sportsnet 1 HD", "NFL 04 | NE at BUF"],
            _strong_team_keywords("New England Patriots"),
            _strong_team_keywords("Buffalo Bills"),
        ) == [False, True]


class TestSelectStreamsForGame:
    """The whole-channel stream gate (#162)."""

    # STRONG keywords, mirroring the apply call site: this gate keeps a stream on
    # a SINGLE side's hit, so a weak place-name keyword here would let 'New York
    # Giants' pass as a Yankees feed, which is the bug (#162). If this fixture is
    # ever relaxed to _team_keywords, the gate stops working in production while
    # the tests still pass.
    YANKEES = _strong_team_keywords("New York Yankees")
    WHITE_SOX = _strong_team_keywords("Chicago White Sox")

    def _mask(self, names):
        return select_streams_for_game(names, self.YANKEES, self.WHITE_SOX)

    def test_generic_broadcaster_streams_all_kept(self):
        # THE non-regression that matters: a broadcaster channel names no team
        # on any stream, so naming no team cannot be evidence against a stream.
        # Inverting the gate would strip this channel bare.
        names = ["MLB Network HD", "MLB Network FHD", "MLB Network SD"]
        assert self._mask(names) == [True, True, True]

    def test_other_matchup_streams_dropped_when_ours_present(self):
        # The reported bug: dedicated NFL feeds riding along on a channel that
        # also carries this game's feed.
        names = [
            "MLB 23 | New York Yankees at Chicago White Sox AWAY",
            "NFL 05 | New York Giants at Chicago Bears",
            "NFL 06 | New York Jets at New England Patriots",
        ]
        assert self._mask(names) == [True, False, False]

    def test_single_matching_stream_kept(self):
        names = ["ESPN UNLTD 101: Baseball: Yankees vs. White Sox"]
        assert self._mask(names) == [True]

    def test_one_side_named_is_enough(self):
        # A feed naming only one side (a team-branded home broadcast of this
        # game) is still this game's feed.
        names = ["Yankees Broadcast", "NBA 07 | Lakers at Celtics"]
        assert self._mask(names) == [True, False]

    def test_unnamed_stream_kept_alongside_named_ones(self):
        # A blank name is not evidence that a stream belongs to another game.
        names = ["Yankees vs White Sox HD", "", "   "]
        assert self._mask(names) == [True, True, True]

    def test_empty_channel_returns_empty_mask(self):
        assert self._mask([]) == []

    def test_field_event_single_sided_gate(self):
        # away_kws empty (the "Field" sentinel has no keywords): the gate keys on
        # the event name alone and must not treat the empty away side as a hit.
        event = _team_keywords("UFC 329: McGregor vs. Holloway 2")
        names = ["UFC 329: McGregor vs. Holloway 2 Main Card", "NBA 07 | Lakers at Celtics"]
        assert select_streams_for_game(names, event, []) == [True, False]


class TestRegexFilter:
    def _cand(self, title: str) -> ChannelCandidate:
        return ChannelCandidate(
            channel_id=1,
            channel_name="ESPN",
            program_title=title,
            program_start=datetime(2026, 4, 27, tzinfo=timezone.utc),
            program_end=datetime(2026, 4, 27, tzinfo=timezone.utc),
        )

    def test_both_teams_required(self):
        cands = [
            self._cand("Penn State at Ohio State"),
            self._cand("Penn State pregame show"),
            self._cand("Random College Football"),
        ]
        out = _regex_filter(cands, "Penn State", "Ohio State")
        # First has both Penn State + Ohio State; second has only one.
        # Note: since "State" is excluded as too-generic and "Penn"/"Ohio"
        # are the discriminating tokens, full-name match is the path.
        assert any(c.program_title.startswith("Penn State at Ohio State") for c in out)
        assert all("pregame" not in c.program_title for c in out)

    def test_no_match(self):
        cands = [self._cand("Some other program")]
        out = _regex_filter(cands, "Wrexham AFC", "Hull City AFC")
        assert out == []


class TestRegexFilterChannelName:
    """Regression for the 'Manchester United' team-channel false-positive.

    A team-branded home channel (channel_name='Manchester United') with EPG
    program_title 'Next Game: Brentford FC @ Manchester United on...' would
    pass the program-title regex filter, fool the LLM, and get picked as the
    'broadcaster' even though it isn't. The fix: require both team names in
    the CHANNEL NAME for a high-confidence match. Real match channels (e.g.
    'EPL01: Manchester United 20:00 Brentford') always satisfy this; team
    channels never do."""

    def _cand(self, channel_name: str, channel_id: int = 1) -> ChannelCandidate:
        return ChannelCandidate(
            channel_id=channel_id,
            channel_name=channel_name,
            program_title="any",
            program_start=datetime(2026, 4, 27, tzinfo=timezone.utc),
            program_end=datetime(2026, 4, 27, tzinfo=timezone.utc),
        )

    def test_team_channel_rejected(self):
        # 'Manchester United' channel does NOT contain 'Brentford': reject.
        cands = [self._cand("Manchester United")]
        out = _regex_filter_channel_name(cands, "Manchester United FC", "Brentford FC")
        assert out == []

    def test_real_match_channel_accepted(self):
        cands = [self._cand("EPL01: Manchester United 20:00 Brentford 27/04")]
        out = _regex_filter_channel_name(cands, "Manchester United FC", "Brentford FC")
        assert len(out) == 1

    def test_returns_all_provider_variants(self):
        # Same fixture across multiple provider channels: all returned for
        # the caller to stack as fallback streams.
        cands = [
            self._cand("EPL01: Manchester United 20:00 Brentford 27/04", 100),
            self._cand("AU (STAN 01) | Manchester United v Brentford PL 2025/26", 101),
            self._cand("USA Soccer01: Manchester United vs Brentford @ 03:00pm EDT", 102),
            self._cand("Random Sport Channel"),  # noise
        ]
        out = _regex_filter_channel_name(cands, "Manchester United FC", "Brentford FC")
        assert {c.channel_id for c in out} == {100, 101, 102}


class TestNationalTeamAliases:
    """#123: national-team channels name the matchup with broadcast forms the
    canonical name doesn't contain — FIFA-style 'USA' and Spanish exonyms
    ('Estados Unidos', 'Brasil'). Without aliases, _team_keywords('United
    States') = ['United States', 'States'], so the provider's
    'FIFA World Cup 2026 06: USA 02:00 Paraguay' channel (and its Spanish
    feeds) never match, leaving the marquee game streamless."""

    def test_united_states_expands_to_usa(self):
        kws = [k.lower() for k in _team_keywords("United States")]
        assert "usa" in kws
        assert "estados unidos" in kws

    def test_spanish_exonym_present(self):
        assert "brasil" in [k.lower() for k in _team_keywords("Brazil")]
        assert "alemania" in [k.lower() for k in _team_keywords("Germany")]

    def _cand(self, channel_name: str, cid: int) -> ChannelCandidate:
        return ChannelCandidate(
            channel_id=cid, channel_name=channel_name, program_title="",
            program_start=datetime(2026, 6, 13, tzinfo=timezone.utc),
            program_end=datetime(2026, 6, 13, tzinfo=timezone.utc),
        )

    def test_wc_channel_variants_all_match(self):
        # The 7 real provider channels for USA vs Paraguay (the 4 dedicated
        # feeds named with USA / Estados Unidos). Tier-1 must catch them all so
        # they stack as fallback streams.
        cands = [
            self._cand("FIFA World Cup 2026 06: USA 02:00 Paraguay", 1),
            self._cand("FIFA World Cup 2026 07: [4K] USA 02:00 Paraguay", 2),
            self._cand("FIFA World Cup 2026 08: Estados Unidos 02:00 Paraguay - En Espanol", 3),
            self._cand("TSN+ 16 : Spanish Feed: FIFA World Cup 2026: USA vs. Paraguay", 4),
            self._cand("Fox Sports 1", 5),  # noise: neither matchup team
        ]
        out = _regex_filter_channel_name(cands, "United States", "Paraguay")
        assert {c.channel_id for c in out} == {1, 2, 3, 4}


class TestPreviewTitleDetection:
    def test_next_game_is_preview(self):
        assert _is_preview_title("Next Game: Brentford @ Manchester United")

    def test_preview_keyword(self):
        assert _is_preview_title("Preview: Manchester United vs Brentford")

    def test_pregame_show(self):
        assert _is_preview_title("Pregame Show on ESPN")
        assert _is_preview_title("Pre-game coverage of the match")

    def test_postgame(self):
        assert _is_preview_title("Postgame Wrap-up")
        assert _is_preview_title("Post-game analysis")

    def test_real_broadcast_not_flagged(self):
        # A live match title should NOT be flagged as a preview.
        assert not _is_preview_title("Premier League: Manchester United vs Brentford")
        assert not _is_preview_title("EPL01: Manchester United 20:00 Brentford 27/04")

    def test_strip_removes_previews(self):
        cands = [
            ChannelCandidate(
                channel_id=1, channel_name="Manchester United",
                program_title="Next Game: Brentford @ Manchester United",
                program_start=datetime(2026, 4, 27, tzinfo=timezone.utc),
                program_end=datetime(2026, 4, 27, tzinfo=timezone.utc),
            ),
            ChannelCandidate(
                channel_id=2, channel_name="Sky Sports 1",
                program_title="Premier League: Manchester United vs Brentford",
                program_start=datetime(2026, 4, 27, tzinfo=timezone.utc),
                program_end=datetime(2026, 4, 27, tzinfo=timezone.utc),
            ),
        ]
        out = _strip_preview_titles(cands)
        assert [c.channel_id for c in out] == [2]


class TestExtractJson:
    def test_plain_object(self):
        assert _extract_json('{"a": 1}') == {"a": 1}

    def test_strips_code_fence_with_lang(self):
        text = '```json\n{"a": 1}\n```'
        assert _extract_json(text) == {"a": 1}

    def test_strips_bare_code_fence(self):
        text = '```\n{"a": 1}\n```'
        assert _extract_json(text) == {"a": 1}

    def test_finds_object_with_prose_around(self):
        text = 'Here is the result:\n{"a": 1, "b": 2}\nthat\'s all.'
        assert _extract_json(text) == {"a": 1, "b": 2}

    def test_garbage_raises(self):
        with pytest.raises(Exception):
            _extract_json("not json at all")


class TestTeamAliases:
    """#4: broadcaster-side abbreviations expand the keyword set so EPG
    titles using 'Man United' match the canonical 'Manchester United'."""

    def test_manchester_united_expansion(self):
        from dispatcharr_ranked_matchups.matcher import _team_keywords
        kws = _team_keywords("Manchester United")
        kws_lower = [k.lower() for k in kws]
        assert "manchester united" in kws_lower  # original name
        assert "man united" in kws_lower  # alias
        assert "man utd" in kws_lower
        # Generic-last-word skip protects against false matches:
        assert "united" not in kws_lower

    def test_manchester_united_fc_form_also_aliased(self):
        # FD.org returns 'Manchester United FC': alias key is the
        # FC-stripped form, but lookup tries both.
        from dispatcharr_ranked_matchups.matcher import _team_keywords
        kws = _team_keywords("Manchester United FC")
        kws_lower = [k.lower() for k in kws]
        assert "man united" in kws_lower
        assert "man utd" in kws_lower

    def test_paris_sg_expansion(self):
        from dispatcharr_ranked_matchups.matcher import _team_keywords
        kws = _team_keywords("Paris Saint-Germain")
        kws_lower = [k.lower() for k in kws]
        assert "paris sg" in kws_lower
        assert "psg" in kws_lower

    def test_no_alias_for_unknown_team_safe(self):
        from dispatcharr_ranked_matchups.matcher import _team_keywords
        kws = _team_keywords("Some Unknown FC")
        kws_lower = [k.lower() for k in kws]
        # Original + stripped should be there; no aliases.
        assert "some unknown fc" in kws_lower
        assert "some unknown" in kws_lower
        # No false-positive aliases like "Man Utd" leaking in.
        assert "man utd" not in kws_lower

    def test_regex_filter_matches_via_alias(self):
        # End-to-end: a Brentford vs Man United EPG title matches.
        from dispatcharr_ranked_matchups.matcher import _regex_filter, ChannelCandidate
        from datetime import datetime, timezone
        c = ChannelCandidate(
            channel_id=1, channel_name="DAZN UK 7",
            program_title="LIVE: Brentford vs Man United",
            program_start=datetime(2026, 5, 24, tzinfo=timezone.utc),
            program_end=datetime(2026, 5, 24, 2, tzinfo=timezone.utc),
        )
        out = _regex_filter([c], "Brentford FC", "Manchester United FC")
        assert len(out) == 1, "alias 'Man United' should match canonical 'Manchester United FC'"


class TestLoadTeamAliases:
    """Validate team_aliases.json shape so the JSON loader doesn't silently
    drop entries due to a missing list or stringly-typed value."""

    def test_json_is_valid(self):
        import json, os
        path = os.path.join(os.path.dirname(__file__), "..", "team_aliases.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        # At least a handful of marquee teams present.
        for k in ("Manchester United", "Paris Saint-Germain", "Real Madrid", "Boston Celtics"):
            assert k in raw

    def test_no_empty_alias_lists(self):
        import json, os
        path = os.path.join(os.path.dirname(__file__), "..", "team_aliases.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for key, vals in raw.items():
            if key.startswith("_"):
                continue
            assert isinstance(vals, list) and len(vals) > 0, \
                f"empty alias list for {key}"
            assert all(isinstance(v, str) and v.strip() for v in vals), \
                f"non-string or empty alias in {key}: {vals}"


class TestMatcherPromptIncludesForeignLanguageHints:
    """#4: MATCHER_SYSTEM_PROMPT should give the LLM hints for German,
    Spanish, Italian, French, Portuguese matchday/highlights vocabulary
    so it matches foreign-language EPG titles correctly."""

    def test_prompt_mentions_german_spieltag(self):
        from dispatcharr_ranked_matchups.matcher import MATCHER_SYSTEM_PROMPT
        assert "Spieltag" in MATCHER_SYSTEM_PROMPT

    def test_prompt_mentions_spanish_jornada(self):
        from dispatcharr_ranked_matchups.matcher import MATCHER_SYSTEM_PROMPT
        assert "jornada" in MATCHER_SYSTEM_PROMPT

    def test_prompt_mentions_italian_giornata(self):
        from dispatcharr_ranked_matchups.matcher import MATCHER_SYSTEM_PROMPT
        assert "giornata" in MATCHER_SYSTEM_PROMPT

    def test_prompt_mentions_french_journee(self):
        from dispatcharr_ranked_matchups.matcher import MATCHER_SYSTEM_PROMPT
        assert "journee" in MATCHER_SYSTEM_PROMPT


class _Game:
    """Minimal stand-in for a GameRow: only the attrs the matcher reads."""

    def __init__(self, home, away, sport_label="NCAAF"):
        self.home = home
        self.away = away
        self.sport_label = sport_label
        self.start_time = datetime(2026, 4, 27, tzinfo=timezone.utc)


def _cand(channel_id, channel_name, program_title):
    return ChannelCandidate(
        channel_id=channel_id,
        channel_name=channel_name,
        program_title=program_title,
        program_start=datetime(2026, 4, 27, tzinfo=timezone.utc),
        program_end=datetime(2026, 4, 27, 4, tzinfo=timezone.utc),
    )


class TestWidenStreamPool:
    """#108: an off-by-default `widen` flag stacks the non-chosen same-fixture
    candidates as fallback streams instead of discarding them.

    Scoping rule: only candidates that name BOTH teams (the `filtered` set the
    LLM disambiguates) get stacked. Single-team-keyword candidates (the
    zero-both-team `wider` path) are NOT stacked, because failing over to a
    stream that only mentions one team risks landing on a different game.
    """

    def _multi_both_team_setup(self, monkeypatch):
        # Both candidates name BOTH teams in the TITLE but neither in the
        # CHANNEL NAME, so tier-1 (regex_strict on channel name) misses and the
        # game falls to the LLM disambiguation path with filtered len == 2.
        cands = [
            _cand(100, "ESPN", "Penn State at Ohio State"),
            _cand(101, "FOX", "Penn State vs Ohio State"),
        ]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        # LLM picks channel 100 as the primary broadcast.
        monkeypatch.setattr(
            "dispatcharr_ranked_matchups.matcher._post_claude",
            lambda *a, **k: {"0": 100},
        )
        return games, lambda g: cands

    def test_llm_single_id_when_widen_off(self, monkeypatch):
        games, lookup = self._multi_both_team_setup(monkeypatch)
        results = match_games_to_channels(games, lookup, api_key="x", model="m")
        assert results[0].method == "llm"
        assert results[0].channel_id == 100
        assert results[0].channel_ids == [100]

    def test_llm_stacks_both_team_candidates_when_widen_on(self, monkeypatch):
        games, lookup = self._multi_both_team_setup(monkeypatch)
        results = match_games_to_channels(
            games, lookup, api_key="x", model="m", widen=True
        )
        assert results[0].method == "llm"
        # Primary stays the LLM's pick; the other both-team variant is stacked
        # after it as a fallback stream source.
        assert results[0].channel_id == 100
        assert results[0].channel_ids == [100, 101]

    def test_widen_does_not_stack_single_team_candidates(self, monkeypatch):
        # No candidate names BOTH teams (filtered == 0). The LLM still picks one
        # from the wider single-team-keyword pool, but widening must NOT stack
        # the others: they could be a different game featuring one of the teams.
        cands = [
            _cand(200, "ESPN", "Penn State football tonight"),
            _cand(201, "BTN", "Ohio State pregame coverage"),
        ]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        monkeypatch.setattr(
            "dispatcharr_ranked_matchups.matcher._post_claude",
            lambda *a, **k: {"0": 200},
        )
        results = match_games_to_channels(
            games, lambda g: cands, api_key="x", model="m", widen=True
        )
        assert results[0].channel_id == 200
        assert results[0].channel_ids == [200]

    def test_regex_strict_stacks_regardless_of_widen(self, monkeypatch):
        # Tier-1 channel-name both-team matches already stack all variants; the
        # widen flag must not change that established behavior (widen off here).
        cands = [
            _cand(300, "EPL01: Penn State 20:00 Ohio State", "Live"),
            _cand(301, "AU: Penn State v Ohio State", "Live"),
        ]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        results = match_games_to_channels(
            games, lambda g: cands, api_key="x", model="m"
        )
        assert results[0].method == "regex_strict"
        assert results[0].channel_ids == [300, 301]

    def test_regex_strict_dedupes_repeated_channel(self):
        # A single channel with two ProgramData rows both passing the filter
        # must appear once. Exercises the shared stacking helper's dedupe.
        cands = [
            _cand(300, "EPL01: Penn State v Ohio State", "First half"),
            _cand(300, "EPL01: Penn State v Ohio State", "Second half"),
            _cand(301, "AU: Penn State v Ohio State", "Live"),
        ]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        results = match_games_to_channels(
            games, lambda g: cands, api_key="x", model="m"
        )
        assert results[0].channel_ids == [300, 301]

    def test_fallback_first_stacks_both_team_when_widen_on(self):
        # No API key -> fallback_first. With widen on and every candidate
        # naming both teams, stack the rest behind the first as fallbacks.
        cands = [
            _cand(400, "ESPN", "Penn State at Ohio State"),
            _cand(401, "FOX", "Penn State vs Ohio State"),
        ]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        results = match_games_to_channels(
            games, lambda g: cands, api_key="", model="m", widen=True
        )
        assert results[0].method == "fallback_first"
        assert results[0].channel_id == 400
        assert results[0].channel_ids == [400, 401]

    def test_fallback_first_single_id_when_widen_off(self):
        cands = [
            _cand(400, "ESPN", "Penn State at Ohio State"),
            _cand(401, "FOX", "Penn State vs Ohio State"),
        ]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        results = match_games_to_channels(
            games, lambda g: cands, api_key="", model="m"
        )
        assert results[0].method == "fallback_first"


def _scand(stream_id, name):
    """A Path C stream-name candidate: channel_name == program_title == the
    stream name, channel_id a negative sentinel, stream_id set."""
    return ChannelCandidate(
        channel_id=-stream_id,
        channel_name=name,
        program_title=name,
        program_start=datetime(2026, 4, 27, tzinfo=timezone.utc),
        program_end=datetime(2026, 4, 27, 4, tzinfo=timezone.utc),
        stream_id=stream_id,
    )


class TestTier1Merge:
    """Tier-1 (channel-name both-team) must MERGE the program-title both-team
    matches behind it as fallback streams, not short-circuit and drop them.

    Regression: once one dedicated-feed channel (channel name has both teams)
    existed, the matcher returned ONLY that channel and silently dropped every
    EPG-confirmed broadcaster (FOX/TSN/BBC whose programme title names the game)
    whose stream pool used to back the matchup channel."""

    def test_strict_merges_program_title_broadcasters(self):
        cands = [
            # dedicated feed: both teams in the CHANNEL NAME (Tier-1 strict)
            _cand(10, "FIFA World Cup 2026 18: Penn State 02:00 Ohio State", "Live"),
            # broadcaster: both teams only in the PROGRAMME TITLE (Tier-2)
            _cand(20, "FOX Sports 1", "Penn State at Ohio State"),
            _cand(21, "TSN 1", "Penn State vs Ohio State"),
        ]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        results = match_games_to_channels(games, lambda g: cands, api_key="", model="m")
        assert results[0].method == "regex_strict"
        # Dedicated feed primary, broadcasters stacked behind it (no LLM).
        assert results[0].channel_ids == [10, 20, 21]
        assert results[0].stream_ids == []

    def test_strict_alone_unchanged_when_no_program_title_matches(self):
        # No broadcaster programme names both teams: behaviour is exactly the
        # pre-merge shape (strict variants only).
        cands = [
            _cand(10, "EPL01: Penn State 20:00 Ohio State", "Live"),
            _cand(11, "AU: Penn State v Ohio State", "Live"),
        ]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        results = match_games_to_channels(games, lambda g: cands, api_key="", model="m")
        assert results[0].channel_ids == [10, 11]
        assert results[0].stream_ids == []


class TestStreamGranularRouting:
    """Path C stream candidates (stream_id set) route to stream_ids, never to
    channel_ids, so the apply attaches the specific stream and not the parent
    channel's unrelated streams."""

    def test_pure_stream_match_via_tier1(self):
        # A stream naming both teams is a Tier-1 match (its channel_name IS the
        # stream name); it must land in stream_ids with empty channel_ids.
        cands = [_scand(500, "USA Soccer10: Penn State vs Ohio State 9pm")]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        results = match_games_to_channels(games, lambda g: cands, api_key="", model="m")
        assert results[0].method == "regex_strict"
        assert results[0].channel_ids == []
        assert results[0].stream_ids == [500]

    def test_channel_and_stream_match_split_correctly(self):
        cands = [
            _cand(10, "EPL01: Penn State v Ohio State", "Live"),       # whole channel
            _scand(500, "USA Soccer10: Penn State vs Ohio State 9pm"),  # one stream
        ]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        results = match_games_to_channels(games, lambda g: cands, api_key="", model="m")
        assert results[0].channel_ids == [10]
        assert results[0].stream_ids == [500]

    def test_stream_match_via_llm_routes_to_stream_ids(self, monkeypatch):
        # Channel name lacks both teams, programme title (= stream name) has them;
        # two such streams → LLM disambiguation. The LLM picks one by its
        # (negative-sentinel) channel_id, and it lands in stream_ids.
        cands = [
            _scand(500, "USA Soccer10: Penn State vs Ohio State"),
            _scand(501, "USA Soccer11: Penn State vs Ohio State"),
        ]
        games = [(_Game("Penn State", "Ohio State"), None, None)]
        # NOTE: these are Tier-1 strict (channel_name = stream name has both
        # teams), so they merge deterministically without the LLM. Assert that.
        results = match_games_to_channels(games, lambda g: cands, api_key="", model="m")
        assert results[0].method == "regex_strict"
        assert results[0].channel_ids == []
        assert sorted(results[0].stream_ids) == [500, 501]


class TestSingleSidedRegexFilters:
    """#127: passing team_b=None matches on team_a (the event name) alone,
    dropping the both-teams requirement for field events."""

    def test_program_title_single_sided(self):
        cands = [
            _cand(1, "ESPN+", "UFC 250: Topuria vs Gaethje"),
            _cand(2, "Random", "Premier League: Arsenal vs Chelsea"),
        ]
        out = _regex_filter(cands, "UFC 250: Topuria vs Gaethje")  # team_b defaults to None
        assert [c.channel_id for c in out] == [1]

    def test_channel_name_single_sided(self):
        cands = [
            _cand(1, "PPV: UFC 250 Topuria Gaethje", ""),
            _cand(2, "EPL01: Arsenal v Chelsea", ""),
        ]
        out = _regex_filter_channel_name(cands, "UFC 250: Topuria vs Gaethje")
        assert [c.channel_id for c in out] == [1]

    def test_both_teams_still_required_when_team_b_given(self):
        # Regression guard: two-team mode must keep the AND gate.
        cands = [_cand(1, "ESPN", "Penn State football tonight")]  # only one team
        assert _regex_filter(cands, "Penn State", "Ohio State") == []


class _FieldGame:
    """A field-event GameRow stand-in: away is the 'Field' sentinel and the
    source flag is set in extra, exactly as field_event.py emits."""

    def __init__(self, event_name):
        self.home = event_name
        self.away = "Field"
        self.sport_label = "UFC"
        self.start_time = datetime(2026, 4, 27, tzinfo=timezone.utc)
        self.extra = {"is_field_event": True}


class TestFieldEventMatching:
    """#127: field events (away='Field') match on the event name alone. Before
    the fix the both-teams gate fed the 'Field' sentinel into the keyword logic
    and nothing could ever match."""

    def test_tier1_channel_name_matches_event_name(self):
        cands = [_cand(10, "UFC 250: Topuria vs Gaethje (PPV)", "Live")]
        games = [(_FieldGame("UFC 250: Topuria vs Gaethje"), None, None)]
        results = match_games_to_channels(games, lambda g: cands, api_key="", model="m")
        assert results[0].method == "regex_strict"
        assert results[0].channel_id == 10

    def test_tier2_program_title_matches_event_name(self):
        # Channel name is generic; the EVENT name is in the program title only.
        cands = [_cand(20, "BT Sport 1", "UFC 250: Topuria vs Gaethje")]
        games = [(_FieldGame("UFC 250: Topuria vs Gaethje"), None, None)]
        results = match_games_to_channels(games, lambda g: cands, api_key="", model="m")
        assert results[0].method == "regex_unique"
        assert results[0].channel_id == 20

    def test_field_event_channel_naming_neither_fails_regex_tiers(self):
        # A channel that names neither the event nor anything relevant must NOT
        # pass the single-sided regex tiers (it may still reach the tier-3 LLM,
        # which is out of scope here).
        cands = [_cand(30, "Random Movie Channel", "Some Film")]
        games = [(_FieldGame("The Masters"), None, None)]
        results = match_games_to_channels(games, lambda g: cands, api_key="", model="m")
        assert results[0].method not in ("regex_strict", "regex_unique")

    def test_sentinel_alone_classifies_without_extra_flag(self):
        # A cached game dict may strip extra; the away sentinel alone must still
        # trigger single-sided matching.
        bare = _Game("The Masters", "Field", sport_label="Golf")  # no .extra attr
        cands = [_cand(40, "Golf Channel: The Masters", "Final round")]
        results = match_games_to_channels([(bare, None, None)], lambda g: cands,
                                          api_key="", model="m")
        assert results[0].method == "regex_strict"
        assert results[0].channel_id == 40
        assert results[0].channel_ids == [40]
