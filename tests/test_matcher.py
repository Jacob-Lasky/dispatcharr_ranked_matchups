"""Tests for matcher.py: pure-logic helpers (no Anthropic, no Django)."""

from datetime import datetime, timezone

import pytest

from dispatcharr_ranked_matchups.matcher import (
    ChannelCandidate,
    _extract_json,
    _is_preview_title,
    _regex_filter,
    _regex_filter_channel_name,
    _strip_preview_titles,
    _team_keywords,
    match_games_to_channels,
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
