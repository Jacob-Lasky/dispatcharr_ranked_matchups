"""Tests for matcher.py — pure-logic helpers (no Anthropic, no Django)."""

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
        # "State" is too generic — must be excluded so we don't false-match
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
        # 'United' alone is suppressed as a generic — see
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
        # 'Manchester United' channel does NOT contain 'Brentford' — reject.
        cands = [self._cand("Manchester United")]
        out = _regex_filter_channel_name(cands, "Manchester United FC", "Brentford FC")
        assert out == []

    def test_real_match_channel_accepted(self):
        cands = [self._cand("EPL01: Manchester United 20:00 Brentford 27/04")]
        out = _regex_filter_channel_name(cands, "Manchester United FC", "Brentford FC")
        assert len(out) == 1

    def test_returns_all_provider_variants(self):
        # Same fixture across multiple provider channels — all returned for
        # the caller to stack as fallback streams.
        cands = [
            self._cand("EPL01: Manchester United 20:00 Brentford 27/04", 100),
            self._cand("AU (STAN 01) | Manchester United v Brentford PL 2025/26", 101),
            self._cand("USA Soccer01: Manchester United vs Brentford @ 03:00pm EDT", 102),
            self._cand("Random Sport Channel"),  # noise
        ]
        out = _regex_filter_channel_name(cands, "Manchester United FC", "Brentford FC")
        assert {c.channel_id for c in out} == {100, 101, 102}


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
