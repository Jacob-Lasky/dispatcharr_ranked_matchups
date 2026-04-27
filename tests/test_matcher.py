"""Tests for matcher.py — pure-logic helpers (no Anthropic, no Django)."""

from datetime import datetime, timezone

import pytest

from dispatcharr_ranked_matchups.matcher import (
    ChannelCandidate,
    _extract_json,
    _regex_filter,
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
