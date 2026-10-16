"""Regression coverage for the optional EPG description renderer."""

import unittest
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "ranked_matchups_naming", Path(__file__).resolve().parents[1] / "naming.py"
)
naming = importlib.util.module_from_spec(spec)
spec.loader.exec_module(naming)


TEMPLATE = (
    "{league_full}{ · tournament} · ★{score}/10\\n"
    "{away_team}{ (rank_away)} v {home_team}{ (rank_home)}\\n"
    "{kickoff}{ · venue}\\n\\n{why}"
)


class DescriptionTemplateTests(unittest.TestCase):
    def test_optional_fields_and_escaped_line_breaks(self):
        ctx = naming.build_context(
            sport_prefix="EPL", sport_label="Premier League",
            away="Brentford", home="Manchester United",
            rank_away=None, rank_home=None, rank_source="standings",
            score_final=9.4, favorite=False, recording=False,
            tagline="title race", tournament="", venue=None,
            is_rivalry=False,
        )
        ctx["why"] = "A title race.\nSource: Sky Sports."
        self.assertEqual(naming.validate_template(TEMPLATE), [])
        self.assertEqual(
            naming.render_description(TEMPLATE, ctx),
            "Premier League · ★9.4/10\n"
            "Brentford v Manchester United\n\n"
            "A title race.\nSource: Sky Sports.",
        )

    def test_placeholder_warning_is_preserved_by_why(self):
        ctx = {"league_full": "NHL", "score": "8.0", "away_team": "Away",
               "home_team": "Home", "why": "Channel match pending: no feed yet."}
        rendered = naming.render_description(TEMPLATE, ctx)
        self.assertIn("Channel match pending: no feed yet.", rendered)
        self.assertNotIn("{", rendered)


if __name__ == "__main__":
    unittest.main()
