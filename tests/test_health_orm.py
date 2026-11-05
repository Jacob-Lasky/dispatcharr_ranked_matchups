"""Real cache/guide/link and feed-state reads for push health."""
import importlib.util
import os
from datetime import timedelta

import pytest

spec = importlib.util.spec_from_file_location("_health_kit", os.path.join(os.path.dirname(__file__), "test_apply_empty_slate.py"))
kit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(kit)


def _health():
    from dispatcharr_ranked_matchups import health
    return health


def scenario_healthy_pipeline_and_real_links(env):
    channel, _ = kit._seed_source_channel(env)
    kit._publish(env, [kit._row(401, kit._now() - timedelta(minutes=10), channel.id)])
    assert _health().pipeline_problems({"status": "ok"}, kit._now() - timedelta(minutes=1), 1500, 200) == []


def scenario_stale_matches_even_after_an_ok_run(env):
    kit._write_cache(env, [], refreshed_at=kit._now() - timedelta(days=1))
    problems = _health().pipeline_problems({"status": "ok"}, kit._now() - timedelta(minutes=1), 1500, 200)
    assert any("stale match cache" in p for p in problems), problems


def scenario_m3u_update_after_matching_is_stale(env):
    channel, _ = kit._seed_source_channel(env)
    kit._publish(env, [kit._row(401, kit._now() + timedelta(hours=1), channel.id)])
    env.m.M3UAccount.objects.update(updated_at=kit._now() + timedelta(seconds=1))
    problems = _health().pipeline_problems({"status": "ok"}, kit._now() - timedelta(minutes=1), 1500, 200)
    assert any("stale match cache" in p for p in problems), problems


def scenario_streamless_live_channel_in_published_guide_even_if_missing_from_cache(env):
    channel, _ = kit._seed_source_channel(env)
    owned = kit._publish(env, [kit._row(401, kit._now() - timedelta(minutes=10), channel.id)])["ranked_matchups:CFB:401"]
    env.m.ChannelStream.objects.filter(channel=owned).delete()
    kit._write_cache(env, [])
    problems = _health().pipeline_problems({"status": "ok"}, kit._now() - timedelta(minutes=1), 1500, 200)
    assert any("0 streams" in p and str(owned.id) in p for p in problems), problems


def scenario_future_placeholder_is_not_a_live_failure(env):
    kit._publish(env, [kit._row(401, kit._now() + timedelta(hours=1), None)])
    assert _health()._live_empty_channels(env.plugin, kit._read_cache(env), kit._now()) == []


def scenario_timeout_cause_and_elapsed_ceiling_are_reported(env):
    kit._write_cache(env, [])
    problems = _health().pipeline_problems({"status": "error", "message": "scoring timed out"},
                                         kit._now() - timedelta(minutes=1), 1500, 1501)
    assert "pipeline error: scoring timed out" in problems
    assert any("1500s ceiling" in p for p in problems)


def scenario_m3u_and_epg_errors_exclude_builtin_inactive_and_dummy(env):
    m = env.m
    m.M3UAccount.objects.create(name="custom", locked=True, status="error")
    m.M3UAccount.objects.create(name="inactive", is_active=False, status="error")
    m.M3UAccount.objects.create(name="Provider", status="error")
    m.EPGSource.objects.create(name="Guide", source_type="xmltv", status="error")
    m.EPGSource.objects.create(name="Dummy", source_type="dummy", status="error")
    problems = _health().refresh_problems()
    assert len(problems) == 2, problems
    assert "M3U" in problems[0] and "Provider" in problems[0]
    assert "EPG" in problems[1] and "Guide" in problems[1]


def scenario_overdue_scheduled_feeds_but_manual_feeds_have_no_deadline(env):
    now = kit._now()
    env.m.M3UAccount.objects.create(name="Provider", status="fetching", refresh_interval=4,
                                    updated_at=now - timedelta(hours=5))
    env.m.EPGSource.objects.create(name="Guide", source_type="xmltv", status="success", refresh_interval=12,
                                   updated_at=now - timedelta(hours=14))
    env.m.EPGSource.objects.create(name="Manual", source_type="xmltv", status="success", refresh_interval=0,
                                   updated_at=now - timedelta(days=30))
    problems = _health().refresh_problems(now)
    assert len(problems) == 2 and all("overdue" in p for p in problems), problems


def scenario_refresh_grace_and_never_refreshed(env):
    now = kit._now()
    env.m.M3UAccount.objects.create(name="Within grace", status="parsing", refresh_interval=4,
                                    updated_at=now - timedelta(hours=4, minutes=20))
    env.m.EPGSource.objects.create(name="Never", source_type="xmltv", refresh_interval=12)
    problems = _health().refresh_problems(now)
    assert len(problems) == 1 and "Never" in problems[0] and "never completed" in problems[0], problems


SCENARIOS = kit.collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return kit.run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_health_orm(harness_results, name):
    assert harness_results.get(name, "scenario did not run") is None, harness_results.get(name)
