"""Real matcher and real stream-link writes for scoring-independent recovery."""
import importlib.util
import os
from datetime import timedelta

import pytest

spec = importlib.util.spec_from_file_location("_recovery_kit", os.path.join(os.path.dirname(__file__), "test_apply_empty_slate.py"))
kit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(kit)


def scenario_late_feed_is_attached_without_scoring(env):
    channel, stream = kit._seed_source_channel(env, "Georgia vs Alabama")
    row = kit._row(401, kit._now() - timedelta(minutes=10), None)
    row["channel_ids"] = []
    kit._write_cache(env, [row], refreshed_at=kit._now() - timedelta(days=4))
    result = env.plugin._action_rematch(kit._settings())
    assert result["status"] == "ok", result
    owned = kit._owned(env)["ranked_matchups:CFB:401"]
    assert list(owned.streams.values_list("id", flat=True)) == [stream.id]
    cache = kit._read_cache(env)
    assert cache["games"][0]["score"] == row["score"]
    assert cache["games"][0]["score_breakdown"] == row["score_breakdown"]
    assert cache["refreshed_at"] < cache["matched_at"]


def scenario_stale_empty_verdict_keeps_manual_repair(env):
    channel, stream = kit._seed_source_channel(env)
    row = kit._row(401, kit._now() - timedelta(minutes=10), channel.id)
    owned = kit._publish(env, [row])["ranked_matchups:CFB:401"]
    env.m.M3UAccount.objects.update(updated_at=kit._now())
    row.update(channel_id=None, channel_ids=[], stream_ids=[])
    kit._write_cache(env, [row], refreshed_at=kit._now() - timedelta(days=4))
    assert env.plugin._action_apply(kit._settings())["status"] == "ok"
    assert list(owned.streams.values_list("id", flat=True)) == [stream.id]


def scenario_fresh_empty_verdict_can_remove_links(env):
    channel, _ = kit._seed_source_channel(env)
    row = kit._row(401, kit._now() - timedelta(minutes=10), channel.id)
    owned = kit._publish(env, [row])["ranked_matchups:CFB:401"]
    env.m.M3UAccount.objects.update(updated_at=kit._now() - timedelta(hours=1))
    row.update(channel_id=None, channel_ids=[], stream_ids=[])
    kit._write_cache(env, [row])
    assert env.plugin._action_apply(kit._settings())["status"] == "ok"
    assert not owned.streams.exists()


def scenario_stale_empty_future_low_score_keeps_manual_repair(env):
    channel, stream = kit._seed_source_channel(env)
    row = kit._row(401, kit._now() + timedelta(hours=1), channel.id)
    row["score"] = 1
    owned = kit._publish(env, [row])["ranked_matchups:CFB:401"]
    env.m.M3UAccount.objects.update(updated_at=kit._now())
    row.update(channel_id=None, channel_ids=[], stream_ids=[])
    kit._write_cache(env, [row], refreshed_at=kit._now() - timedelta(days=4))
    assert env.plugin._action_apply(kit._settings())["status"] == "ok"
    assert env.m.Channel.objects.filter(pk=owned.pk).exists()
    assert list(owned.streams.values_list("id", flat=True)) == [stream.id]


def scenario_targeted_rematch_preserves_other_rows_and_snapshot(env):
    channel, stream = kit._seed_source_channel(env, "Georgia vs Alabama")
    row = kit._row(401, kit._now() + timedelta(hours=1), channel.id)
    other = kit._row(402, kit._now() + timedelta(hours=2), None, home="Texas", away="Oklahoma")
    other["channel_ids"] = []
    owned = kit._publish(env, [row, other])["ranked_matchups:CFB:401"]
    before = kit._read_cache(env)
    result = env.plugin._action_rematch(kit._settings(rematch_channel_id=owned.id))
    assert result["status"] == "ok", result
    after = kit._read_cache(env)
    assert after["matched_at"] == before["matched_at"]
    assert after["games"][1] == before["games"][1]
    assert after["games"][0]["matched_at"] > after["matched_at"]


SCENARIOS = kit.collect_scenarios(globals())


@pytest.fixture(scope="module")
def harness_results(tmp_path_factory):
    return kit.run_harness(tmp_path_factory, __file__, SCENARIOS)


@pytest.mark.parametrize("name", SCENARIOS)
def test_rematch_orm(harness_results, name):
    assert harness_results.get(name, "scenario did not run") is None, harness_results.get(name)
