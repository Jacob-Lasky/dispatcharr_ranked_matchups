#!/usr/bin/env python3
"""Export a read-only snapshot of a Dispatcharr instance's channels + EPG.

Runs INSIDE the Dispatcharr container (it needs Django). Writes the JSON the
offline replay harness (tools/replay_match.py) consumes, so the matcher/lookup
can be exercised against real data with NO plugin runtime, NO discovery, NO
reload, i.e. without wedging the live worker.

Read-only: SELECTs only, no writes. Safe to run on a production container.

  docker cp tools/export_snapshot.py Dispatcharr:/tmp/export_snapshot.py
  docker exec Dispatcharr python /tmp/export_snapshot.py /tmp/dg_snapshot.json
  docker cp Dispatcharr:/tmp/dg_snapshot.json ./dg_snapshot.json

Then replay offline on the host:
  python tools/replay_match.py dg_snapshot.json --game "UFC Freedom 250: ..." --prefix UFC
"""
import json
import os
import sys


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/dg_snapshot.json"
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dispatcharr.settings")
    django.setup()
    from apps.channels.models import Channel, ChannelGroup, ChannelStream, Stream
    from apps.epg.models import ProgramData
    from django.db.models import Q

    # Must match plugin.py's TVG_ID_PREFIX. Imported rather than duplicated
    # would mean importing plugin.py, which starts the scheduler thread; this
    # script runs INSIDE the container against the live DB and must stay inert.
    TVG_ID_PREFIX = "ranked_matchups:"
    # Must ALSO match plugin.py's _OWNED_TVG_ID_LEGACY_MARKERS. Excluding only
    # the current prefix made the snapshot disagree with live provenance: a
    # stream attached solely to a legacy "dummy_top_matchups" channel is
    # uncurated in production and would replay as curated.
    OWNED_LEGACY_MARKERS = ("dummy_top_matchups",)

    chans = [
        {"id": c.id, "name": c.name, "tvg_id": getattr(c, "tvg_id", None),
         "epg_data_id": c.epg_data_id}
        for c in Channel.objects.all().only("id", "name", "tvg_id", "epg_data_id")
    ]
    progs = [
        {"id": p.id, "title": p.title,
         # sub_title/description feed the #143 European-broadcaster path; a
         # snapshot without them replays as if every programme were untitled
         # below the headline, which is the bug that path exists to fix.
         "sub_title": p.sub_title, "description": p.description,
         "start_time": p.start_time.isoformat() if p.start_time else None,
         "end_time": p.end_time.isoformat() if p.end_time else None,
         "epg_id": p.epg_id}
        for p in ProgramData.objects.all().only("id", "title", "sub_title", "description", "start_time", "end_time", "epg_id")
    ]
    # Streams power Path C (stream-name matching) and, since #206, the
    # per-group policy and the provenance sort key. channel_group_id is needed
    # for the group policy and `curated` for provenance; WITHOUT them a replay
    # silently exercises the pre-#206 path and reports no difference.
    streams = [
        {"id": s.id, "name": s.name, "channel_group_id": s.channel_group_id}
        for s in Stream.objects.all().only("id", "name", "channel_group_id")
    ]
    # Group names, so a replay can resolve the policy the same way the apply
    # does (_channel_group_names casefolds; the raw name is exported so the
    # harness stays free to change that).
    groups = dict(ChannelGroup.objects.values_list("id", "name"))
    # Provenance (#206): stream ids the USER attached to a channel of their own.
    # Excludes our OWN virtual channels for the reason _curated_stream_ids
    # documents: counting them makes provenance self-reinforcing.
    curated = sorted(
        ChannelStream.objects
        .exclude(
            Q(channel__tvg_id__startswith=TVG_ID_PREFIX)
            | Q(channel__tvg_id__in=OWNED_LEGACY_MARKERS)
        )
        .values_list("stream_id", flat=True)
        .distinct()
    )
    json.dump({
        "channels": chans, "programs": progs, "streams": streams,
        "channel_groups": groups, "curated_stream_ids": curated,
    }, open(out, "w"))
    print(f"exported {len(chans)} channels, {len(progs)} programs, "
          f"{len(streams)} streams, {len(groups)} groups, "
          f"{len(curated)} curated stream ids -> {out}")


if __name__ == "__main__":
    main()
