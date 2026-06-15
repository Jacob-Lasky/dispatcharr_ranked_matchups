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
    from apps.channels.models import Channel
    from apps.epg.models import ProgramData

    chans = [
        {"id": c.id, "name": c.name, "tvg_id": getattr(c, "tvg_id", None),
         "epg_data_id": c.epg_data_id}
        for c in Channel.objects.all().only("id", "name", "tvg_id", "epg_data_id")
    ]
    progs = [
        {"id": p.id, "title": p.title,
         "start_time": p.start_time.isoformat() if p.start_time else None,
         "end_time": p.end_time.isoformat() if p.end_time else None,
         "epg_id": p.epg_id}
        for p in ProgramData.objects.all().only("id", "title", "start_time", "end_time", "epg_id")
    ]
    json.dump({"channels": chans, "programs": progs}, open(out, "w"))
    print(f"exported {len(chans)} channels, {len(progs)} programs -> {out}")


if __name__ == "__main__":
    main()
