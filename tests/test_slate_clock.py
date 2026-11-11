"""Real packets() path with deterministic TS output instead of an ffmpeg child."""
import inspect
import json
import os
import sys
import time
import types

from dispatcharr_ranked_matchups import slate


def stamp(value, prefix=2):
    return bytes((prefix << 4 | ((value >> 29) & 14) | 1, (value >> 22) & 255,
                  ((value >> 14) & 254) | 1, (value >> 7) & 255, ((value << 1) & 254) | 1))


def packet(pts=0, cc=0, pid=256):
    p = bytearray([255] * 188)
    p[:6] = bytes((71, 64 | (pid >> 8), pid & 255, 48 | cc, 7, 16))
    p[6:12] = bytes((pts >> 25, (pts >> 17) & 255, (pts >> 9) & 255,
                    (pts >> 1) & 255, (pts & 1) << 7 | 126, 0))
    p[12:26] = b'\x00\x00\x01\xe0\x00\x00\x80\x80\x05' + stamp(pts)
    return bytes(p)


def read_pts(p):
    b = p[21:26]
    return (b[0] & 14) << 29 | b[1] << 22 | (b[2] & 254) << 14 | b[3] << 7 | b[4] >> 1


def run_packets(monkeypatch, prior):
    # Emit three complete synthetic video PES packets through the exact public
    # generator, including read(), framing, rewriting and process cleanup.
    raw = b''.join(packet(i * 18000, i) for i in range(3))
    info = {'streams': [{'codec_type':'video','codec_name':'h264','avg_frame_rate':'5/1'},
                        {'codec_type':'audio','codec_name':'aac','sample_rate':'48000'}]}
    monkeypatch.setattr(slate, '_run', lambda *a: (0, json.dumps(info).encode()))
    class Process:
        def __init__(self, args, output): os.write(output, raw)
        def close(self): pass
    monkeypatch.setattr(slate, '_Process', Process)
    monkeypatch.setattr(slate, '_channel_clock', lambda *a: prior, raising=False)
    options = {'channel_id':'synthetic'} if 'channel_id' in inspect.signature(slate.packets).parameters else {}
    output = b''.join(slate.packets('/unused', time.time() + 2, **options))
    return [output[i:i+188] for i in range(0,len(output),188)]


def test_provider_to_slate_never_rewinds_pts_dts_or_pcr(monkeypatch):
    old = 90000 * 100
    prior = {'at': time.time() - 2, 'clocks': {'pts:video':old,'decode:video':old,'pcr':old*300},
             'counters':{256:7}}
    packets = run_packets(monkeypatch, prior)
    first = next(p for p in packets if p[1] & 64)
    assert read_pts(first) >= old + 180000, 'provider→slate rewound the channel clock'
    # PTS-only PES supplies both presentation and decoding timestamps.
    from dispatcharr_ranked_matchups.slate_clock import fields
    clocks = {name:value for name,_,value in fields(first)}
    assert clocks['decode:video'] >= old + 180000
    assert clocks['pcr'] >= (old + 180000) * 300


def test_provider_to_slate_continuity_counters_continue_per_pid(monkeypatch):
    prior = {'at':time.time(),'clocks':{},'counters':{256:14}}
    packets = run_packets(monkeypatch, prior)
    assert [p[3] & 15 for p in packets if p[3] & 16] == [15,0,1], 'slate reset the PID continuity counter'


def test_cold_start_has_zero_clock_and_advances(monkeypatch):
    packets = run_packets(monkeypatch, {'at':time.time(),'clocks':{},'counters':{}})
    assert [read_pts(p) for p in packets if p[1] & 64] == [0,18000,36000]


def test_discontinuity_does_not_drop_payload():
    from dispatcharr_ranked_matchups.slate_clock import Clock
    original = bytes((71,64,0,16)) + bytes(range(184))
    output = Clock({'at':0,'clocks':{},'counters':{0:5}}).rewrite(original)
    assert len(output) == 376
    assert output[5] & 128
    assert output[188+4:] == original[4:]
    assert output[3] & 15 == 5 and output[188+3] & 15 == 6


def test_snapshot_and_clocks_wrap_without_going_backward():
    from dispatcharr_ranked_matchups.slate_clock import Clock, snapshot, WRAP, fields
    previous = packet(WRAP - 90000, 15)
    prior = snapshot([previous], 100)
    output = Clock(prior, now=lambda:102).rewrite(packet(0))
    values = {name:value for name,_,value in fields(output[:188])}
    assert values['pts:video'] == 90000
    assert values['pcr'] == 90000 * 300


def test_explicit_decode_clock_keeps_the_provider_composition_delay():
    from dispatcharr_ranked_matchups.slate_clock import Clock,fields
    p=bytearray(packet())
    p[19:21]=bytes((192,10))
    p[21:31]=stamp(0,3)+stamp(0,1)
    prior={'at':10,'clocks':{'pts:video':900000,'dts:video':891000,'pcr':885000*300},'counters':{}}
    output=Clock(prior,now=lambda:11).rewrite(bytes(p))
    values={name:value for name,_,value in fields(output)}
    assert values['pts:video']==990000 and values['dts:video']==981000


def test_channel_clock_reads_binary_redis_buffer(monkeypatch):
    raw = packet(900000, 9)
    class Buffer:
        def get(self, key): return b'3'
        def mget(self, keys): return [None, None, raw]
        def zscore(self, key, index): return 100.0
    client = types.SimpleNamespace(get_buffer=lambda: Buffer())
    keys = types.SimpleNamespace(buffer_index=lambda c:'index',
        buffer_chunk=lambda c,i:f'chunk:{i}',chunk_timestamps=lambda c:'times')
    monkeypatch.setitem(sys.modules,'core.utils',types.SimpleNamespace(RedisClient=client))
    monkeypatch.setitem(sys.modules,'apps.proxy.live_proxy.redis_keys',types.SimpleNamespace(RedisKeys=keys))
    actual = slate._channel_clock('synthetic')
    assert actual['at']==100.0 and actual['clocks']['pts:video']==900000
    assert actual['counters'][256]==9
