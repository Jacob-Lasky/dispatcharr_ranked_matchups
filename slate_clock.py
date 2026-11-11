"""Continue a slate's TS clocks/counters from the channel's last buffered input.

Only slate packets pass through here. Provider packets and channel profiles are
never rewritten. All clocks retain their MPEG-TS wraparound domains.
"""
from __future__ import annotations

import time

SIZE = 188
WRAP = 1 << 33


def timestamp(data):
    return ((data[0] & 14) << 29 | data[1] << 22 |
            (data[2] & 254) << 14 | data[3] << 7 | data[4] >> 1)


def put_timestamp(packet, pos, value):
    value %= WRAP
    packet[pos:pos + 5] = bytes(((packet[pos] & 240) | ((value >> 29) & 14) | 1,
        (value >> 22) & 255, ((value >> 14) & 254) | 1,
        (value >> 7) & 255, ((value << 1) & 254) | 1))


def fields(packet):
    """Timestamp locations in a complete TS packet, including its clock domain."""
    result = []
    afc = (packet[3] >> 4) & 3
    if afc & 2:
        length = packet[4]
        if length > 183:
            return result
        if length >= 7 and packet[5] & 16:
            base = int.from_bytes(packet[6:10], 'big') << 1 | packet[10] >> 7
            extension = (packet[10] & 1) << 8 | packet[11]
            result.append(('pcr', 6, base * 300 + extension))
    else:
        length = -1
    pos = 5 + length if afc & 2 else 4
    if not afc & 1 or not packet[1] & 64 or pos + 14 > SIZE:
        return result
    if packet[pos:pos + 3] != b'\x00\x00\x01':
        return result
    stream_id = packet[pos + 3]
    kind = 'video' if 224 <= stream_id <= 239 else 'audio' if 192 <= stream_id <= 223 or stream_id == 189 else None
    if kind is None or packet[pos + 6] & 192 != 128:
        return result
    flags = packet[pos + 7] >> 6
    if flags in (2, 3):
        result.append(('pts:' + kind, pos + 9, timestamp(packet[pos + 9:pos + 14])))
    if flags == 3 and pos + 19 <= SIZE:
        result.append(('dts:' + kind, pos + 14, timestamp(packet[pos + 14:pos + 19])))
    elif flags == 2:
        # PTS-only PES also supplies the decode clock (no B-frame reordering).
        result.append(('decode:' + kind, pos + 9, timestamp(packet[pos + 9:pos + 14])))
    return result


def snapshot(chunks, received_at):
    clocks, counters = {}, {}
    for chunk in chunks:
        for pos in range(0, len(chunk) - SIZE + 1, SIZE):
            packet = chunk[pos:pos + SIZE]
            if packet[0] != 71:
                continue
            pid = (packet[1] & 31) << 8 | packet[2]
            counters[pid] = packet[3] & 15
            for name, _, value in fields(packet):
                clocks[name] = value
    return {'clocks': clocks, 'counters': counters, 'at': received_at}


class Clock:
    def __init__(self, prior, now=time.time):
        self.prior = prior
        self.now = now
        self.counters = dict(prior['counters'])
        self.offsets = {}
        self.elapsed = None
        self.seen = set()

    def rewrite(self, data):
        """Rewrite complete packets; preserve payload even when an AF is absent."""
        output = bytearray()
        for pos in range(0, len(data), SIZE):
            packet = bytearray(data[pos:pos + SIZE])
            if len(packet) != SIZE or packet[0] != 71:
                raise ValueError('Slate clock requires packet-aligned MPEG-TS')
            pid = (packet[1] & 31) << 8 | packet[2]
            afc = (packet[3] >> 4) & 3
            payload = bool(afc & 1)
            last = self.counters.get(pid, ((packet[3] & 15) - int(payload)) & 15)
            if pid not in self.seen and pid != 8191:
                self.seen.add(pid)
                if afc & 2 and packet[4] >= 1:
                    packet[5] |= 128
                else:
                    # Never steal bytes from PES/PSI payload to add flags.
                    marker = bytearray([255] * SIZE)
                    marker[:6] = bytes((71, packet[1] & 31, packet[2], 32 | last, 183, 128))
                    output.extend(marker)
            cc = (last + int(payload)) & 15
            packet[3] = packet[3] & 240 | cc
            self.counters[pid] = cc
            for name, offset, value in fields(packet):
                if name.startswith('decode:'):
                    continue
                if self.elapsed is None:
                    self.elapsed = max(0, self.now() - self.prior['at'])
                if name not in self.offsets:
                    old = self.prior['clocks'].get(name)
                    if old is None and name.startswith('dts:'):
                        old = self.prior['clocks'].get(name.replace('dts:', 'decode:'))
                    rate = 27000000 if name == 'pcr' else 90000
                    self.offsets[name] = round(old + self.elapsed * rate - value) if old is not None else 0
                value += self.offsets[name]
                if name == 'pcr':
                    base, extension = divmod(value % (WRAP * 300), 300)
                    packet[offset:offset + 6] = bytes((base >> 25, (base >> 17) & 255,
                        (base >> 9) & 255, (base >> 1) & 255,
                        ((base & 1) << 7) | 126 | (extension >> 8), extension & 255))
                else:
                    put_timestamp(packet, offset, value)
            output.extend(packet)
        return bytes(output)
