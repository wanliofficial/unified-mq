#!/usr/bin/env python3
"""S10-D D.6 capacity bench: WebRTC signaling relay throughput + latency.

Drives the *production* signaling front end (``broker_proto_webrtc``) over the
exact wire path a browser uses (``wss://.../websocket/<room>`` -> authenticate
-> opaque ``to`` relay), and measures the two things a capacity report needs:

  * **throughput** -- how many signals/second the relay sustains, at a given
    connection count x room count x per-pair signaling rate, in both
    ``sequential`` (one pair active at a time) and ``concurrent`` (every pair
    active at once) modes;
  * **latency** -- one-way ``A -> B`` relay latency, from a monotonic timestamp
    the sender stamps into ``data["t"]``. Loopback on one host shares the clock,
    so ``recv_ns - send_ns`` is a honest one-way figure.

The relay is the whole ``webrtc_conn -> webrtc_dispatch -> peer`` path; nothing
here shortcuts it. Numbers are what they are on the box they ran on (see the
report's machine line) and are not extrapolated (R-E5).
"""
import argparse
import asyncio
import json
import statistics
import sys
import time
from pathlib import Path

# Reuse the exact D.1 signaling client: the same handshake, the same relay.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from webrtc_signaling import SignalingClient, _decode_event  # noqa: E402

USER = "alice"
PASSWORD = "alicepw"


def _now_ns():
    return time.monotonic_ns()


class Pair:
    """One room with exactly two peers (A, B). A relays to B at a target rate."""

    def __init__(self, host, port, room, rate, total):
        self.host = host
        self.port = port
        self.room = room
        self.rate = rate            # signals/sec; 0 == send as fast as possible
        self.total = total          # signals this pair exchanges this run
        self.a = None
        self.b = None
        self.latencies_us = []
        self.sent = 0
        self.recv = 0
        self.errors = 0
        self._t0 = None
        self._t1 = None

    async def open(self):
        # Both peers must live in the SAME room: the relay only delivers a signal
        # to peers in the sender's room (a cross-room `to' would answer with
        # `unknown_peer', R-X4). Splitting A and B into `{room}-a'/`{room}-b'
        # made B's `recv_signal' block forever -- the relay never reached it.
        self.a = await SignalingClient.connect(
            self.host, self.port, self.room, USER, PASSWORD)
        self.b = await SignalingClient.connect(
            self.host, self.port, self.room, USER, PASSWORD)

    async def close(self):
        await self.a.close()
        await self.b.close()

    async def run_concurrent(self):
        """A and B active together: B drains, A paces/sends. Both ends of the relay."""
        self._t0 = _now_ns()
        drain_task = asyncio.ensure_future(self._drain_b())
        await self._drive_a()
        await drain_task
        self._t1 = _now_ns()

    async def run_sequential(self):
        """One full round-trip pair at a time inside this pair: A sends, B drains."""
        self._t0 = _now_ns()
        for _ in range(self.total):
            t_send = _now_ns()
            await self.a.send_signal(self.b.peer_id, "ping", {"t": t_send})
            raw = await self.b.recv_signal()
            self.sent += 1
            self.recv += 1
            t_recv = _now_ns()
            self.latencies_us.append((t_recv - t_send) / 1000.0)
            if self.rate > 0:
                await asyncio.sleep(1.0 / self.rate)
        self._t1 = _now_ns()

    async def _drive_a(self):
        if self.rate > 0:
            interval = 1.0 / self.rate
            for _ in range(self.total):
                t_send = _now_ns()
                try:
                    await self.a.send_signal(self.b.peer_id, "ping", {"t": t_send})
                    self.sent += 1
                except Exception:
                    self.errors += 1
                await asyncio.sleep(interval)
        else:
            # Max throughput: fire without pacing, bounded only by the event loop.
            for _ in range(self.total):
                t_send = _now_ns()
                try:
                    await self.a.send_signal(self.b.peer_id, "ping", {"t": t_send})
                    self.sent += 1
                except Exception:
                    self.errors += 1

    async def _drain_b(self):
        while self.recv < self.total:
            try:
                raw = await asyncio.wait_for(self.b.recv_signal(), timeout=5.0)
            except asyncio.TimeoutError:
                break
            t_recv = _now_ns()
            try:
                msg = _decode_event(raw)
                t_send = msg.data["t"]
                self.latencies_us.append((t_recv - t_send) / 1000.0)
                self.recv += 1
            except Exception:
                self.errors += 1


def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


async def run_one(host, port, rooms, peers_per_room, rate, total, mode):
    pairs = []
    for r in range(rooms):
        p = Pair(host, port, f"bench-room-{r}", rate, total)
        await p.open()
        pairs.append(p)

    wall0 = time.monotonic()
    if mode == "concurrent":
        await asyncio.gather(*(p.run_concurrent() for p in pairs))
    else:
        for p in pairs:
            await p.run_sequential()
    wall1 = time.monotonic()

    for p in pairs:
        await p.close()

    all_lat = [x for p in pairs for x in p.latencies_us]
    total_sent = sum(p.sent for p in pairs)
    total_recv = sum(p.recv for p in pairs)
    total_err = sum(p.errors for p in pairs)
    wall_s = wall1 - wall0
    result = {
        "rooms": rooms,
        "peers_per_room": peers_per_room,
        "connections": rooms * peers_per_room,
        "rate_per_pair": rate,
        "total_per_pair": total,
        "mode": mode,
        "wall_seconds": round(wall_s, 4),
        "signals_sent": total_sent,
        "signals_relayed": total_recv,
        "errors": total_err,
        "aggregate_throughput_per_s": round(total_recv / wall_s, 1) if wall_s > 0 else None,
        "per_pair_throughput_per_s": round((total_recv / rooms) / wall_s, 1)
            if wall_s > 0 and rooms > 0 else None,
        "latency_us": {
            "count": len(all_lat),
            "mean": round(statistics.mean(all_lat), 1) if all_lat else None,
            "p50": round(pct(all_lat, 50), 1) if all_lat else None,
            "p95": round(pct(all_lat, 95), 1) if all_lat else None,
            "p99": round(pct(all_lat, 99), 1) if all_lat else None,
            "max": round(max(all_lat), 1) if all_lat else None,
        },
    }
    return result


def main():
    ap = argparse.ArgumentParser(description="S10-D WebRTC signaling relay bench")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--rooms", type=int, required=True)
    ap.add_argument("--peers-per-room", type=int, default=2)
    ap.add_argument("--rate", type=int, default=0,
                    help="signals/sec per pair; 0 = max throughput")
    ap.add_argument("--total", type=int, default=200,
                    help="signals per pair for this run")
    ap.add_argument("--mode", choices=["sequential", "concurrent"], required=True)
    ap.add_argument("--out", default=None, help="write JSON result here")
    args = ap.parse_args()

    result = asyncio.run(run_one(
        args.host, args.port, args.rooms, args.peers_per_room,
        args.rate, args.total, args.mode))
    blob = json.dumps(result, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(blob + "\n")
        print(f"wrote {args.out}")
    print(blob)


if __name__ == "__main__":
    main()
