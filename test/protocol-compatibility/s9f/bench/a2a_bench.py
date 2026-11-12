#!/usr/bin/env python3
"""S9-F capacity: A2A gateway throughput + latency, measured with the official
A2A Python SDK client (a2a-sdk==1.3.0, protobuf) against the real broker
listener.

Each sample is one `SendMessage` round trip that the broker auto-completes:
HTTP POST -> a2a JSON-RPC -> task create -> SUBMITTED -> WORKING -> COMPLETED
-> event log append -> reply. That is the whole production data path.
"""
import asyncio
import json
import statistics
import sys
import time

import httpx

import a2a.types as t
from a2a.client import ClientFactory, ClientConfig

BASE = "http://127.0.0.1:18094/"
TOKEN = "a2a-evidence-token"


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return 0.0
    k = max(0, min(len(xs) - 1, int(round((p / 100.0) * (len(xs) - 1)))))
    return xs[k]


def request(text):
    return t.SendMessageRequest(message=t.Message(
        message_id="bench-" + str(time.perf_counter_ns()),
        role=t.Role.ROLE_USER,
        parts=[t.Part(text=text)]))


async def one(client):
    t0 = time.perf_counter()
    async for _ in client.send_message(request("bench")):
        pass
    return (time.perf_counter() - t0) * 1000.0


async def main():
    seq_n = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    conc = int(sys.argv[2]) if len(sys.argv) > 2 else 16

    httpx_client = httpx.AsyncClient(
        headers={"Authorization": f"Bearer {TOKEN}"}, timeout=30.0)
    factory = ClientFactory(ClientConfig(
        streaming=False, polling=False, httpx_client=httpx_client,
        supported_protocol_bindings=["JSONRPC"], accepted_output_modes=["text"]))
    client = await factory.create_from_url(BASE)

    for _ in range(5):
        await one(client)

    lat = []
    t0 = time.perf_counter()
    for _ in range(seq_n):
        lat.append(await one(client))
    wall = time.perf_counter() - t0

    per = max(1, seq_n // conc)

    async def worker():
        out = []
        for _ in range(per):
            out.append(await one(client))
        return out

    t1 = time.perf_counter()
    parts = await asyncio.gather(*[worker() for _ in range(conc)])
    wall_c = time.perf_counter() - t1
    lat_c = [x for ps in parts for x in ps]

    result = {
        "protocol": "A2A",
        "sdk": "a2a-sdk 1.3.0 (protobuf)",
        "operation": "SendMessage with task_auto_complete (full gateway round trip)",
        "sequential": {
            "samples": len(lat),
            "wall_ms": round(wall * 1000, 1),
            "throughput_per_s": round(len(lat) / wall, 1),
            "p50_ms": round(pct(lat, 50), 3),
            "p95_ms": round(pct(lat, 95), 3),
            "p99_ms": round(pct(lat, 99), 3),
            "mean_ms": round(statistics.fmean(lat), 3),
        },
        "concurrent": {
            "concurrency": conc,
            "samples": len(lat_c),
            "wall_ms": round(wall_c * 1000, 1),
            "throughput_per_s": round(len(lat_c) / wall_c, 1),
            "p50_ms": round(pct(lat_c, 50), 3),
            "p95_ms": round(pct(lat_c, 95), 3),
            "p99_ms": round(pct(lat_c, 99), 3),
            "mean_ms": round(statistics.fmean(lat_c), 3),
        },
    }
    print(json.dumps(result, indent=2))
    await httpx_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
