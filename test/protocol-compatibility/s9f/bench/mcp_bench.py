#!/usr/bin/env python3
"""S9-F capacity: MCP gateway throughput + latency, measured with the official
MCP SDK client (mcp==2.3.0) against the real broker listener.

Each sample is one `tools/call` round trip: HTTP POST -> mcp_conn -> mcp_dispatch
-> publish to `$mcp/tools/call` -> evidence backend -> reply `$mcp/reply/...` ->
back over the same HTTP response. That is the whole production data path.
"""
import asyncio
import json
import statistics
import sys
import time

import httpx2
from contextlib import asynccontextmanager

from mcp import Client
from mcp.client.streamable_http import streamable_http_client

BASE = "http://127.0.0.1:18096/"
TOKEN = "mcp-evidence-token"


@asynccontextmanager
async def transport(url):
    http_client = httpx2.AsyncClient(
        headers={"Authorization": f"Bearer {TOKEN}"}, timeout=30.0)
    async with streamable_http_client(
            url, http_client=http_client, terminate_on_close=False) as st:
        yield st


async def one(client):
    t0 = time.perf_counter()
    await client.call_tool("emit-list-change", {"topic": "bench"})
    return (time.perf_counter() - t0) * 1000.0


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return 0.0
    k = max(0, min(len(xs) - 1, int(round((p / 100.0) * (len(xs) - 1)))))
    return xs[k]


async def main():
    async with Client(transport(BASE), mode="2026-07-28") as client:
        # warmup
        for _ in range(5):
            await one(client)

        seq_n = int(sys.argv[1]) if len(sys.argv) > 1 else 200
        lat = []
        t0 = time.perf_counter()
        for _ in range(seq_n):
            lat.append(await one(client))
        wall = time.perf_counter() - t0

        conc = int(sys.argv[2]) if len(sys.argv) > 2 else 16
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
            "protocol": "MCP",
            "sdk": "mcp 2.3.0",
            "operation": "tools/call (full gateway round trip)",
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


if __name__ == "__main__":
    asyncio.run(main())
