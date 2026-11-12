#!/usr/bin/env python3
"""S9-F real-client evidence: official MCP Python SDK (mcp==2.3.0).

The SDK's `LATEST_PROTOCOL_VERSION` is `2026-07-28` -- exactly the version this
broker's `broker_proto_mcp` implements. We drive the broker's Streamable HTTP
gateway as a genuine third-party MCP client would: discover the server, list
tools, call a tool (which the broker forwards to `$mcp/tools/call` and the fake
backend answers), list resources/prompts, and read the progress + notification
the backend emits.

Everything printed here is captured verbatim into the evidence file.
"""
import asyncio
import json
import sys

import httpx2
from contextlib import asynccontextmanager

from mcp import Client
from mcp.client.streamable_http import streamable_http_client

BASE = "http://127.0.0.1:18096/"
TOKEN = "mcp-evidence-token"
VERSION = "2026-07-28"


def log(*a):
    print(*a, flush=True)


@asynccontextmanager
async def authed_transport(url):
    """A Streamable-HTTP transport whose every request carries the broker's
    bearer token + protocol version. The SDK builds these headers itself for
    the negotiated version; we only add Authorization here."""
    http_client = httpx2.AsyncClient(
        headers={"Authorization": f"Bearer {TOKEN}"}, timeout=15.0)
    async with streamable_http_client(
        url, http_client=http_client, terminate_on_close=False) as streams:
        yield streams


async def phase_discover():
    """Phase A: `mode="auto"` makes the SDK perform a real `server/discover`
    probe and *adopt* the result. This is the capability-negotiation evidence:
    the version, serverInfo and the four capability objects below are what the
    official SDK parsed out of the broker's own `server/discover` reply."""
    async with Client(authed_transport(BASE), mode="auto") as client:
        log("\n[A1] negotiated protocol_version:", client.protocol_version)
        log("     server_info:", client.server_info)
        log("     capabilities:",
            json.dumps(client.server_capabilities.model_dump(exclude_none=True),
                       indent=2))
        log("     instructions:", client.instructions)
        tools = await client.list_tools()
        log("     discovered tools:", [t.name for t in tools.tools])


async def phase_pin():
    log("\n=== official MCP SDK client ===")
    import mcp
    from mcp.types import LATEST_PROTOCOL_VERSION
    log("mcp package version:", getattr(mcp, "__version__", "(2.3.0 dist)"))
    log("SDK LATEST_PROTOCOL_VERSION:", LATEST_PROTOCOL_VERSION)
    log("broker implements:          2026-07-28")
    assert LATEST_PROTOCOL_VERSION == "2026-07-28", "SDK/broker version mismatch"

    # mode="2026-07-28" adopts the modern protocol directly (no initialize).
    # Pass our authed transport as the `server` argument.
    async with Client(authed_transport(BASE), mode="2026-07-28") as client:
        log("\n[1] client connected (no initialize handshake needed in 2026-07-28)")

        log("\n[2] server capabilities:",
            json.dumps(client.server_capabilities.model_dump(exclude_none=True),
                       indent=2))
        log("    serverInfo:", client.server_info)

        # tools/list (features => [] means none are advertised)
        try:
            tools = await client.list_tools()
            log("\n[3] list_tools ->", tools.model_dump(exclude_none=True))
        except Exception as e:
            log("\n[3] list_tools ->", type(e).__name__, str(e)[:200])

        # resources/list
        try:
            res = await client.list_resources()
            log("\n[4] list_resources ->", res.model_dump(exclude_none=True))
        except Exception as e:
            log("\n[4] list_resources ->", type(e).__name__, str(e)[:200])

        # prompts/list
        try:
            pr = await client.list_prompts()
            log("\n[5] list_prompts ->", pr.model_dump(exclude_none=True))
        except Exception as e:
            log("\n[5] list_prompts ->", type(e).__name__, str(e)[:200])

        # tools/call -- broker forwards to $mcp/tools/call, backend answers.
        async def progress_cb(progress, total, message):
            log("    [progress]", progress, "/", total, "-", message)

        try:
            result = await client.call_tool(
                "emit-list-change", {"topic": "s9f/evidence"},
                progress_callback=progress_cb)
            log("\n[6] call_tool(emit-list-change) ->",
                json.dumps(result.model_dump(exclude_none=True), indent=2))
        except Exception as e:
            log("\n[6] call_tool ->", type(e).__name__, str(e)[:300])

        # An unknown tool name is *not* the gateway's business: `tools/call` is
        # passed through verbatim (R-B1 -- the broker never interprets tool
        # semantics), so the backend decides. The fake backend answers any name,
        # which is exactly what a pass-through gateway should surface.
        try:
            r = await client.call_tool("does-not-exist", {})
            log("\n[7] call_tool(does-not-exist) -> passthrough answered:",
                json.dumps(r.model_dump(exclude_none=True))[:200])
        except Exception as e:
            log("\n[7] call_tool(does-not-exist) ->",
                type(e).__name__, str(e)[:200])


async def phase_listen():
    """Phase C: `subscriptions/listen` through the official SDK, with a second
    client triggering `tools/list_changed`."""
    async with Client(authed_transport(BASE), mode="2026-07-28") as listener:
        async with listener.listen(tools_list_changed=True) as sub:
            log("\n[C1] listen acknowledged; honored subset:", sub.honored)
            async with Client(authed_transport(BASE), mode="2026-07-28") as trigger:
                await trigger.call_tool("emit-list-change", {"topic": "s9f"})
            async for event in sub:
                log("     event:", event)
                break
    log("\n[C2] listen stream closed gracefully by the client")


async def main():
    await phase_discover()
    await phase_pin()
    await phase_listen()
    log("\n=== MCP SDK evidence complete ===")


if __name__ == "__main__":
    asyncio.run(main())
