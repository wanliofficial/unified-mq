#!/usr/bin/env python3
"""S10-0 spike client: drive the ws signaling listener and record what the wire
actually does.

Measurements this produces (all quoted in REPORT.md):
  1. handshake response byte count + the exact `Sec-WebSocket-Protocol` echo
  2. a text `ping` -> text `pong` (the upstream library's convention)
  3. a native opcode 9 ping -> native opcode 10 pong (RFC 6455 baseline)
  4. a binary frame -> server close 1003
  5. a `to` message from peer A arriving at peer B (process direct delivery)

Nothing here is a product client; it exists to answer the Task 0.3 questions.
"""
import asyncio
import json
import sys

import websockets


async def scenario(port, out):
    uri = f"ws://127.0.0.1:{port}/websocket/room1"
    log = []

    async with websockets.connect(uri, subprotocols=["webrtc.v1"]) as a:
        log.append(("handshake_status", a.response.status_code))
        log.append(("handshake_subprotocol", a.subprotocol))
        raw_headers = [(k, v) for k, v in a.response.headers.items()]
        log.append(("handshake_headers", raw_headers))

        await a.send(json.dumps({"event": "authenticate",
                                 "data": {"username": "u", "password": "p"}}))
        a_auth = json.loads(await a.recv())
        log.append(("authenticated", a_auth))
        log.append(("a_peer_id_minted_by_server", a_auth["data"]["peer_id"]))

        # text ping / text pong (upstream semantics, distinct from opcode 9)
        await a.send("ping")
        log.append(("text_pong", await a.recv()))

        # native ping: websockets' own ping() uses opcode 9
        pong_waiter = await a.ping(b"spike")
        await asyncio.wait_for(pong_waiter, timeout=5)
        log.append(("native_pong_opcode9", "ok"))

        # second peer joins, then A sends `to` B
        async with websockets.connect(uri, subprotocols=["webrtc.v1"]) as b:
            await b.send(json.dumps({"event": "authenticate", "data": {}}))
            b_auth = json.loads(await b.recv())
            log.append(("b_authenticated", b_auth))
            b_peer = b_auth["data"]["peer_id"]
            # R-S7: the client cannot choose its own id, so it must learn B's
            # from the server event before it can address it.
            await a.send(json.dumps({"to": b_peer, "event": "offer",
                                     "data": {"sdp": "v=0 fake"}}))
            log.append(("delivered_to_b", json.loads(await asyncio.wait_for(b.recv(), 5))))

        # binary frame must be refused with 1003, not silently ignored
        await a.send(b"\x00\x01\x02")
        try:
            await asyncio.wait_for(a.recv(), timeout=5)
        except websockets.exceptions.ConnectionClosed as exc:
            log.append(("binary_close_code", exc.rcvd.code if exc.rcvd else None))

    with open(out, "w", encoding="utf-8") as fh:
        for item in log:
            fh.write(repr(item) + "\n")
    for item in log:
        print(item)


def main():
    if len(sys.argv) != 3:
        print("usage: spike_client.py <port> <outfile>", file=sys.stderr)
        return 2
    asyncio.run(scenario(int(sys.argv[1]), sys.argv[2]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
