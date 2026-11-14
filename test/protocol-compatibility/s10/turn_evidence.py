#!/usr/bin/env python3
"""S10-D D.1 evidence: a real aiortc peer drives our TURN server through the
full Allocate / long-term-credential / relay-address path.

This is the third-party-client interop proof for the TURN half of our ICE
server. It uses an independent, real WebRTC stack (aiortc 1.13.0 / aioice
0.13.0) as the client against the production ``broker_ice`` TURN server, so
"a real client can allocate a relay from us with the right credential and is
refused with the wrong one" is a measured fact, not an assertion.

What it proves
--------------
For each credential case we force aioice into relay-only mode
(``transport_policy = RELAY``, RFC 9429 sec. 3.5.3 -- the equivalent of a
browser's ``iceTransportPolicy:"relay"``; aiortc's public API has no such knob
so ``webrtc_signaling.force_relay_policy`` reaches the private ``aioice``
connection, and we say so here rather than hide it). Relay-only means aioice
gathers *only* relay candidates, so:

  * with the correct ``alice`` long-term credential the peer must Allocate
    successfully, and its SDP carries a ``typ relay`` candidate whose address is
    our node's advertised ICE IP and whose port falls inside the node's
    configured relay range (49500-49519);
  * with a wrong credential the Allocate is refused, so the peer gathers **zero**
    relay candidates -- our credential enforcement holds against a real client.

Honest boundary: the media-relay plane
---------------------------------------
Between two aioice peers this does **not** complete a relay-to-relay media path,
and we record that as a finding rather than hiding it. aioice's TURN client
sends data exclusively via ChannelBind and never issues a CreatePermission
(``aioice.turn.TurnTransport.send_data`` calls ``channel_bind`` directly). Our
server implements RFC 5766 sec. 9 faithfully: a ChannelBind without a prior
CreatePermission is refused with 403, which is exactly what the frozen CT case
``channel_bind_without_a_permission_is_a_403`` in ``turn_allocation_SUITE``
asserts. Real browsers send CreatePermission and therefore interoperate; aioice
omits it and therefore cannot complete relay-to-relay media against a strict
server. That is a limitation of the aioice client, not a defect in ``broker_ice``
(the full permission -> channel -> ChannelData -> peer path is proven CT-side
with a spec-compliant client in ``ice_test_client``). We deliberately do **not**
relax the server's 403 to make aioice pass, because that would break the frozen
CT contract and weaken the security property the 403 exists to protect.

This script proves the Allocate/credential/relay-address surface with a real
client; it does not claim relay-to-relay media throughput.
"""
import argparse
import asyncio
import json
import sys

from aiortc import RTCPeerConnection, RTCConfiguration, RTCIceServer

import webrtc_signaling as sig
import webrtc_evidence as we

# The node's configured relay pool (s10_node: relay_port_range => {49500,49519}).
RELAY_RANGE = (49500, 49519)
TURN_USERNAME = "alice"
TURN_CREDENTIAL = "alicepw"


async def _gather_relay(turn_port, ice_host, username, password):
    """One relay-only peer; return the relay candidates it gathered.

    Returns ``(relay_candidates, sdp)``. With relay-only mode forced, aioice
    gathers only relay candidates, so a non-empty list means the Allocate and
    auth succeeded against our server."""
    server = RTCIceServer(
        urls=[f"turn:{ice_host}:{turn_port}?transport=udp"],
        username=username, credential=password)
    pc = RTCPeerConnection(
        configuration=RTCConfiguration(iceServers=[server]))
    pc.createDataChannel("s10-turn-allocate")
    patched = sig.force_relay_policy(pc)
    await pc.setLocalDescription(await pc.createOffer())
    sdp = pc.localDescription.sdp
    candidates = sig.parse_candidates(sdp)
    await pc.close()
    return candidates.get("relay", []), sdp, patched


def _port_in_range(relay_candidates, ice_host, lo, hi):
    """Do the relay candidates carry our advertised address on a port inside the
    configured relay range? We parse the SDP directly so the port is exact."""
    ports = []
    for line in _relay_lines_from_sdp(_last_sdp[0]):
        tokens = line.split()
        # a=candidate:<foundation> <comp> <transport> <priority> <ip> <port> ...
        if len(tokens) >= 6 and tokens[4] == ice_host:
            try:
                ports.append(int(tokens[5]))
            except ValueError:
                pass
    in_range = [p for p in ports if lo <= p <= hi]
    return ports, in_range


_last_sdp = [None]


def _relay_lines_from_sdp(sdp):
    return [ln.strip() for ln in sdp.splitlines()
            if "typ relay" in ln and "candidate" in ln]


async def run(turn_port, ice_host, out_lines):
    lo, hi = RELAY_RANGE

    # --- correct credential: must allocate a relay in range -----------------
    good_relay, good_sdp, patched = await _gather_relay(
        turn_port, ice_host, TURN_USERNAME, TURN_CREDENTIAL)
    _last_sdp[0] = good_sdp
    ports, in_range = _port_in_range(good_relay, ice_host, lo, hi)
    good_ok = len(good_relay) > 0 and len(in_range) > 0
    log(out_lines, "turn_allocate_ok", {
        "credential": TURN_USERNAME,
        "relay_policy_patched": patched,
        "relay_candidates": len(good_relay),
        "relay_ports": ports,
        "relay_lines": _relay_lines_from_sdp(good_sdp),
        "range": [lo, hi],
        "ok": good_ok,
    })

    # --- wrong credential: must gather zero relays --------------------------
    bad_relay, _, _ = await _gather_relay(
        turn_port, ice_host, TURN_USERNAME, "definitely-wrong-password")
    bad_ok = len(bad_relay) == 0
    log(out_lines, "turn_wrong_credential_refused", {
        "credential": TURN_USERNAME,
        "relay_candidates": len(bad_relay),
        "ok": bad_ok,
    })

    # --- honest record of the media-plane boundary --------------------------
    log(out_lines, "media_plane_boundary", {
        "finding": "aioice relays data only via ChannelBind and never issues "
                   "CreatePermission; broker_ice correctly refuses a "
                   "ChannelBind without a permission with 403 (RFC 5766 9.2, "
                   "pinned by turn_allocation_SUITE:channel_bind_"
                   "without_a_permission_is_a_403). Relay-to-relay media is "
                   "therefore not completed with aioice; the permission -> "
                   "channel -> ChannelData -> peer path is proven CT-side with "
                   "a spec-compliant client (ice_test_client).",
        "server_not_modified": True,
    })

    return good_ok and bad_ok


def log(lines, event, payload):
    lines.append({"event": event, **payload})
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ice-host", default=None)
    parser.add_argument("--turn-port", type=int, required=True)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    ice_host = args.ice_host or we._lan_ipv4()
    print(f"S10-TURN-CONFIG ice_host={ice_host} turn_port={args.turn_port} "
          f"range={list(RELAY_RANGE)}")

    lines = []
    try:
        ok = asyncio.run(run(args.turn_port, ice_host, lines))
    except Exception as exc:  # noqa: BLE001
        log(lines, "error", {"type": type(exc).__name__, "message": str(exc)})
        ok = False

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            for item in lines:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
    log(lines, "verdict", {"ok": bool(ok)})
    if args.out:
        with open(args.out, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"event": "verdict", "ok": bool(ok)},
                                ensure_ascii=False) + "\n")
    print("S10-TURN-VERDICT " + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
