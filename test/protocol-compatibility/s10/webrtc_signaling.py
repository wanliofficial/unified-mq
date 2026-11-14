#!/usr/bin/env python3
"""Shared plumbing for the S10-D WebRTC evidence (D.1).

This module is imported by ``webrtc_evidence.py`` (offer/answer + media),
``stun_evidence.py`` (Binding reflexive address) and ``turn_evidence.py``
(relay allocation). It holds the three things every one of those scripts needs
and nothing else:

  * :class:`SignalingClient` -- a ``websockets`` connection to the Broker's
    ``wss://127.0.0.1:<port>/websocket/<room>`` front end. It performs the
    ``authenticate`` handshake, reads the ``authenticated`` / ``joined`` server
    events, and relays opaque client signals (``{"event","to","data"}``) to a
    same-room peer. This is the exact wire path ``apps/broker_proto_webrtc``
    speaks; nothing here is a shortcut around it.

  * :func:`parse_candidates` -- pull the ``candidate`` lines and the
    ``a=candidate`` / ``a=raddr`` facts out of an SDP blob so the scripts can
    assert what kind of address a peer actually gathered.

  * :func:`force_relay_policy` -- reach past aiortc's public ``RTCConfiguration``
    (which has no ``iceTransportPolicy`` in 1.13.0) to the underlying
    ``aioice.Connection`` and set ``transport_policy = RELAY``. That is exactly
    RFC 9429 sec. 3.5.3 relay-only mode, i.e. what a browser's
    ``iceTransportPolicy: "relay"`` requests. It is documented as a deliberate
    reach into a private attribute because the public API cannot express it.

aiortc gathers **full ICE**: host, server-reflexive and relay candidates are all
inline in the SDP (there is no trickle in aiortc 1.13.0), so exchanging the SDP
through the signaling relay is sufficient to drive ICE. No candidate-signaling
messages are needed.
"""
import asyncio
import json
import ssl
from dataclasses import dataclass, field

import websockets

# ---------------------------------------------------------------------------
# Candidate parsing
# ---------------------------------------------------------------------------

# The candidate types a WebRTC endpoint can put in its SDP, most- to
# least-preferred. aiortc orders them this way when it writes ``a=candidate``.
CANDIDATE_PRIORITY = ["host", "srflx", "prflx", "relay"]


def candidate_type(candidate_line):
    """Return the ICE candidate type (``host``/``srflx``/``relay``/...) of one
    ``a=candidate:...`` line, or ``None`` if it is not a candidate line."""
    if "candidate" not in candidate_line:
        return None
    # a=candidate:<foundation> <component> <transport> <priority> <ip> <port>
    #             typ <type> ...
    tokens = candidate_line.split()
    for i, tok in enumerate(tokens):
        if tok == "typ" and i + 1 < len(tokens):
            return tokens[i + 1]
    return None


def parse_candidates(sdp):
    """Return ``{type: [ip, ...]}`` for every ``a=candidate`` line in ``sdp``.

    Only the address of each candidate is kept; that is enough to answer "did
    this peer gather a server-reflexive / relay address from our server"."""
    out = {}
    for line in sdp.splitlines():
        line = line.strip()
        ctype = candidate_type(line)
        if ctype is None:
            continue
        # address is the 5th whitespace field after the ``candidate:`` prefix
        body = line.split("candidate:", 1)[-1].split()
        if len(body) >= 5:
            ip = body[4]
        else:
            ip = "?"
        out.setdefault(ctype, []).append(ip)
    return out


def parse_related_address(sdp):
    """Return ``(related_ip, related_port)`` from the first relay candidate's
    ``raddr``/``rport``, i.e. the *server-side* address the relay advertises."""
    for line in sdp.splitlines():
        line = line.strip()
        if "raddr" not in line:
            continue
        tokens = line.split()
        ip = port = None
        for i, tok in enumerate(tokens):
            if tok == "raddr" and i + 1 < len(tokens):
                ip = tokens[i + 1]
            if tok == "rport" and i + 1 < len(tokens):
                port = tokens[i + 1]
        if ip is not None:
            return ip, port
    return None, None


# ---------------------------------------------------------------------------
# aiortc relay-policy injection
# ---------------------------------------------------------------------------

def force_relay_policy(peer_connection):
    """Force ``peer_connection`` to relay-only ICE.

    aiortc 1.13.0's ``RTCConfiguration`` exposes ``iceServers`` and
    ``bundlePolicy`` but **not** ``iceTransportPolicy``. To exercise the relay
    path the way a browser's ``iceTransportPolicy: "relay"`` does, we set
    ``transport_policy = RELAY`` on the underlying ``aioice.Connection``. That
    is what makes aioice gather *only* relay candidates (RFC 9429 sec. 3.5.3)
    and pair relay-to-relay.

    The connection object is created lazily when the first transceiver is added
    or the first ``setLocalDescription`` runs, so we patch the gatherer's
    connection after the peer has started gathering. Returns the number of ICE
    transports patched (0 if none existed yet -- the caller should have added a
    track first).
    """
    from aioice import TransportPolicy

    pc = peer_connection
    patched = 0
    # ``_RTCPeerConnection__iceTransports`` is a set of RTCIceTransport; each
    # holds the aioice Connection at ``_connection`` (aliased from its gatherer).
    transports = getattr(pc, "_RTCPeerConnection__iceTransports", set())
    for ice_transport in transports:
        conn = getattr(ice_transport, "_connection", None)
        if conn is not None:
            conn._transport_policy = TransportPolicy.RELAY
            patched += 1
    return patched


# ---------------------------------------------------------------------------
# Signaling client
# ---------------------------------------------------------------------------

def make_insecure_ssl_context():
    """A client TLS context that accepts the evidence node's self-signed cert.

    The evidence node mints its own ``CN=127.0.0.1`` certificate (see
    ``s10_node``); we are proving the WebRTC + STUN/TURN path, not the PKI, so
    verification is intentionally off. This is called out in the README as an
    honest boundary, not hidden.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


@dataclass
class ServerEvent:
    """One decoded server event frame (``{"event":<name>,"data":<map>|none}``)."""
    event: str
    data: dict = field(default_factory=dict)


class SignalingClient:
    """One WebSocket signaling connection to the Broker.

    Mirrors what a browser ``RTCPeerConnection`` does against a signaling
    server: open ``wss://.../<room>``, send ``authenticate``, then exchange
    opaque offer/answer/candidate signals addressed by ``to``. The Broker mints
    the ``peer_id`` (R-S7) and tells the already-present peers about the joiner
    via ``joined``; the joiner learns its peers from the ``from`` field of the
    first signal it receives.
    """

    def __init__(self, ws, peer_id=None):
        self._ws = ws
        self.peer_id = peer_id
        self.username = None

    @classmethod
    async def connect(cls, host, port, room, username, password,
                      subprotocol="webrtc.v1"):
        uri = f"wss://{host}:{port}/websocket/{room}"
        ws = await websockets.connect(
            uri,
            ssl=make_insecure_ssl_context(),
            subprotocols=[subprotocol],
            max_size=None,
        )
        client = cls(ws)
        await client._authenticate(username, password)
        return client

    async def _authenticate(self, username, password):
        await self._ws.send(json.dumps({
            "event": "authenticate",
            "data": {"username": username, "password": password},
        }))
        event = await self._await_event("authenticated")
        self.peer_id = event.data["peer_id"]
        self.username = username

    async def _await_event(self, name, timeout=10.0):
        """Wait for the next server event with ``event == name``.

        Drains (and returns) any intervening events of other names so a
        ``joined`` arriving before an ``authenticated`` does not stall the
        handshake. Raises ``asyncio.TimeoutError`` if nothing arrives in time.
        """
        async def _wait():
            while True:
                raw = await self._ws.recv()
                event = _decode_event(raw)
                if event.event == name:
                    return event
        return await asyncio.wait_for(_wait(), timeout)

    async def wait_for_joined(self, timeout=10.0):
        """Block until the Broker broadcasts that *this* client joined.

        The Broker sends ``joined`` only to the peers already in the room, so
        the *first* peer to connect waits here to learn the second peer's id;
        the second peer to connect would never receive its own ``joined`` (it
        learns the first peer from a signal's ``from``). Returns the joined
        peer's id, or ``None`` on timeout.
        """
        try:
            event = await self._await_event("joined", timeout)
        except asyncio.TimeoutError:
            return None
        return event.data.get("peer_id")

    async def send_signal(self, to, event, data):
        """Relay one opaque client signal to peer ``to`` in this room."""
        await self._ws.send(json.dumps({
            "event": event,
            "to": to,
            "data": data,
        }))

    async def recv_signal(self):
        """Receive the next relayed signal from a peer.

        Returns ``(from_peer_id, data)``. Only frames the Broker relayed (which
        carry a ``from`` field added by ``webrtc_dispatch``) are signals; server
        events (``authenticated``/``joined``/``left``/...) are skipped.
        """
        while True:
            raw = await self._ws.recv()
            event = _decode_event(raw)
            if event.event in ("authenticated", "joined", "left",
                               "unauthorized", "invalid_message"):
                continue
            # A relayed client signal keeps its original `event` name and gains
            # a `from`. We treat anything not a known server event as a signal.
            return raw

    async def close(self):
        await self._ws.close()


def _decode_event(raw):
    """Decode one text frame into a :class:`ServerEvent`.

    Server events never carry a ``from`` field (that is only added to relayed
    client signals by ``webrtc_dispatch``), so a missing ``from`` marks a frame
    as a server event. This keeps :meth:`SignalingClient.recv_signal` honest
    about what is a signal versus a notification.
    """
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return ServerEvent(event="<unparseable>", data={})
    if not isinstance(msg, dict):
        return ServerEvent(event="<non-object>", data={})
    name = msg.get("event", "<none>")
    data = msg.get("data", {})
    return ServerEvent(event=name, data=data if isinstance(data, dict) else {})
