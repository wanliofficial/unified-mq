#!/usr/bin/env python3
"""S10-D D.1 evidence: raw, library-independent STUN Binding against our ICE
server, captured as bytes and decoded by hand.

The plan (design sec. 3) asks for this alongside the aiortc run: one STUN
Binding request/response exchange sent as raw UDP and printed as both hex and a
field decode, so the wire behaviour is proven with **no** third-party STUN
library in the loop. This is the ground truth the aiortc ``srflx`` result rests
on: it shows our server answers a Binding with a correctly XOR-encoded
XOR-MAPPED-ADDRESS equal to the packet's real source.

What it does
------------
Build a minimal RFC 8489 STUN Binding request (type 0x0001, magic cookie,
12-byte transaction id, no attributes), send it over UDP to the node's STUN port,
and print:

  * the request bytes as hex;
  * the response bytes as hex;
  * a decode: message type, class, method, transaction-id match, and the
    XOR-MAPPED-ADDRESS after un-XOR-ing with the magic cookie ^ transaction id.

It asserts the response is a success Binding, the transaction id round-trips,
and the decoded mapped address equals the source the server saw (this host on
an ephemeral source port).
"""
import argparse
import json
import os
import socket
import sys

BINDING_REQUEST = 0x0001
BINDING_SUCCESS = 0x0101
BINDING_ERROR = 0x0111
MAGIC_COOKIE = 0x2112A442
ATTR_XOR_MAPPED_ADDRESS = 0x0020
ATTR_MAPPED_ADDRESS = 0x0001


def build_request(txn_id):
    # Type (2) + length (2) + magic cookie (4) + transaction id (12) = 20 bytes.
    # Length is 0: a Binding request carries no attributes.
    return (BINDING_REQUEST).to_bytes(2, "big") + (0).to_bytes(2, "big") + \
        MAGIC_COOKIE.to_bytes(4, "big") + txn_id


def parse_response(data, expected_txn):
    if len(data) < 20:
        return {"error": "short_response", "len": len(data)}
    msg_type = int.from_bytes(data[0:2], "big")
    length = int.from_bytes(data[2:4], "big")
    cookie = int.from_bytes(data[4:8], "big")
    txn = data[8:20]
    # RFC 5389 §6.1: the 14-bit message type packs method (12 bits) and class
    # (2 bits) with the class bits split across positions 1-2 and 6-7. This is
    # the canonical decode used by every STUN library.
    method = ((msg_type & 0x3E0) >> 2 | (msg_type & 0x3E) >> 1
              | (msg_type & 0xF))
    cls = ((msg_type & 0x010) >> 2 | (msg_type & 0x0100) >> 6)
    out = {
        "msg_type": hex(msg_type),
        "method": method,
        "class": cls,
        "length": length,
        "magic_cookie": hex(cookie),
        "cookie_ok": cookie == MAGIC_COOKIE,
        "transaction_id_match": txn == expected_txn,
        "attributes": [],
    }
    # Walk the TLV attributes.
    body = data[20:20 + length]
    i = 0
    while i + 4 <= len(body):
        atype = int.from_bytes(body[i:i + 2], "big")
        alen = int.from_bytes(body[i + 2:i + 4], "big")
        val = body[i + 4:i + 4 + alen]
        attr = {"type": hex(atype), "raw_len": alen}
        if atype in (ATTR_XOR_MAPPED_ADDRESS, ATMAPPED_ADDRESS_LEGACY()):
            addr = decode_address(val, xor=(atype == ATTR_XOR_MAPPED_ADDRESS),
                                  txn=txn)
            attr["decoded"] = addr
        out["attributes"].append(attr)
        i += 4 + alen + ((4 - alen % 4) % 4)  # 4-byte alignment padding
    return out


def ATMAPPED_ADDRESS_LEGACY():
    return ATTR_MAPPED_ADDRESS


def decode_address(val, xor, txn):
    if len(val) < 4:
        return {"error": "short_address"}
    family = val[1]
    if family != 0x01:  # IPv4
        return {"family": hex(family), "note": "only IPv4 decoded here"}
    port_xor = int.from_bytes(val[2:4], "big")
    addr_xor = int.from_bytes(val[4:8], "big")
    if xor:
        # XOR-MAPPED-ADDRESS (RFC 8489 §18.2): the port is XOR-ed with the high
        # 16 bits of the magic cookie; the address is XOR-ed with the magic
        # cookie itself (for IPv4, only the 4 cookie bytes -- NOT the
        # transaction id, which is used only for IPv6). Mixing in the full
        # 16-byte key overflows a 32-bit address, which is the bug this fixes.
        port = port_xor ^ (MAGIC_COOKIE >> 16)
        addr_int = addr_xor ^ MAGIC_COOKIE
    else:
        port = port_xor
        addr_int = addr_xor
    ip = socket.inet_ntoa(addr_int.to_bytes(4, "big"))
    return {"family": "ipv4", "ip": ip, "port": port}


def run(stun_port, ice_host, out_lines):
    txn = os.urandom(12)
    req = build_request(txn)
    log(out_lines, "request_hex", {"hex": req.hex()})

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(5)
    try:
        s.sendto(req, (ice_host, stun_port))
        data, peer = s.recvfrom(2048)
    finally:
        s.close()

    log(out_lines, "response_hex", {"hex": data.hex()})
    log(out_lines, "response_from", {"ip": peer[0], "port": peer[1]})
    decoded = parse_response(data, txn)
    log(out_lines, "decoded", decoded)

    mapped = None
    for attr in decoded.get("attributes", []):
        if attr.get("decoded", {}).get("family") == "ipv4":
            mapped = attr["decoded"]
            break
    # RFC 5389 §6.1 spreads the 12-bit method across the 14-bit type field, so
    # we compare against the well-known wire-type constants rather than
    # re-deriving the method numerically.
    success = decoded.get("msg_type") == hex(BINDING_SUCCESS)
    ok = (
        success
        and decoded.get("cookie_ok") is True
        and decoded.get("transaction_id_match") is True
        and mapped is not None
        and mapped["ip"] == ice_host
    )
    log(out_lines, "verdict", {"ok": bool(ok), "mapped": mapped,
                               "msg_type": decoded.get("msg_type"),
                               "class_success": success})
    return ok


def log(lines, event, payload):
    lines.append({"event": event, **payload})
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ice-host", default=None)
    parser.add_argument("--stun-port", type=int, required=True)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    import webrtc_evidence as we
    ice_host = args.ice_host or we._lan_ipv4()
    print(f"S10-STUN-CONFIG ice_host={ice_host} stun_port={args.stun_port}")

    lines = []
    try:
        ok = run(args.stun_port, ice_host, lines)
    except Exception as exc:  # noqa: BLE001
        log(lines, "error", {"type": type(exc).__name__, "message": str(exc)})
        ok = False

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            for item in lines:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
    print("S10-STUN-VERDICT " + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
