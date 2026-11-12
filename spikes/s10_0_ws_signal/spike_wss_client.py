#!/usr/bin/env python3
"""S10-0 spike: measure the `wss` path (ranch ssl transport).

Two things worth knowing before S10-A commits to TLS:
  1. does ranch 2.3.0's ssL transport hand us a socket that the same
     ws_handshake / ws_frame code works on, unchanged?
  2. what does a self-signed cert cost at connection setup (the number S10-D
     will need for a capacity claim)?

A self-signed cert is generated here, on the fly, in the spike's own directory.
Nothing is installed system-wide and no fixed port is used.
"""
import asyncio
import json
import ssl
import ssl as _ssl
import sys

import websockets


def make_cert(directory):
    import subprocess
    key = f"{directory}/spike-key.pem"
    crt = f"{directory}/spike-cert.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", key, "-out", crt, "-days", "1",
         "-subj", "/CN=127.0.0.1"],
        check=True, capture_output=True)
    return key, crt


async def scenario(port, outfile, certdir):
    key, crt = make_cert(certdir)
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE

    uri = f"wss://127.0.0.1:{port}/websocket/room1"
    log = []
    async with websockets.connect(uri, ssl=ctx, subprotocols=["webrtc.v1"]) as a:
        log.append(("wss_status", a.response.status_code))
        log.append(("wss_subprotocol", a.subprotocol))
        log.append(("wss_tls_version", a.transport.get_extra_info("ssl_object").version()))
        await a.send(json.dumps({"event": "authenticate", "data": {}}))
        log.append(("wss_authenticated", json.loads(await a.recv())))
        await a.send("ping")
        log.append(("wss_text_pong", await a.recv()))

    with open(outfile, "w", encoding="utf-8") as fh:
        for item in log:
            fh.write(repr(item) + "\n")
    for item in log:
        print(item)


def main():
    if len(sys.argv) != 4:
        print("usage: spike_wss_client.py <port> <outfile> <certdir>", file=sys.stderr)
        return 2
    asyncio.run(scenario(int(sys.argv[1]), sys.argv[2], sys.argv[3]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
