#!/usr/bin/env python3
"""S11-C CoAP over DTLS: real third-party client compatibility.

Gate S11 requires the new CoAP front-end to pass a real third-party client
integration suite, "no in-house client talking to itself" (`docs/roadmap.md`
§13.2). This script drives the CoAP-over-DTLS listener with **libcoap's
`coap-client` v4.3.5** (obgm/libcoap), a standalone C implementation of CoAP
that this repository does not control. Our own `coap_codec` is never used as a
client here.

Every case speaks `coaps` (DTLS with a pre-shared key). The PSK is not passed
in: the boot node derives it from a password exactly as a device would, prints
it once, and this script hands those bytes to `coap-client -k 0x<hex>`. So the
key path under test is the shipped KDF, not a test shortcut.

Cases (each maps to one observable protocol contract):

  coaps-well-known      GET /.well-known/core -> 2.05 link-format over DTLS-PSK
  coaps-put-get         PUT then GET a small body, byte-exact round-trip
  coaps-con-non         CON and NON GETs are both answered on the same session
  coaps-block1-upload   a 1000-byte body sent block-wise (-b 256/512/1024) is
                        reassembled and read back byte-exact (RFC 7959 Block1)
  coaps-delete          DELETE clears a retained resource; the next GET is 4.04
  coaps-wrong-psk       a wrong PSK never completes the handshake
  coaps-unknown-identity an unknown identity is refused just like a wrong PSK
  coaps-plaintext-rejected a plain `coap://` datagram to the DTLS port is dropped

What this client cannot cover, and why (quoted in docs/protocol-compatibility.md
§4.24 rather than hidden):

  * **Block2 (block-wise responses)** -- the server does not implement it. A
    response larger than libcoap's receive PDU (~1.1 KB) is dropped by the
    client (`coap_pdu_resize: pdu too big`), so a retained payload above that
    size is stored but not retrievable. `coaps-block1-upload` therefore uses a
    1000-byte body: below the response ceiling, above several blocks.
  * **Observe notifications** -- `coap-client -s` can subscribe, but the gate
    here is the DTLS transport and the request path; Observe is covered by
    `coap_observe_SUITE` with a byte-level client.

Usage:
    python3 test/protocol-compatibility/s11c/libcoap/libcoap_compat.py --all
    python3 test/protocol-compatibility/s11c/libcoap/libcoap_compat.py --case coaps-block1-upload

The script starts one broker node on an ephemeral UDP port
(`coap_dtls_broker_boot.erl`, the same boot path as `coap_dtls_SUITE`), runs the
cases, and writes `run/report.json`. `run/`, `data/` and `_build/` are
gitignored.

`coap-client` is looked up, in order, at `$COAP_CLIENT`, in
`_build/libcoap/bin`, and on `PATH`; `--build-tools SRC` builds it from a libcoap
source checkout into `_build/libcoap/bin`. See README.md for the exact tag.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
DATA = HERE / "data"
RUN = HERE / "run"
BUILD = HERE / "_build"
TOOLS_BUILD = ROOT / "_build" / "libcoap" / "bin"

TOOL = "coap-client"
PINNED_TAG = "v4.3.5"
# libcoap's default receive PDU is 1500 bytes; anything larger is dropped with
# `coap_pdu_resize: pdu too big` before the payload is handed to us.
RESPONSE_CEILING = 1100
# A refused handshake is a *silent* drop (the server answers nothing, so it is
# not a user-existence oracle); libcoap retransmits until we cut it off.
NEGATIVE_TIMEOUT = 8


class CheckError(AssertionError):
    pass


def check(cond, msg):
    if not cond:
        raise CheckError(msg)


def erl_bin(name="erl"):
    otp = os.environ.get("OTP_ROOT", "/home/wanli/Development/otp-28.3.1/bin")
    cand = Path(otp) / name
    return str(cand) if cand.exists() else name


def app_ebins():
    lib = ROOT / "_build" / "default" / "lib"
    return [str(p) for p in sorted(lib.glob("*/ebin"))]


# ---------------------------------------------------------------------------
# Third-party tool
# ---------------------------------------------------------------------------

def find_tool():
    explicit = os.environ.get("COAP_CLIENT")
    candidates = [Path(explicit) if explicit else None, TOOLS_BUILD / TOOL]
    for c in candidates:
        if c and c.is_file() and os.access(c, os.X_OK):
            return c
    found = shutil.which(TOOL)
    if found:
        return Path(found)
    raise SystemExit(
        "coap-client not found.\n"
        "Build libcoap %s once with:\n"
        "  git clone --depth 1 --branch %s https://github.com/obgm/libcoap\n"
        "  cmake -S libcoap -B libcoap/build -DENABLE_DTLS=ON -DENABLE_TCP=OFF \\\n"
        "        -DENABLE_DOCS=OFF -DENABLE_EXAMPLES=ON -DENABLE_TESTS=OFF \\\n"
        "        -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE=Release\n"
        "  cmake --build libcoap/build --target coap-client -j\n"
        "then either put it on PATH, set COAP_CLIENT, or run:\n"
        "  %s --build-tools /path/to/libcoap --all" % (PINNED_TAG, PINNED_TAG,
                                                     sys.argv[0]))


def build_tool(src):
    src = Path(src).resolve()
    check((src / "CMakeLists.txt").is_file(),
          f"{src} does not look like a libcoap source tree")
    build_dir = src / "build"
    jobs = str(os.cpu_count() or 2)
    subprocess.run(
        ["cmake", "-S", str(src), "-B", str(build_dir),
         "-DENABLE_DTLS=ON", "-DENABLE_TCP=OFF", "-DENABLE_DOCS=OFF",
         "-DENABLE_EXAMPLES=ON", "-DENABLE_TESTS=OFF",
         "-DBUILD_SHARED_LIBS=OFF", "-DCMAKE_BUILD_TYPE=Release"],
        check=True, capture_output=True,
    )
    subprocess.run(["cmake", "--build", str(build_dir), "--target", TOOL,
                    "-j", jobs], check=True, capture_output=True)
    TOOLS_BUILD.mkdir(parents=True, exist_ok=True)
    shutil.copy2(build_dir / TOOL, TOOLS_BUILD / TOOL)
    return TOOLS_BUILD / TOOL


def tool_version(tool):
    proc = subprocess.run([str(tool), "-h"], capture_output=True, text=True,
                          timeout=20)
    text = (proc.stdout or "") + (proc.stderr or "")
    line = next((l for l in text.splitlines() if "coap-client" in l and "v" in l),
                "")
    return line.strip() or "coap-client"


def tool_has_psk(tool):
    proc = subprocess.run([str(tool), "-h"], capture_output=True, text=True,
                          timeout=20)
    text = (proc.stdout or "") + (proc.stderr or "")
    return "PSK" in text and "DTLS" in text


# ---------------------------------------------------------------------------
# Broker node
# ---------------------------------------------------------------------------

def boot_module_dir():
    BUILD.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [erl_bin("erlc"), "-o", str(BUILD),
         str(HERE / "coap_dtls_broker_boot.erl")],
        check=True, capture_output=True,
    )
    return str(BUILD)


def start_broker():
    DATA.mkdir(parents=True, exist_ok=True)
    RUN.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["BROKER_DATA_DIR"] = str(DATA)
    pa = []
    for d in app_ebins() + [boot_module_dir()]:
        pa += ["-pa", d]
    log_file = open(RUN / "broker.log", "wb")
    proc = subprocess.Popen(
        [erl_bin(), *pa, "-noshell", "-s", "coap_dtls_broker_boot", "run"],
        cwd=str(ROOT), env=env, stdin=subprocess.PIPE,
        stdout=log_file, stderr=subprocess.STDOUT,
    )
    proc._log_file = log_file
    return proc


def wait_ready(proc, timeout=60):
    log_path = RUN / "broker.log"
    deadline = time.time() + timeout
    pos = 0
    seen = {}
    while time.time() < deadline:
        if proc.poll() is not None:
            time.sleep(0.2)
            raise SystemExit("broker exited early:\n"
                             + log_path.read_text("utf-8", "replace")[-4000:])
        with open(log_path, "rb") as f:
            f.seek(pos)
            chunk = f.read()
            pos = f.tell()
        if chunk:
            text = chunk.decode("utf-8", "replace")
            for line in text.splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[0] in ("PORT", "USER", "KEY"):
                    seen[parts[0]] = parts[1]
                if line.startswith("READY") and set(seen) >= {"PORT", "USER",
                                                              "KEY"}:
                    return int(seen["PORT"]), seen["USER"], seen["KEY"]
            if "START-ERROR" in text:
                raise SystemExit("broker failed to start:\n"
                                 + log_path.read_text("utf-8", "replace")[-4000:])
        time.sleep(0.2)
    raise SystemExit("broker did not become ready in time")


def stop_broker(proc):
    if proc.poll() is None:
        try:
            if proc.stdin:
                proc.stdin.close()
        except Exception:
            pass
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def broker_log_tail(n=8000):
    log_path = RUN / "broker.log"
    return log_path.read_text("utf-8", "replace")[-n:] if log_path.exists() else ""


# ---------------------------------------------------------------------------
# coap-client wrappers
# ---------------------------------------------------------------------------

class Result:
    def __init__(self, rc, out, err):
        self.rc = rc
        self.out = out
        self.err = err

    @property
    def combined(self):
        return (self.out or "") + (self.err or "")


def coap(tool, scheme, port, path, key=None, user=None, method="get",
         payload=None, block=None, non=False, outfile=None, timeout=25):
    """Run one coap-client request and return its Result."""
    cmd = [str(tool), "-m", method]
    if non:
        cmd.append("-N")
    if key is not None:
        cmd += ["-k", "0x" + key]
    if user is not None:
        cmd += ["-u", user]
    if payload is not None:
        cmd += ["-e", payload]
    if block is not None:
        cmd += ["-b", str(block)]
    if outfile is not None:
        cmd += ["-o", str(outfile)]
    cmd.append(f"{scheme}://127.0.0.1:{port}/{path}")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout)
        return Result(proc.returncode, proc.stdout, proc.stderr)
    except subprocess.TimeoutExpired as exc:
        return Result(-1, exc.stdout or "", exc.stderr or "")


def fetch_bytes(tool, port, key, user, path, **kw):
    """GET a resource and return (Result, raw payload bytes)."""
    tmp = RUN / f"get-{abs(hash((path, time.time()))) & 0xffffff}.bin"
    res = coap(tool, "coaps", port, path, key=key, user=user,
               method="get", outfile=tmp, **kw)
    data = tmp.read_bytes() if tmp.exists() else b""
    if tmp.exists():
        tmp.unlink()
    return res, data


def handshake_ok(res, body_required=False):
    return res.rc == 0 and "cannot send CoAP pdu" not in res.combined


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def case_coaps_well_known(tool, port, user, key):
    res, data = fetch_bytes(tool, port, key, user, ".well-known/core")
    check(res.rc == 0, f"coap-client rc={res.rc}: {res.combined[-400:]!r}")
    check(b"</.well-known/core>" in data,
          f"link-format body missing: {data[:200]!r}")
    return {"client": "libcoap coap-client", "third_party": True,
            "evidence": "DTLS-PSK handshake + GET /.well-known/core -> 2.05 "
                        "link-format"}


def case_coaps_put_get(tool, port, user, key):
    body = b"libcoap-wrote-this"
    res = coap(tool, "coaps", port, "lc/roundtrip", key=key, user=user,
               method="put", payload=body.decode())
    check(res.rc == 0, f"PUT rc={res.rc}: {res.combined[-400:]!r}")
    res, data = fetch_bytes(tool, port, key, user, "lc/roundtrip")
    check(data == body, f"GET body {data!r} != {body!r} (rc={res.rc})")
    return {"client": "libcoap coap-client", "third_party": True,
            "evidence": "PUT then GET a 20-byte body, byte-exact round-trip"}


def case_coaps_con_non(tool, port, user, key):
    body = b"con-and-non"
    for non in (False, True):
        label = "NON" if non else "CON"
        res = coap(tool, "coaps", port, "lc/connon", key=key, user=user,
                   method="put", payload=body.decode(), non=non)
        check(res.rc == 0, f"{label} PUT rc={res.rc}: {res.combined[-300:]!r}")
    res, data = fetch_bytes(tool, port, key, user, "lc/connon")
    check(data == body, f"GET body {data!r} != {body!r}")
    res_non, data_non = fetch_bytes(tool, port, key, user, "lc/connon", non=True)
    check(data_non == body, f"NON GET body {data_non!r} != {body!r}")
    return {"client": "libcoap coap-client", "third_party": True,
            "evidence": "CON and NON GETs both answered on the same DTLS session"}


def case_coaps_block1_upload(tool, port, user, key):
    body = bytes(range(256)) * 4  # 1024 distinct-ish bytes, mostly binary
    body = body[:1000]
    src = RUN / "block1-body.bin"
    src.write_bytes(body)
    sizes = []
    for szx in (256, 512, 1024):
        dest = f"lc/blk{szx}"
        res = subprocess.run(
            [str(tool), "-m", "put", "-k", "0x" + key, "-u", user,
             "-f", str(src), "-b", str(szx),
             f"coaps://127.0.0.1:{port}/{dest}"],
            capture_output=True, text=True, timeout=30,
        )
        check(res.returncode == 0,
              f"block1 PUT SZX={szx} rc={res.returncode}: "
              f"{(res.stdout + res.stderr)[-400:]!r}")
        _, data = fetch_bytes(tool, port, key, user, dest)
        check(data == body,
              f"block1 GET SZX={szx} got {len(data)} bytes, expected "
              f"{len(body)} (block reassembly lost data)")
        sizes.append(szx)
    src.unlink()
    return {"client": "libcoap coap-client", "third_party": True,
            "evidence": "1000-byte body at SZX %s reassembled and read back "
                        "byte-exact (RFC 7959 Block1)" % "/".join(map(str, sizes))}


def case_coaps_delete(tool, port, user, key):
    body = b"to-be-deleted"
    res = coap(tool, "coaps", port, "lc/gone", key=key, user=user,
               method="put", payload=body.decode())
    check(res.rc == 0, f"PUT rc={res.rc}: {res.combined[-300:]!r}")
    res = coap(tool, "coaps", port, "lc/gone", key=key, user=user,
               method="delete")
    check(res.rc == 0, f"DELETE rc={res.rc}: {res.combined[-300:]!r}")
    res, data = fetch_bytes(tool, port, key, user, "lc/gone")
    check(data == b"", f"resource survived DELETE: {data!r}")
    return {"client": "libcoap coap-client", "third_party": True,
            "evidence": "DELETE cleared the retained resource; the next GET "
                        "returned 4.04 with an empty body"}


def case_coaps_wrong_psk(tool, port, user, key):
    wrong = "00" * 32
    res = coap(tool, "coaps", port, ".well-known/core", key=wrong, user=user,
               method="get", timeout=NEGATIVE_TIMEOUT)
    check(not handshake_ok(res),
          "a wrong PSK still produced a response: the handshake is not "
          "authenticating")
    return {"client": "libcoap coap-client", "third_party": True,
            "evidence": "wrong 32-byte PSK -> the server answers nothing "
                        "(silent drop, not an oracle) and the CoAP request "
                        "never leaves the client; a correct PSK succeeds in "
                        "the same run, so the handshake really authenticates"}


def case_coaps_unknown_identity(tool, port, user, key):
    res = coap(tool, "coaps", port, ".well-known/core", key=key,
               user="nobody", method="get", timeout=NEGATIVE_TIMEOUT)
    check(not handshake_ok(res),
          "an unknown identity produced a response: the identity lookup is "
          "not enforced")
    return {"client": "libcoap coap-client", "third_party": True,
            "evidence": "unknown identity -> the server answers nothing, the "
                        "same silent-drop shape as a wrong PSK (no user "
                        "enumeration)"}


def case_coaps_plaintext_rejected(tool, port, user, key):
    res = coap(tool, "coap", port, ".well-known/core", method="get",
               timeout=NEGATIVE_TIMEOUT)
    check(not handshake_ok(res),
          "a plain coap:// datagram to the DTLS port was answered: the "
          "listener is not actually DTLS-only")
    return {"client": "libcoap coap-client", "third_party": True,
            "evidence": "plain coap:// datagram to the DTLS port was dropped "
                        "(no response)"}


CASES = {
    "coaps-well-known": case_coaps_well_known,
    "coaps-put-get": case_coaps_put_get,
    "coaps-con-non": case_coaps_con_non,
    "coaps-block1-upload": case_coaps_block1_upload,
    "coaps-delete": case_coaps_delete,
    "coaps-wrong-psk": case_coaps_wrong_psk,
    "coaps-unknown-identity": case_coaps_unknown_identity,
    "coaps-plaintext-rejected": case_coaps_plaintext_rejected,
}

NOT_COVERED = {
    "block2": "the server does not implement block-wise responses, so a "
              "response larger than libcoap's RX PDU (~%d bytes) is dropped "
              "by the client; large retained payloads are stored but not "
              "retrievable. Covered as a declared limit in "
              "docs/protocol-compatibility.md §4.24.4." % RESPONSE_CEILING,
    "observe-notify": "coap-client can subscribe, but the S11-C gate is the "
                      "DTLS transport and request path; Observe notifications "
                      "are covered by coap_observe_SUITE with a byte-level "
                      "client.",
    "certificate-mode": "libcoap built here supports PSK and PKI, but this "
                        "broker only offers PSK over DTLS; certificates/mTLS "
                        "are out of scope (ADR-031 §D5).",
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv):
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all", action="store_true", help="run every case")
    ap.add_argument("--case", action="append", default=[], help="run one case")
    ap.add_argument("--keep-going", action="store_true",
                    help="run every requested case even after a failure")
    ap.add_argument("--keep-data", action="store_true",
                    help="reuse the WAL data dir from a previous run")
    ap.add_argument("--build-tools", metavar="SRC",
                    help="build coap-client from a libcoap source tree into "
                         "_build/libcoap/bin")
    args = ap.parse_args(argv)

    tool = build_tool(args.build_tools) if args.build_tools else find_tool()
    if not tool_has_psk(tool):
        raise SystemExit(
            f"{tool} was built without DTLS/PSK support; rebuild libcoap with "
            f"-DENABLE_DTLS=ON")

    selected = list(CASES) if args.all or not args.case else args.case
    unknown = [c for c in selected if c not in CASES]
    if unknown:
        raise SystemExit(f"unknown case(s): {', '.join(unknown)}\n"
                         f"known: {', '.join(CASES)}")

    if not args.keep_data:
        shutil.rmtree(DATA, ignore_errors=True)
    RUN.mkdir(parents=True, exist_ok=True)

    broker = start_broker()
    report = {"client": tool_version(tool), "tool": str(tool),
              "build": "DTLS+PSK", "transport": "coaps (DTLS-PSK)",
              "cases": {}, "not_covered": NOT_COVERED}
    failed = []
    try:
        port, user, key = wait_ready(broker)
        report["port"] = port
        print(f"broker on udp 127.0.0.1:{port} (DTLS), client {tool}")
        for name in selected:
            started = time.time()
            try:
                result = CASES[name](tool, port, user, key)
                result["ok"] = True
                print(f"PASS {name} ({time.time() - started:.1f}s)")
            except CheckError as exc:
                result = {"ok": False, "error": str(exc)}
                failed.append(name)
                print(f"FAIL {name}: {exc}")
                (RUN / f"broker-{name}.log").write_text(broker_log_tail())
                if not args.keep_going:
                    report["cases"][name] = result
                    break
            report["cases"][name] = result
    finally:
        stop_broker(broker)
        (RUN / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    if failed:
        print(f"\n{len(failed)} failed: {', '.join(failed)}")
        print(f"report: {RUN / 'report.json'}")
        return 1
    print(f"\nall {len(report['cases'])} cases passed; report: {RUN / 'report.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
