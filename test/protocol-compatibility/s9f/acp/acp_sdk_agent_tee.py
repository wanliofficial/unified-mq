#!/usr/bin/env python3
"""Run the official ACP SDK agent, recording the *raw* client->agent JSON
lines to `client_to_agent.jsonl` on the way in.

The SDK reads fd 0 via `asyncio.connect_read_pipe(sys.stdin)`, so a Python-level
file wrapper would be bypassed. Instead we splice an OS pipe in front of fd 0
and tee with a thread: the agent is untouched, every byte the broker's
`acp_port` writes is logged verbatim.
"""
import os
import runpy
import sys
import threading

LOG = os.environ.get("S9F_AGENT_FRAMES",
                     "/tmp/opencode/s9f-evidence/acp/client_to_agent.jsonl")


def main():
    open(LOG, "wb").close()

    # pipe: [0] read end (becomes fd 0), [1] write end (tee target)
    pipe_r, pipe_w = os.pipe()
    real_stdin = os.dup(0)
    log_fd = os.open(LOG, os.O_WRONLY | os.O_APPEND)

    def pump():
        while True:
            chunk = os.read(real_stdin, 65536)
            if not chunk:
                try:
                    os.close(pipe_w)
                except OSError:
                    pass
                return
            os.write(log_fd, chunk)
            try:
                os.write(pipe_w, chunk)
            except OSError:
                return

    threading.Thread(target=pump, daemon=True).start()
    os.dup2(pipe_r, 0)
    os.close(pipe_r)
    sys.stdin = os.fdopen(0, "rb", buffering=0)

    runpy.run_path(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "acp_sdk_agent.py"),
        run_name="__main__")


if __name__ == "__main__":
    main()
