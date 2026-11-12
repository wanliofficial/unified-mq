#!/usr/bin/env python3
"""S9-F real-agent evidence: official ACP Python SDK (agent-client-protocol
==1.0.0), Agent side over stdio.

Topology: the Erlang broker is the ACP *Client*. It spawns THIS process as the
agent subprocess (its `agents` entry names this script) and drives it with
`initialize` / `session/new` / `session/prompt`, answering the single Client
callback `session/request_permission` that this agent issues. This proves our
Client interoperates with a genuine, spec-compliant third-party ACP Agent --
not a hand-rolled escript.

This agent:
  * answers `initialize` with protocolVersion 1 + agent capabilities,
  * answers `session/new` with a fresh sessionId,
  * on `prompt`, emits an `agent_message_chunk` via session/update and returns
    an end_turn PromptResponse,
  * when the prompt text contains "danger", it calls request_permission (the
    client -- our broker -- decides), then honours the decision.
"""
import asyncio
import sys
import uuid

import acp
from acp import (
    Agent, InitializeResponse, NewSessionResponse, PromptResponse,
    RequestPermissionResponse,
)
from acp import schema as S
from acp.schema import (
    Implementation, AgentCapabilities, AgentMessageChunk, TextContentBlock,
    PermissionOption, ToolCall, ToolCallUpdate, PromptCapabilities,
)

# Enums are typing.Literal aliases; use their string members directly.
ToolKind = S.ToolKind
ToolCallStatus = S.ToolCallStatus
PermissionOptionKind = S.PermissionOptionKind


class EvidenceAgent(Agent):
    def __init__(self):
        self._sessions = {}
        self._conn = None

    async def initialize(self, protocol_version, client_capabilities=None,
                         client_info=None, **kwargs):
        sys.stderr.write(
            f"[acp-agent] initialize protocol_version={protocol_version} "
            f"client_info={client_info}\n")
        sys.stderr.flush()
        return InitializeResponse(
            protocolVersion=protocol_version,
            agentCapabilities=AgentCapabilities(
                loadSession=False,
                mcpCapabilities={},
                promptCapabilities=PromptCapabilities(
                    image=False, audio=False, embeddedContext=True),
            ),
            agentInfo=Implementation(
                name="unified-mq-evidence-agent",
                title="S9-F Evidence Agent (official ACP SDK)",
                version="1.0.0",
            ),
        )

    async def new_session(self, cwd, additional_directories=None,
                          mcp_servers=None, **kwargs):
        sid = "acp-sdk-session-" + uuid.uuid4().hex[:8]
        self._sessions[sid] = {"cwd": cwd}
        sys.stderr.write(f"[acp-agent] new_session cwd={cwd} -> {sid}\n")
        sys.stderr.flush()
        return NewSessionResponse(sessionId=sid, modes=None)

    async def authenticate(self, method_id, **kwargs):
        return None

    def on_connect(self, conn):
        # The AgentSideConnection is passed to on_connect; keep it so prompt()
        # can call back to the client (session_update / request_permission).
        self._conn = conn


    async def prompt(self, session_id, prompt, **kwargs):
        text = _prompt_text(prompt)
        sys.stderr.write(f"[acp-agent] prompt session={session_id} text={text!r}\n")
        sys.stderr.flush()

        conn = self._conn  # set by the run wrapper

        low = text.lower()
        if "deny" in low or "danger" in low:
            # "danger" -> kind=execute (allowlisted); "deny" -> kind=edit (not
            # allowlisted, so the broker's deny-by-default policy refuses).
            kind = "edit" if "deny" in low else "execute"
            title = "edit-config" if kind == "edit" else "run-shell"
            # Issue the single Client callback: request_permission. Our broker
            # (the Client) decides via acp_policy.
            # request_permission takes a ToolCallUpdate, not a ToolCall.
            tool = ToolCallUpdate(
                toolCallId="tool-" + uuid.uuid4().hex[:8],
                title=title,
                kind=kind,
                status="pending",
                rawInput={"target": "/tmp/evidence.conf"},
            )
            options = [
                PermissionOption(optionId="allow-once", name="Allow once",
                                 kind="allow_once"),
                PermissionOption(optionId="reject", name="Reject",
                                 kind="reject_once"),
            ]
            resp = await conn.request_permission(
                session_id=session_id, tool_call=tool, options=options)
            sys.stderr.write(
                f"[acp-agent] request_permission -> "
                f"outcome={resp.outcome.outcome}\n")
            sys.stderr.flush()
            if resp.outcome.outcome == "selected":
                await conn.session_update(
                    session_id=session_id,
                    update=AgentMessageChunk(
                        content=TextContentBlock(
                            text="Permission granted; running command.")))
                return PromptResponse(stopReason="end_turn")
            else:
                return PromptResponse(stopReason="refusal")

        # Normal prompt: emit a message chunk and finish.
        await conn.session_update(
            session_id=session_id,
            update=AgentMessageChunk(
                content=TextContentBlock(
                    text=f"Evidence agent handled: {text}")))
        return PromptResponse(stopReason="end_turn")


def _prompt_text(prompt):
    if isinstance(prompt, str):
        return prompt
    out = []
    for block in prompt or []:
        root = getattr(block, "root", block)
        if isinstance(root, TextContentBlock):
            out.append(root.text)
        elif isinstance(root, dict) and "text" in root:
            out.append(root["text"])
    return " ".join(out)


async def _main():
    agent = EvidenceAgent()
    # `run_agent` with default stdio streams (sys.stdin / sys.stdout) is exactly
    # the pipe our `acp_port` transport reads and writes.
    await acp.run_agent(agent)


if __name__ == "__main__":
    asyncio.run(_main())
