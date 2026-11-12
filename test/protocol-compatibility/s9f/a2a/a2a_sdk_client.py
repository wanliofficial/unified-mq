#!/usr/bin/env python3
"""S9-F real-client evidence: official A2A Python SDK (a2a-sdk==1.3.0).

Drives the broker's A2A 1.0.0 JSON-RPC gateway as a genuine third-party A2A
client: resolve the Agent Card from /.well-known/agent-card.json, send a
message (SendMessage -> auto-completed Task), read the Task back (GetTask),
and list tasks. With task_auto_complete=true the broker runs the
SUBMITTED->COMPLETED lifecycle itself, so no external agent backend is needed.

The a2a-sdk 1.3.0 API is protobuf-based (a2a_pb2): send_message takes a
SendMessageRequest and yields an AsyncIterator[StreamResponse].
"""
import asyncio
import json

import httpx

import a2a.types as t
from a2a.client import ClientFactory, ClientConfig, A2ACardResolver

BASE = "http://127.0.0.1:18094/"
TOKEN = "a2a-evidence-token"


def log(*a):
    print(*a, flush=True)


def pb_to_dict(obj):
    """Best-effort conversion of a protobuf message to a plain dict."""
    try:
        from google.protobuf.json_format import MessageToDict
        return MessageToDict(obj, preserving_proto_field_name=True)
    except Exception:
        return {"_repr": str(obj)}


async def main():
    log("=== official A2A SDK client (a2a-sdk 1.3.0, protobuf) ===")
    try:
        import importlib.metadata as md
        log("a2a-sdk version:", md.version("a2a-sdk"))
    except Exception:
        log("a2a-sdk version: (unknown)")

    httpx_client = httpx.AsyncClient(
        headers={"Authorization": f"Bearer {TOKEN}"}, timeout=20.0)

    # Resolve the Agent Card separately so we can show what the broker offers.
    log("\n[1] resolving Agent Card from", BASE + ".well-known/agent-card.json")
    resolver = A2ACardResolver(httpx_client, BASE)
    card = await resolver.get_agent_card()
    card_d = pb_to_dict(card)
    log("    name:", card_d.get("name"))
    log("    description:", card_d.get("description"))
    log("    version:", card_d.get("version"))
    log("    capabilities:", card_d.get("capabilities"))
    log("    supported interfaces:",
        [i.get("url") for i in card_d.get("supportedInterfaces", [])]
        or card_d.get("supported_interfaces"))
    skills = card_d.get("skills", [])
    log("    skills:", [s.get("name") or s.get("id") for s in skills])

    factory = ClientFactory(ClientConfig(
        streaming=False,
        polling=False,
        httpx_client=httpx_client,
        supported_protocol_bindings=["JSONRPC"],
        accepted_output_modes=["text"],
    ))
    log("\n[2] creating client (non-streaming) ...")
    client = await factory.create_from_url(BASE)
    log("    client:", type(client).__name__)

    # SendMessage -> auto-completed Task.
    def make_request(text):
        msg = t.Message(
            message_id="s9f-msg",
            role=t.Role.ROLE_USER,
            parts=[t.Part(text=text)],
        )
        return t.SendMessageRequest(message=msg)

    log("\n[3] SendMessage (task_auto_complete=true) ...")
    result = None
    async for resp in client.send_message(make_request("S9-F evidence: run the lifecycle")):
        result = resp
    if result is not None:
        rd = pb_to_dict(result)
        log("    StreamResponse:", json.dumps(rd, ensure_ascii=False)[:1200])

    # Second message, then GetTask by the resulting task id.
    log("\n[4] second SendMessage ...")
    task_id = None
    async for resp in client.send_message(make_request("second message")):
        rd = pb_to_dict(resp)
        task = rd.get("task")
        if task:
            task_id = task.get("id")
            log("    task id:", task_id)
            log("    task state:", task.get("status", {}).get("state"))

    if task_id:
        log("\n[5] GetTask", task_id, "...")
        try:
            task = await client.get_task(t.GetTaskRequest(id=task_id))
            td = pb_to_dict(task)
            log("    task id:", td.get("id"))
            log("    task state:", td.get("status", {}).get("state"))
            log("    history entries:", len(td.get("history", [])))
        except Exception as e:
            log("    get_task ->", type(e).__name__, str(e)[:300])

    log("\n[6] ListTasks ...")
    try:
        listed = await client.list_tasks(t.ListTasksRequest(page_size=10))
        ld = pb_to_dict(listed)
        tasks = ld.get("tasks", [])
        log("    tasks:", len(tasks),
            [x.get("status", {}).get("state") for x in tasks])
    except Exception as e:
        log("    list_tasks ->", type(e).__name__, str(e)[:300])


    log("\n=== A2A SDK evidence complete ===")
    await httpx_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
