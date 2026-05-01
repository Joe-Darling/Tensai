"""Event bus + IPC."""
from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventKind(str, Enum):
    DISCORD_MESSAGE = "discord"
    WORKER_TOOL_CALL = "worker_tool_call"
    OM_TOOL_CALL = "om_tool_call"
    TIMER = "timer"
    COMPACTION_REQUIRED = "compaction_required"
    COMPACTION_IMMINENT = "compaction_imminent"
    SCHEDULED_FOLLOWUP = "scheduled_followup"
    REPLY_DROPPED_CHECK = "reply_dropped_check"
    WORKER_QUIET = "worker_quiet"


@dataclass
class Event:
    kind: EventKind
    payload: dict[str, Any]
    reply_future: asyncio.Future | None = field(default=None)


# Priorities for the bus. Lower number = higher priority. User
# Discord messages and reaction-driven escalation responses jump
# ahead of worker tool-call events and other system-generated work
# so OM responds to the user first when the queue has built up.
PRIORITY_USER = 0
PRIORITY_DEFAULT = 10


class EventBus:
    def __init__(self) -> None:
        # PriorityQueue items are (priority:int, sequence:int, event).
        # The sequence is a monotonic tiebreaker so two same-priority
        # events stay FIFO with each other (and so heapq doesn't try
        # to compare Event objects, which aren't orderable).
        self._q: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._seq: int = 0

    def _priority_for(self, event: Event) -> int:
        # User input gets the front of the queue.
        if event.kind is EventKind.DISCORD_MESSAGE:
            return PRIORITY_USER
        return PRIORITY_DEFAULT

    async def publish(self, event: Event) -> None:
        self._seq += 1
        priority = self._priority_for(event)
        await self._q.put((priority, self._seq, event))

    async def consume(self) -> Event:
        _prio, _seq, event = await self._q.get()
        return event

    async def publish_and_wait(self, event: Event, timeout: float | None = None) -> Any:
        loop = asyncio.get_event_loop()
        event.reply_future = loop.create_future()
        await self.publish(event)
        if timeout:
            return await asyncio.wait_for(event.reply_future, timeout=timeout)
        return await event.reply_future


class IPCServer:
    def __init__(self, socket_path, bus: EventBus, handler):
        self.socket_path = socket_path
        self.bus = bus
        self.handler = handler
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        import os
        try:
            os.unlink(self.socket_path)
        except FileNotFoundError:
            pass
        self._server = await asyncio.start_unix_server(
            self._handle_client, path=str(self.socket_path)
        )

    async def _handle_client(self, reader, writer) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    req = json.loads(line.decode())
                except Exception as e:
                    resp = {"id": None, "ok": False, "error": f"bad json: {e}"}
                    writer.write((json.dumps(resp) + "\n").encode())
                    await writer.drain()
                    continue

                req_id = req.get("id") or str(uuid.uuid4())
                try:
                    result = await self.handler(
                        req["tool"], req.get("args", {}), req.get("context", {})
                    )
                    resp = {"id": req_id, "ok": True, "result": result}
                except Exception as e:
                    resp = {"id": req_id, "ok": False, "error": str(e)}
                writer.write((json.dumps(resp) + "\n").encode())
                await writer.drain()
        finally:
            writer.close()


class IPCClient:
    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_unix_connection(self.socket_path)

    async def call(self, tool: str, args: dict, context: dict) -> Any:
        async with self._lock:
            if not self._writer:
                await self.connect()
            req_id = str(uuid.uuid4())
            req = {"id": req_id, "op": "tool_call", "tool": tool,
                   "args": args, "context": context}
            assert self._writer and self._reader
            self._writer.write((json.dumps(req) + "\n").encode())
            await self._writer.drain()
            line = await self._reader.readline()
            resp = json.loads(line.decode())
            if not resp.get("ok"):
                raise RuntimeError(resp.get("error", "unknown"))
            return resp["result"]