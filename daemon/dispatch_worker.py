"""IPC dispatch for worker-side tool calls.

Each worker has its own MCP stdio server that forwards tool calls to
the daemon. ``dispatch_worker_tool`` routes the call to a handler and
also publishes a corresponding ``WORKER_TOOL_CALL`` event onto the bus
so the OM gets re-invoked with the worker's update.

Worker tools:

- ``worker_report``: progress heartbeat (info / warning / error).
- ``worker_request_decision``: blocking ask-the-OM call.
- ``worker_complete``: terminal success.
- ``worker_failed``: terminal failure.
- ``worker_paused``: max-turns hit; OM decides whether to resume.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable

from .bus import Event, EventKind

if TYPE_CHECKING:
    from .main import Daemon

Handler = Callable[["Daemon", str, dict], Awaitable[dict]]


async def _worker_report(daemon: "Daemon", worker_id: str,
                          args: dict) -> dict:
    result = await daemon.workers.handle_report(
        worker_id, args["severity"], args["message"],
        args.get("artifacts", []),
    )
    await daemon.bus.publish(Event(
        kind=EventKind.WORKER_TOOL_CALL,
        payload={"worker_id": worker_id, "kind": "report",
                 "payload": args},
    ))
    return result


async def _worker_request_decision(daemon: "Daemon", worker_id: str,
                                     args: dict) -> dict:
    # Decision request is interesting: the WORKER_TOOL_CALL event is
    # published BEFORE handle_decision_request blocks, so the OM gets
    # invoked with the request while the worker is still suspended.
    await daemon.bus.publish(Event(
        kind=EventKind.WORKER_TOOL_CALL,
        payload={"worker_id": worker_id, "kind": "decision_request",
                 "payload": args},
    ))
    return await daemon.workers.handle_decision_request(
        worker_id, args["question"], args["options"],
        args.get("context", ""), args.get("recommendation", ""),
    )


async def _worker_complete(daemon: "Daemon", worker_id: str,
                             args: dict) -> dict:
    result = await daemon.workers.handle_completion(
        worker_id, args["summary"], args.get("artifacts", []),
        args.get("open_items", []),
    )
    w = await daemon.db.get_worker(worker_id)
    await daemon.db.log_event(
        kind="worker_completed",
        project_id=w["project_id"] if w else None,
        summary=f"worker {worker_id} completed: {args['summary'][:200]}",
        details={"artifacts": args.get("artifacts", [])},
    )
    await daemon.bus.publish(Event(
        kind=EventKind.WORKER_TOOL_CALL,
        payload={"worker_id": worker_id, "kind": "completion",
                 "payload": args},
    ))
    return result


async def _worker_failed(daemon: "Daemon", worker_id: str,
                          args: dict) -> dict:
    result = await daemon.workers.handle_failure(
        worker_id, args["reason"], args["what_was_tried"]
    )
    w = await daemon.db.get_worker(worker_id)
    await daemon.db.log_event(
        kind="worker_failed",
        project_id=w["project_id"] if w else None,
        summary=f"worker {worker_id} failed: {args['reason'][:200]}",
    )
    await daemon.bus.publish(Event(
        kind=EventKind.WORKER_TOOL_CALL,
        payload={"worker_id": worker_id, "kind": "failure",
                 "payload": args},
    ))
    return result


async def _worker_paused(daemon: "Daemon", worker_id: str,
                          args: dict) -> dict:
    result = await daemon.workers.handle_paused(
        worker_id,
        args["progress_summary"],
        args["what_remains"],
        args.get("artifacts", []),
        int(args.get("recommended_next_turns", 0)),
    )
    w = await daemon.db.get_worker(worker_id)
    await daemon.db.log_event(
        kind="worker_paused",
        project_id=w["project_id"] if w else None,
        summary=(f"worker {worker_id} paused (hit max-turns): "
                 f"{args['progress_summary'][:200]}"),
    )
    await daemon.bus.publish(Event(
        kind=EventKind.WORKER_TOOL_CALL,
        payload={"worker_id": worker_id, "kind": "paused",
                 "payload": args},
    ))
    return result


WORKER_TOOL_HANDLERS: dict[str, Handler] = {
    "worker_report": _worker_report,
    "worker_request_decision": _worker_request_decision,
    "worker_complete": _worker_complete,
    "worker_failed": _worker_failed,
    "worker_paused": _worker_paused,
}


async def dispatch_worker_tool(daemon: "Daemon", tool: str, args: dict,
                                worker_id: str) -> dict:
    handler = WORKER_TOOL_HANDLERS.get(tool)
    if handler is None:
        raise ValueError(f"unknown worker tool: {tool}")
    return await handler(daemon, worker_id, args)
