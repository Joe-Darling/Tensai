"""Worker-side MCP server. Claude CLI spawns this in each worker session.

Worker_id is passed via env var ORCH_WORKER_ID set by worker_runner.
Invoked as: python -m mcp_servers.worker_tools
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp.server.fastmcp import FastMCP  # noqa: E402

from daemon.bus import IPCClient  # noqa: E402

SOCKET = os.environ.get("ORCH_IPC_SOCKET", "/tmp/orchestrator.sock")
WORKER_ID = os.environ.get("ORCH_WORKER_ID")
if not WORKER_ID:
    print("WARNING: ORCH_WORKER_ID not set; tool calls will be unrouted.",
          file=sys.stderr)

mcp = FastMCP(name="orchestrator-worker")
client = IPCClient(SOCKET)
_context = {"role": "worker", "worker_id": WORKER_ID}


async def _call(tool: str, args: dict) -> dict:
    return await client.call(tool, args, _context)


@mcp.tool()
async def report_to_om(severity: str, message: str,
                        artifacts: list[str] | None = None) -> dict:
    """Send a status update to the orchestrator. NON-BLOCKING.

    Use for: finished a meaningful phase, discovered something
    noteworthy, recovered from an error. DO NOT use for routine
    "I read a file" chatter.

    severity: 'info' | 'warning' | 'error'
    """
    return await _call("worker_report", {
        "severity": severity, "message": message,
        "artifacts": artifacts or [],
    })


@mcp.tool()
async def request_decision_from_om(question: str, options: list[dict],
                                    context: str = "",
                                    recommendation: str = "") -> dict:
    """Ask the orchestrator for a directional decision. BLOCKING: your
    next action waits until the orchestrator responds.

    Use ONLY when the wrong choice would cost real rework.
    """
    return await _call("worker_request_decision", {
        "question": question, "options": options,
        "context": context, "recommendation": recommendation,
    })


@mcp.tool()
async def mark_task_complete(summary: str,
                              artifacts: list[str] | None = None,
                              open_items: list[str] | None = None) -> dict:
    """Signal task completion. Call this before exiting your session."""
    return await _call("worker_complete", {
        "summary": summary, "artifacts": artifacts or [],
        "open_items": open_items or [],
    })


@mcp.tool()
async def mark_task_failed(reason: str, what_was_tried: str) -> dict:
    """Signal unrecoverable failure."""
    return await _call("worker_failed", {
        "reason": reason, "what_was_tried": what_was_tried,
    })


@mcp.tool()
async def mark_task_paused(progress_summary: str, what_remains: str,
                            recommended_next_turns: int,
                            artifacts: list[str] | None = None) -> dict:
    """Signal that you've used your turn budget but the task isn't
    finished. Call this in the [MAX_TURNS_REACHED] continuation turn
    that the daemon grants you.

    progress_summary: what you accomplished. Be specific.
    what_remains: what's left to do. Concrete, actionable.
    recommended_next_turns: int. Honest estimate of additional turns
        needed to finish. Round up generously.
    artifacts: any files you wrote, as absolute paths (same rules as
        mark_task_complete: real files only, no placeholders).

    The orchestrator will see this and decide whether to resume you
    (with more turns granted) or kill the task. Don't start new work
    in the continuation turn — just report state."""
    return await _call("worker_paused", {
        "progress_summary": progress_summary,
        "what_remains": what_remains,
        "artifacts": artifacts or [],
        "recommended_next_turns": int(recommended_next_turns),
    })


if __name__ == "__main__":
    mcp.run()