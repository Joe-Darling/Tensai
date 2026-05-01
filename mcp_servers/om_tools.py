"""OM-side MCP server."""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp.server.fastmcp import FastMCP  # noqa: E402

from daemon.bus import IPCClient  # noqa: E402

SOCKET = os.environ.get("ORCH_IPC_SOCKET") or (
    sys.argv[1] if len(sys.argv) > 1 else "/tmp/orchestrator.sock"
)

mcp = FastMCP(name="orchestrator-om")
client = IPCClient(SOCKET)
_context = {"role": "om"}


async def _call(tool: str, args: dict) -> dict:
    return await client.call(tool, args, _context)


@mcp.tool()
async def spawn_worker(project_id: str, task: str, model: str = "sonnet",
                        allow_edits: bool = True, max_turns: int = 50,
                        enable_agent_teams: bool = False,
                        engine: str = "claude") -> dict:
    """Spawn a worker on a registered project.

    engine: "claude" (default) or "codex" (OpenAI Codex CLI, runs
    against the user's ChatGPT Plus subscription).

    AUTO-ROUTING: when Claude is in rate-limit pressure (typically
    the 'allowed_warning' state at ~90% of the 5-hour quota), the
    daemon AUTOMATICALLY reroutes engine="claude" spawns to Codex
    to preserve Claude budget for OM itself. The spawn still
    succeeds; the response will have:
      auto_rerouted_to_codex: true
      engine: "codex"
      requested_engine: "claude"
    A 🔀 notice is posted to the project channel. When you see this,
    mention to the user that the worker was offloaded to Codex.

    When to pick "codex" explicitly:
    - The user asks ("use codex for this", "save Claude budget").
    - Bulk mechanical edits where speed of completion matters
      more than nuanced reasoning.
    - You want to preempt the auto-routing threshold and offload
      earlier.

    When to STAY with "claude":
    - Long, complex reasoning tasks.
    - Tasks where the worker may need turn-budget continuation
      (Codex doesn't support paused/resume).
    - Tasks where the user will likely want to send_to_worker mid-task
      to redirect (not supported on Codex).

    Codex worker capabilities (Phase 1 with MCP):
    - Full orchestrator tool surface: report_to_om,
      request_decision_from_om, mark_task_complete, mark_task_failed
    - tail_worker_output works (engine-aware parsing)
    - File edits, bash commands work via Codex's --full-auto mode

    Codex worker limitations:
    - No send_to_worker mid-task (kill + respawn instead)
    - No session resume — once exited, the session is gone
    - No max_turns enforcement at CLI level (daemon sweep still applies)

    model: For Claude, "sonnet" / "opus" / "haiku". For Codex, leave
    as default (codex picks current best) or pass an OpenAI model
    name like "gpt-5-codex"; these are passed through if not one of
    the Claude-style names.

    Returns: {worker_id, spawned_at, engine, requested_engine,
    auto_rerouted_to_codex}."""
    return await _call("spawn_worker", {
        "project_id": project_id, "task": task, "model": model,
        "allow_edits": allow_edits, "max_turns": max_turns,
        "enable_agent_teams": enable_agent_teams,
        "engine": engine,
    })


@mcp.tool()
async def send_to_worker(worker_id: str, message: str,
                          max_turns: int = 50) -> dict:
    """Send a follow-up message to a completed/paused worker, resuming
    its session.

    max_turns: budget for this resumption. Default 50. When resuming a
    worker that was paused due to hitting max-turns, set this to at
    least the worker's recommended_next_turns + buffer (e.g. 1.5x).
    Setting too low will just hit max-turns again."""
    return await _call("send_to_worker", {
        "worker_id": worker_id, "message": message,
        "max_turns": max_turns,
    })


@mcp.tool()
async def get_worker_status(worker_id: str) -> dict:
    """Get detailed status for a worker — state, activity timestamps,
    turn count, tool-call count, last few tool names."""
    return await _call("get_worker_status", {"worker_id": worker_id})


@mcp.tool()
async def tail_worker_output(worker_id: str, n: int = 20) -> dict:
    """Read the last N tool calls a worker made plus their results.
    Each entry has: tool name, input_summary (path + range for Read,
    command for Bash, etc.), output_summary (truncated tool output),
    is_error, in_progress (true for the most recent call if it hasn't
    returned yet — useful for 'what is the worker waiting on right
    now'). Skips orchestrator-MCP calls (worker→OM chatter).

    Use this when:
    - A worker has gone quiet (WORKER_QUIET event) and you want to see
      what it's actually doing before deciding to kill or wait.
    - get_worker_status shows tool names but you need the actual
      content of those calls to triage.
    - You suspect a worker is stuck on something — the in_progress
      flag tells you which tool call hasn't completed.

    n defaults to 20, capped at 50."""
    return await _call("tail_worker_output", {
        "worker_id": worker_id, "n": int(n),
    })


@mcp.tool()
async def list_workers(project_id: str | None = None,
                        state: str | None = None) -> dict:
    """Inventory of workers, optionally filtered."""
    return await _call("list_workers", {
        "project_id": project_id, "state": state,
    })


@mcp.tool()
async def kill_worker(worker_id: str, reason: str) -> dict:
    """Gracefully cancel a worker."""
    return await _call("kill_worker", {
        "worker_id": worker_id, "reason": reason,
    })


@mcp.tool()
async def respond_to_worker_decision(worker_id: str, chosen_option: str,
                                      reasoning: str = "") -> dict:
    """Answer a worker's blocking request_decision_from_om call."""
    return await _call("respond_to_worker_decision", {
        "worker_id": worker_id, "chosen_option": chosen_option,
        "reasoning": reasoning,
    })


@mcp.tool()
async def escalate_to_pm(summary: str, context: str, urgency: str = "fyi",
                          options: list[dict] | None = None,
                          recommendation: str = "",
                          related_worker_id: str = "") -> dict:
    """Ping the user on Discord for a decision.

    Each option in the options list should be a dict with 'label' and
    'consequences'. When options are provided, the daemon seeds numeric
    reactions (1⃣..4⃣, up to 4 options) on the posted message. The
    user can tap a reaction to resolve the escalation — that
    synthesizes a 'chose option N' message back to you. They can also
    respond in text as before. Use reactions for quick yes/no or
    multi-choice; text replies are still available for anything
    nuanced."""
    return await _call("escalate_to_pm", {
        "summary": summary, "context": context, "urgency": urgency,
        "options": options or [], "recommendation": recommendation,
        "related_worker_id": related_worker_id or None,
    })


@mcp.tool()
async def load_project_context(project_id: str,
                                include: list[str] | None = None,
                                notes_limit: int = 20,
                                notes_since: str = "",
                                workers_limit: int = 10) -> dict:
    """Pull project context with pagination/filtering.

    include: which sections to load. Options: "claude_md", "notes",
      "workers". Default: all three.
    notes_limit: max notes to include (most recent). Default 20.
    notes_since: ISO datetime; only notes newer than this. Empty = no filter.
    workers_limit: max recent workers to include (most recent first,
      across all states). Default 10, capped at 50.

    When "workers" is included, `recent_workers` entries now carry the
    completion event's artifacts, rejected_artifacts, and open_items
    in addition to the base worker-row fields. This lets you answer
    'what did a worker do on project X?' without relying on the lossy
    compaction summary or the start-of-session event log."""
    return await _call("load_project_context", {
        "project_id": project_id,
        "include": include,
        "notes_limit": notes_limit,
        "notes_since": notes_since or None,
        "workers_limit": workers_limit,
    })


@mcp.tool()
async def save_project_note(project_id: str, note: str,
                             category: str = "observation",
                             replace_latest: bool = False) -> dict:
    """Append a note to a project's running notes file."""
    return await _call("save_project_note", {
        "project_id": project_id, "note": note, "category": category,
        "replace_latest": replace_latest,
    })


@mcp.tool()
async def list_projects() -> dict:
    """List all registered projects."""
    return await _call("list_projects", {})


@mcp.tool()
async def scaffold_project_directory(path: str) -> dict:
    """Create a new project directory under projects_root."""
    return await _call("scaffold_project_directory", {"path": path})


@mcp.tool()
async def register_project(project_id: str, path: str,
                            description: str,
                            max_concurrent_workers: int | None = None) -> dict:
    """Register a new project. Optional max_concurrent_workers caps
    how many workers can run in parallel on this project (default
    behavior: use the global default of 3 if unset). Pass a higher
    number for projects that benefit from parallelism (independent
    files, no shared state); a lower one (1 or 2) for projects where
    workers would step on each other (shared config files, single
    binary build, etc)."""
    return await _call("register_project", {
        "project_id": project_id, "path": path,
        "description": description,
        "max_concurrent_workers": max_concurrent_workers,
    })


@mcp.tool()
async def set_project_worker_cap(project_id: str,
                                   max_workers: int | None) -> dict:
    """Adjust the per-project worker cap on the fly. max_workers=null
    clears the cap (project falls back to the global default of 3).
    Use to raise the cap for projects that are clearly safe with more
    parallelism, or lower it after seeing collisions.

    The cap is enforced at spawn time — if a project is at its limit,
    spawn_worker raises a ValueError listing the count and cap, and
    you decide whether to kill_worker something or wait."""
    return await _call("set_project_worker_cap", {
        "project_id": project_id, "max_workers": max_workers,
    })


@mcp.tool()
async def archive_project(project_id: str,
                            delete_channel: bool = True) -> dict:
    """Remove a project from the orchestrator: deletes the project row,
    its workers, worker events, escalations, and cancels pending
    scheduled events tied to it. By default also deletes the Discord
    project channel. The project's filesystem at its registered path
    is NOT touched — re-registering with the same path restores it
    fresh.

    Refuses if any worker for the project is still in 'running' state.
    Kill those first via kill_worker, then retry.

    Use when:
    - The user asks to clean up a mock or abandoned project.
    - A project has been replaced by another and you want the channel
      gone to keep Discord readable.
    - A project was registered by mistake.

    Don't use as a general 'free up DB space' — the periodic worker
    purge handles that on its own."""
    return await _call("archive_project", {
        "project_id": project_id,
        "delete_channel": delete_channel,
    })


@mcp.tool()
async def reply_to_user(message: str, project_id: str = "",
                         mention: bool = False) -> dict:
    """Post a text message to the user."""
    return await _call("reply_to_user", {
        "message": message,
        "project_id": project_id or None,
        "mention": mention,
    })


@mcp.tool()
async def send_file_to_user(file_path: str, message: str = "",
                              project_id: str = "") -> dict:
    """Send a file attachment to the user."""
    return await _call("send_file_to_user", {
        "file_path": file_path,
        "message": message,
        "project_id": project_id or None,
    })


@mcp.tool()
async def zip_project(project_id: str, output_name: str = "") -> dict:
    """Create a zip of a project at /tmp."""
    return await _call("zip_project", {
        "project_id": project_id,
        "output_name": output_name,
    })


@mcp.tool()
async def request_compaction(reason: str) -> dict:
    """Queue a compaction turn."""
    return await _call("request_compaction", {"reason": reason})


@mcp.tool()
async def finalize_compaction(summary: str) -> dict:
    """Complete a compaction turn. MANDATORY during [COMPACTION_REQUIRED]."""
    return await _call("finalize_compaction", {"summary": summary})


@mcp.tool()
async def get_orchestrator_stats() -> dict:
    """Snapshot of operating budget and activity — session tokens,
    compaction thresholds, workers spawned today, pending scheduled
    events, and Anthropic rate-limit state (reset times, allow/warn/
    reject status, no % consumed)."""
    return await _call("get_orchestrator_stats", {})


@mcp.tool()
async def schedule_followup(reason: str,
                              delay_minutes: float = 0,
                              fire_at_iso: str = "",
                              project_id: str = "",
                              condition: str = "") -> dict:
    """Schedule a [SCHEDULED_FOLLOWUP] event to fire in the future.
    When it fires, you'll be re-invoked with an event that includes
    the reason you gave here, so you remember why.

    Pass EXACTLY ONE of:
    - delay_minutes: float, fire this many minutes from now. Must be > 0.
    - fire_at_iso: ISO-8601 UTC timestamp (e.g. '2026-04-25T10:00:00+00:00').

    Params:
    - reason: a short string describing what you want to do when this
      fires. Be specific — this is what reminds future-you why you
      set the reminder. Examples: 'check if worker 3a8b is done and
      send the scorecard', 'audit the long-running training job at
      the 2M-games mark', 'remind the user about the deadline'.
    - project_id: optional, attaches the follow-up to a project so
      replies route there.
    - condition: optional JSON string for conditional firing. If set,
      the daemon evaluates the condition when the timer fires; if the
      condition would have the followup be unnecessary, the event is
      dropped silently (cancelled with an audit-trail reason). The
      most common pattern: 'if worker X hasn't reported by then, fire
      so I can investigate' — pass
      `{"type": "worker_not_reported", "worker_id": "abc12345"}`.
      The followup fires if the worker has had no activity since the
      schedule was created, OR if the worker reached a terminal state
      (completed/failed/killed). It's dropped if the worker is alive
      and reporting.
      Supported condition types:
      - "worker_not_reported" with "worker_id"
      Pass null/omit for unconditional firing (legacy behavior).

    Constraints:
    - fire_at must be 30+ seconds in the future and <=30 days out
    - max 50 pending scheduled events at any time

    Returns {scheduled_id, fire_at_iso, conditional}. Save the id if
    you might want to cancel — though you usually won't.

    GOOD uses:
    - 'In 20 min, check if the worker I just spawned finished and
      auto-send the result to the user'
    - Conditional: 'In 20 min, fire if worker abc12345 has gone
      silent — but if it's still actively reporting, no need'
    - 'Tomorrow at 9am UTC, audit active training runs and post a
      brief status'
    - 'In 1 hour, nudge the user about the escalation they didn't
      respond to' (but only if it's important enough to re-nag)

    BAD uses:
    - Scheduling follow-ups for things that are actually reactive.
      If there's a worker completion event to wait on, wait for it —
      don't schedule a 'check on the worker' reminder, the worker's
      own completion will ping you.
    - Spamming: don't schedule 5 reminders for the same thing.
    - Using this instead of just replying now. If you can act now,
      act now. Scheduling is for genuine future actions."""
    args: dict = {"reason": reason, "project_id": project_id or None}
    if fire_at_iso:
        args["fire_at_iso"] = fire_at_iso
    if delay_minutes:
        args["delay_minutes"] = delay_minutes
    if condition:
        args["condition"] = condition
    return await _call("schedule_followup", args)


@mcp.tool()
async def cancel_scheduled_event(schedule_id: int, reason: str = "") -> dict:
    """Cancel a pending scheduled follow-up by its id. Returns
    {cancelled: bool}. Fails silently-ish (returns False) if the event
    was already fired or already cancelled."""
    return await _call("cancel_scheduled_event", {
        "schedule_id": schedule_id, "reason": reason,
    })


@mcp.tool()
async def list_scheduled_events() -> dict:
    """List pending scheduled follow-ups (not yet fired, not cancelled).
    Returns {pending: [{id, fire_at, reason, project_id, created_at},
    ...]}. Useful before scheduling new items to avoid piling up."""
    return await _call("list_scheduled_events", {})


if __name__ == "__main__":
    mcp.run()