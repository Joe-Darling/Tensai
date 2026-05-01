"""Human-readable rendering of bus events for the OM's prompt.

The OM is a stateless-per-event subprocess. Every turn begins with a
formatted preamble describing the event that triggered the turn —
``[FROM_USER]``, ``[FROM_WORKER ...]``, ``[COMPACTION_REQUIRED]``, etc.
``format_event`` is what produces those preambles.

``format_events_for_seed`` formats a chunk of the durable event log
that's prepended to a fresh session after compaction so the OM has a
shot at reconstructing context the compaction summary missed.

The user's configured name is woven into preambles where the OM is
addressing or referring to them, so the orchestrator's prose stays
natural. Everything else is pure functions of the event payloads —
keeping them out of :class:`daemon.om_runner.OMRunner` makes them
trivial to test and reason about in isolation.
"""
from __future__ import annotations

import json

from .bus import Event, EventKind


def format_event(event: Event, user_name: str) -> str:
    kind = event.kind
    p = event.payload
    if kind is EventKind.DISCORD_MESSAGE:
        return _format_discord_message(p, user_name)
    if kind is EventKind.WORKER_TOOL_CALL:
        return (f"[FROM_WORKER {p['worker_id']}] kind={p['kind']}\n"
                f"{json.dumps(p['payload'], indent=2)}")
    if kind is EventKind.TIMER:
        return f"[TIMER] {p.get('detail', '')}"
    if kind is EventKind.COMPACTION_REQUIRED:
        return ("[COMPACTION_REQUIRED] Produce the compaction summary per "
                "the format in your CLAUDE.md. After this turn a new "
                "session will begin seeded with your output.")
    if kind is EventKind.COMPACTION_IMMINENT:
        return _format_compaction_imminent(p, user_name)
    if kind is EventKind.SCHEDULED_FOLLOWUP:
        return _format_scheduled_followup(p)
    if kind is EventKind.REPLY_DROPPED_CHECK:
        return _format_reply_dropped(p, user_name)
    if kind is EventKind.WORKER_QUIET:
        return _format_worker_quiet(p)
    return f"[UNKNOWN_EVENT] {p!r}"


def _format_discord_message(p: dict, user_name: str) -> str:
    lines = []
    if p.get("project_context"):
        lines.append(f"[CONTEXT: project={p['project_context']}]")
    if p.get("reply_to_content"):
        author = p.get("reply_to_author", "someone")
        lines.append(
            f"[REPLY_TO: {user_name}'s message is a reply to an earlier "
            f"message from {author}. Quoted content below — use it as "
            f"context even if you don't remember the original.]"
        )
        lines.append("```")
        lines.append(p["reply_to_content"])
        lines.append("```")
    attachments = p.get("attachments", [])
    attachment_types = p.get("attachment_types", [])
    if attachments:
        lines.append(f"[ATTACHMENTS from {user_name}:]")
        for path, kind_ in zip(attachments, attachment_types):
            if kind_ == "image":
                lines.append(
                    f"- IMAGE: {path}  (call Read on this path to "
                    f"see the image)"
                )
            elif kind_ == "text":
                lines.append(
                    f"- TEXT FILE: {path}  (call Read on this path to "
                    f"see the content)"
                )
            else:
                lines.append(
                    f"- FILE: {path}  (binary; Read may or may not work)"
                )
    lines.append(f"[FROM_USER] {p['text']}")
    return "\n".join(lines)


def _format_compaction_imminent(p: dict, user_name: str) -> str:
    tokens = p.get("tokens", 0)
    max_tokens = p.get("max_tokens", 0)
    pct = int(100 * tokens / max_tokens) if max_tokens else 0
    return (
        f"[COMPACTION_IMMINENT] Your session is at ~{pct}% of the "
        f"compaction threshold ({tokens:,} of {max_tokens:,} tokens). "
        f"Compaction will force a fresh session when you cross the "
        f"hard threshold, and your summary will be your only carry-"
        f"forward memory (plus the event log).\n\n"
        f"This is a chance — NOT an obligation — to review what's "
        f"in context right now and write durable notes for anything "
        f"that would be lost. Things worth checking:\n"
        f"  - Ongoing work that hasn't been written to project notes\n"
        f"  - Active workers: check their status and note current state\n"
        f"  - Decisions you made this session that aren't in notes\n"
        f"  - Anything {user_name} is waiting on\n\n"
        f"Use save_project_note (with replace_latest=True for "
        f"status-type entries). You can call get_orchestrator_stats "
        f"or list_workers to see what's live.\n\n"
        f"If there's nothing worth preserving beyond what's already "
        f"in notes, reply_to_user with a brief status and do nothing "
        f"else. Don't compact now — you'll be prompted when it's "
        f"actually time. This is a one-shot warning per session; "
        f"you won't get another until after compaction resets."
    )


def _format_scheduled_followup(p: dict) -> str:
    sid = p.get("id")
    reason = p.get("reason", "(no reason recorded)")
    created_at = p.get("created_at", "")
    fire_at = p.get("fire_at", "")
    proj = p.get("project_id")
    proj_line = f"[CONTEXT: project={proj}]\n" if proj else ""
    return (
        f"{proj_line}[SCHEDULED_FOLLOWUP] Earlier (at "
        f"{created_at[:16].replace('T', ' ')} UTC) you scheduled this "
        f"follow-up via schedule_followup(). Scheduled-for: "
        f"{fire_at[:16].replace('T', ' ')} UTC. Reason you gave:\n\n"
        f"  \"{reason}\"\n\n"
        f"(schedule id: {sid})\n\n"
        f"Act on this now. If circumstances have changed and the "
        f"follow-up is no longer needed, just reply_to_user briefly "
        f"(or do nothing if truly inapplicable). Don't worry if the "
        f"original context is fuzzy — notes and the event log are "
        f"there if you need to ground yourself."
    )


def _format_reply_dropped(p: dict, user_name: str) -> str:
    orig_text = p.get("original_text", "")[:500]
    tools_called = p.get("tool_calls", [])
    tools_str = ", ".join(tools_called) if tools_called else "(none)"
    proj = p.get("original_project_context")
    proj_line = f"[CONTEXT: project={proj}]\n" if proj else ""
    return (
        f"{proj_line}[REPLY_DROPPED?] Your last turn responded to a "
        f"message from {user_name} but ended without calling any "
        f"user-facing tool (reply_to_user, send_file_to_user, "
        f"escalate_to_pm, spawn_worker, etc.). Tools you did call: "
        f"{tools_str}\n\n"
        f"{user_name}'s original message was:\n  \"{orig_text}\"\n\n"
        f"If you prepared an answer in assistant text, {user_name} "
        f"DID NOT see it — that text stays inside your subprocess "
        f"and never reaches them. Call reply_to_user now with the "
        f"answer (use project_id={proj!r} if relevant).\n\n"
        f"If you stayed silent deliberately, end this turn with no "
        f"tool calls. This nudge will not fire twice."
    )


def _format_worker_quiet(p: dict) -> str:
    wid = p.get("worker_id", "?")
    proj = p.get("project_id")
    proj_line = f"[CONTEXT: project={proj}]\n" if proj else ""
    quiet_min = int(p.get("quiet_for_seconds", 0)) // 60
    last_tools = p.get("last_tools") or []
    tools_str = ", ".join(last_tools) if last_tools else "(none recorded)"
    alive = p.get("subprocess_alive")
    alive_str = ("subprocess alive (probably busy with internal work)"
                 if alive else "subprocess EXITED but state was still "
                                "running — likely silent death; "
                                "check immediately")
    task_preview = p.get("task_preview", "")[:200]
    turns = p.get("turn_count", 0)
    budget = p.get("turn_budget", 0)
    return (
        f"{proj_line}[WORKER_QUIET] Worker `{wid}` hasn't called "
        f"any orchestrator tool in ~{quiet_min} minutes. State: "
        f"{alive_str}.\n\n"
        f"Task (preview): {task_preview}\n"
        f"Turns so far: {turns}/{budget}\n"
        f"Last 5 tool calls: {tools_str}\n\n"
        f"This nudge fires ONCE per worker per run — won't repeat. "
        f"Decide what to do:\n"
        f"  - Likely fine (long bash, big build): do nothing, the "
        f"worker will report when it surfaces.\n"
        f"  - Looks stuck: call get_worker_status for more detail, "
        f"or kill_worker if you want to abandon.\n"
        f"  - Silent death (subprocess_alive=False): the daemon "
        f"will normally launch a continuation flow on its own; "
        f"check back via get_worker_status in 30s if you don't see "
        f"a kind=paused or kind=completion event soon.\n\n"
        f"If you choose to do nothing, end your turn without calls."
    )


def format_events_for_seed(events: list[dict],
                            max_chars: int = 4000) -> str:
    """Render the durable event log for the post-compaction seed.

    Truncated from the head if it exceeds ``max_chars`` so the most
    recent events survive — those are the ones the OM most likely
    cares about."""
    if not events:
        return ""
    lines = []
    for e in events:
        ts = e["timestamp"][:16].replace("T", " ")
        proj = f"[{e['project_id']}] " if e.get("project_id") else ""
        lines.append(f"- {ts}  {proj}{e['summary']}")
    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[-max_chars:]
        newline = text.find("\n")
        if newline > -1:
            text = text[newline + 1:]
        text = "(older events truncated)\n" + text
    return text
