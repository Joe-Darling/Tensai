"""IPC dispatch for OM-side tool calls.

The OM's MCP server forwards every tool call to the daemon over its
Unix socket. ``dispatch_om_tool`` is the single entry point that routes
a tool name to its handler. Each handler is a small async function that
takes the running ``Daemon`` and the tool's ``args`` dict, and returns
the JSON-serializable result the OM sees as the tool's return value.

Adding a new OM tool: write a handler here, register it in
``OM_TOOL_HANDLERS``, and add the matching ``@mcp.tool`` definition in
``mcp_servers/om_tools.py``.
"""
from __future__ import annotations

import asyncio
import json
import logging
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Awaitable, Callable

from .bus import Event, EventKind
from .paths import (
    ensure_under_projects_root,
    is_safe_send_path,
    resolve_for_validation,
)
from .project_notes import load_project_context, save_project_note
from .rate_limits import format_rate_limits
from .scheduling import (
    MAX_PENDING_SCHEDULED_PER_SESSION,
    parse_fire_at,
)

if TYPE_CHECKING:
    from .main import Daemon

log = logging.getLogger(__name__)

Handler = Callable[["Daemon", dict], Awaitable[dict]]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- Worker lifecycle ----------

async def _spawn_worker(daemon: "Daemon", args: dict) -> dict:
    spawn_result = await daemon.workers.spawn(**args)
    wid = spawn_result["worker_id"]
    effective_engine = spawn_result["engine"]
    auto_rerouted = spawn_result.get("auto_rerouted", False)
    project_id = args.get("project_id", "?")
    task_preview = args.get("task", "")[:100]
    if len(args.get("task", "")) > 100:
        task_preview += "…"
    asyncio.create_task(daemon._post_spawn_and_thread(
        wid, project_id, task_preview, effective_engine,
    ))
    if auto_rerouted:
        # Tell the user an automatic engine swap happened so the
        # decision is visible — not silent magic.
        pressure = spawn_result.get("pressure") or {}
        pressure_types = pressure.get("types") or []
        pressure_label = ", ".join(
            f"{t.get('type')}={t.get('status')}"
            for t in pressure_types
        ) or "(unspecified)"
        msg = (f"🔀 Auto-routed worker `{wid}` to **Codex** "
               f"(Claude rate-limit pressure: {pressure_label}). "
               f"Saving Claude budget for OM.")
        if project_id and project_id != "?":
            asyncio.create_task(
                daemon.bot.system_post_to_project(project_id, msg)
            )
        else:
            asyncio.create_task(daemon.bot.system_post(msg))
    await daemon.db.log_event(
        kind="worker_spawned", project_id=project_id,
        summary=(f"worker {wid} spawned: {task_preview}"
                  + (" [auto-routed to codex]"
                     if auto_rerouted else "")),
        details={
            "worker_id": wid,
            "model": args.get("model", "sonnet"),
            "engine": effective_engine,
            "requested_engine": spawn_result.get("requested_engine"),
            "auto_rerouted": auto_rerouted,
        },
    )
    return {
        "worker_id": wid,
        "spawned_at": _now_iso(),
        "engine": effective_engine,
        "requested_engine": spawn_result.get("requested_engine"),
        "auto_rerouted_to_codex": auto_rerouted,
    }


async def _send_to_worker(daemon: "Daemon", args: dict) -> dict:
    # Codex worker session resume isn't wired up yet. The codex exec
    # resume subcommand exists but we run with --ephemeral (no session
    # rollout), so there's nothing to resume. To deliver new
    # instructions to a Codex worker, kill it and spawn a fresh one
    # referencing the partial output it produced.
    target_worker = await daemon.db.get_worker(args["worker_id"])
    if target_worker and target_worker.get("engine") == "codex":
        return {
            "delivered": False,
            "error": (f"worker {args['worker_id']} is a Codex "
                      f"worker; mid-task send_to_worker is not "
                      f"yet supported (the worker runs "
                      f"--ephemeral so session resume is "
                      f"unavailable). To redirect, kill it and "
                      f"spawn a fresh worker referencing the "
                      f"output produced so far."),
        }
    try:
        await daemon.workers.send(
            args["worker_id"],
            args["message"],
            max_turns=int(args.get("max_turns", 50)),
        )
        w = await daemon.db.get_worker(args["worker_id"])
        pid = w["project_id"] if w else None
        msg = (f"↪️ Resumed worker `{args['worker_id']}`: "
               f"{args['message'][:100]}"
               f"{'…' if len(args['message']) > 100 else ''}")
        if pid:
            asyncio.create_task(
                daemon.bot.system_post_to_project(pid, msg)
            )
        else:
            asyncio.create_task(daemon.bot.system_post(msg))
        await daemon.db.log_event(
            kind="worker_resumed", project_id=pid,
            summary=f"resumed worker {args['worker_id']}",
            details={"message": args["message"][:200]},
        )
        return {"delivered": True}
    except ValueError as e:
        return {"delivered": False, "error": str(e)}


async def _get_worker_status(daemon: "Daemon", args: dict) -> dict:
    return await daemon.workers.get_worker_status_detail(
        args["worker_id"]
    )


async def _tail_worker_output(daemon: "Daemon", args: dict) -> dict:
    return await daemon.workers.tail_output(
        args["worker_id"], n=int(args.get("n", 20)),
    )


async def _list_workers(daemon: "Daemon", args: dict) -> dict:
    return {"workers": await daemon.db.list_workers(
        project_id=args.get("project_id"), state=args.get("state"),
    )}


async def _kill_worker(daemon: "Daemon", args: dict) -> dict:
    killed = await daemon.workers.kill(args["worker_id"], args["reason"])
    if killed:
        w = await daemon.db.get_worker(args["worker_id"])
        await daemon.db.log_event(
            kind="worker_killed",
            project_id=w["project_id"] if w else None,
            summary=f"worker {args['worker_id']} killed: {args['reason']}",
        )
    return {"killed": killed}


async def _respond_to_worker_decision(daemon: "Daemon", args: dict) -> dict:
    ok = daemon.workers.resolve_decision(
        args["worker_id"], args["chosen_option"],
        args.get("reasoning", ""),
    )
    return {"resumed": ok}


# ---------- Discord-facing ----------

async def _escalate_to_pm(daemon: "Daemon", args: dict) -> dict:
    esc_id = uuid.uuid4().hex[:10]
    await daemon.db.record_escalation(
        esc_id, args["summary"], args.get("context", ""),
        args.get("options") or None, args.get("recommendation") or None,
        args.get("urgency", "fyi"),
        args.get("related_worker_id") or None,
    )
    await daemon._post_escalation(esc_id, args)
    related_worker = args.get("related_worker_id")
    project_id = None
    if related_worker:
        w = await daemon.db.get_worker(related_worker)
        if w:
            project_id = w["project_id"]
    await daemon.db.log_event(
        kind="escalation", project_id=project_id,
        summary=f"escalation {esc_id}: {args['summary'][:150]}",
        details={"urgency": args.get("urgency", "fyi")},
    )
    return {"escalation_id": esc_id}


async def _reply_to_user(daemon: "Daemon", args: dict) -> dict:
    project_id = args.get("project_id")
    if project_id:
        await daemon.bot.post_to_project(
            project_id, args["message"],
            mention=args.get("mention", False),
        )
    else:
        await daemon.bot.post(
            args["message"], mention=args.get("mention", False),
        )
    return {"posted": True}


async def _send_file_to_user(daemon: "Daemon", args: dict) -> dict:
    ok, fp, err = is_safe_send_path(
        daemon.cfg, daemon._project_root_cache, args["file_path"],
    )
    if not ok:
        return {"sent": False, "error": err}
    result = await daemon.bot.post_file_to_project(
        project_id=args.get("project_id"),
        file_path=str(fp),
        message=args.get("message", ""),
    )
    if result.get("sent"):
        await daemon.db.log_event(
            kind="file_sent", project_id=args.get("project_id"),
            summary=f"sent file {fp.name} to {daemon.cfg.user_name}",
            details={"path": str(fp),
                      "size_bytes": result.get("size_bytes")},
        )
    return result


# ---------- Project management ----------

async def _load_project_context(daemon: "Daemon", args: dict) -> dict:
    return await load_project_context(
        daemon.cfg, daemon.db,
        args["project_id"],
        include=args.get("include") or ["claude_md", "notes", "workers"],
        notes_limit=args.get("notes_limit", 20),
        notes_since=args.get("notes_since"),
        workers_limit=args.get("workers_limit", 10),
    )


async def _save_project_note(daemon: "Daemon", args: dict) -> dict:
    save_project_note(
        daemon.cfg, args["project_id"], args["note"],
        args.get("category", "observation"),
        replace_latest=args.get("replace_latest", False),
    )
    return {"saved": True}


async def _list_projects(daemon: "Daemon", args: dict) -> dict:
    return {"projects": await daemon.db.list_projects()}


async def _scaffold_project_directory(daemon: "Daemon",
                                        args: dict) -> dict:
    target, parent_resolved = resolve_for_validation(args["path"])
    ensure_under_projects_root(daemon.cfg, parent_resolved, target)
    created = not target.exists()
    target.mkdir(parents=True, exist_ok=True)
    return {"created": created, "path": str(target)}


async def _register_project(daemon: "Daemon", args: dict) -> dict:
    input_path, parent_resolved = resolve_for_validation(args["path"])
    try:
        resolved_target = input_path.resolve(strict=True)
    except FileNotFoundError:
        raise ValueError(
            f"path does not exist: {input_path}. "
            f"Create the directory first, then retry."
        )
    if not resolved_target.is_dir():
        raise ValueError(
            f"path is not a directory: {resolved_target}"
        )
    ensure_under_projects_root(daemon.cfg, parent_resolved, input_path)
    await daemon.db.register_project(
        project_id=args["project_id"],
        path=str(input_path),
        description=args.get("description", ""),
        max_concurrent_workers=args.get("max_concurrent_workers"),
    )
    channel_id = await daemon.bot.ensure_project_channel(
        args["project_id"],
        description=args.get("description", ""),
    )
    has_claude_md = (input_path / "CLAUDE.md").exists()
    if not has_claude_md:
        msg = (f"⚠️ Project `{args['project_id']}` registered but "
               f"has no CLAUDE.md. Workers spawned here will have "
               f"minimal context until one is written.")
        if channel_id:
            asyncio.create_task(daemon.bot.system_post_to_project(
                args["project_id"], msg
            ))
        else:
            asyncio.create_task(daemon.bot.system_post(msg))
    await daemon.db.log_event(
        kind="project_registered", project_id=args["project_id"],
        summary=f"registered project {args['project_id']} "
                f"at {input_path}",
    )
    return {
        "registered": True, "path": str(input_path),
        "resolved_path": str(resolved_target),
        "has_claude_md": has_claude_md,
        "status_channel_id": channel_id,
    }


async def _set_project_worker_cap(daemon: "Daemon", args: dict) -> dict:
    # max_workers can be None (clear cap → fallback to default).
    raw = args.get("max_workers")
    cap_val: int | None
    if raw is None:
        cap_val = None
    else:
        cap_val = int(raw)
        if cap_val < 1:
            raise ValueError(
                "max_workers must be at least 1, or null to "
                "clear the cap (which falls back to the global "
                "default)."
            )
    updated = await daemon.db.set_project_worker_cap(
        args["project_id"], cap_val,
    )
    if not updated:
        return {"updated": False,
                "error": f"unknown project: {args['project_id']}"}
    await daemon.db.log_event(
        kind="project_cap_changed",
        project_id=args["project_id"],
        summary=(f"set worker cap for {args['project_id']} to "
                  f"{cap_val if cap_val is not None else 'default'}"),
    )
    return {"updated": True, "project_id": args["project_id"],
            "max_workers": cap_val}


async def _archive_project(daemon: "Daemon", args: dict) -> dict:
    delete_channel = bool(args.get("delete_channel", True))
    result = await daemon.db.archive_project(args["project_id"])
    if not result.get("archived"):
        return result
    # Delete the Discord channel if requested. Best-effort — if it
    # fails we still return success on the DB-side archive (the row is
    # gone) but flag the channel error.
    channel_id = result.get("channel_id")
    channel_deleted = False
    channel_error: str | None = None
    if delete_channel and channel_id:
        try:
            ch = daemon.bot.get_channel(int(channel_id))
            if ch is not None:
                await ch.delete(reason=(
                    f"Tensai: project '{args['project_id']}' "
                    f"archived via OM tool"
                ))
                channel_deleted = True
        except Exception as e:
            channel_error = f"{type(e).__name__}: {e}"
            log.exception(
                "Failed to delete channel %s for archived "
                "project %s", channel_id, args["project_id"],
            )
    await daemon.db.log_event(
        kind="project_archived",
        project_id=args["project_id"],
        summary=(f"archived project {args['project_id']} "
                  f"({result['workers_purged']} workers, "
                  f"{result['events_purged']} events, "
                  f"{result['escalations_purged']} escalations "
                  f"purged)"),
    )
    return {
        **result,
        "channel_deleted": channel_deleted,
        "channel_error": channel_error,
    }


async def _zip_project(daemon: "Daemon", args: dict) -> dict:
    project = await daemon.db.get_project(args["project_id"])
    if not project:
        raise ValueError(f"unknown project: {args['project_id']}")
    src = Path(project["path"])
    src_resolved = src.resolve()
    if not src_resolved.is_dir():
        raise ValueError(
            f"project path is not a directory: {src_resolved}"
        )
    ts = int(datetime.now().timestamp())
    name = args.get("output_name") or f"{args['project_id']}-{ts}"
    if not name.endswith(".zip"):
        name += ".zip"
    name = "".join(c for c in name if c.isalnum() or c in "._-")
    out_path = Path(tempfile.gettempdir()) / name
    base = str(out_path.with_suffix(""))
    try:
        created = await asyncio.to_thread(
            shutil.make_archive,
            base_name=base, format="zip",
            root_dir=str(src_resolved.parent),
            base_dir=src_resolved.name,
        )
        final_path = Path(created)
        return {
            "created": True, "path": str(final_path),
            "size_bytes": final_path.stat().st_size,
        }
    except Exception as e:
        return {"created": False,
                "error": f"{type(e).__name__}: {e}"}


# ---------- Compaction & stats ----------

async def _request_compaction(daemon: "Daemon", args: dict) -> dict:
    if (daemon.om.current_session_id
            and daemon.om.current_session_id
            in daemon.om._compaction_queued_sessions):
        return {"queued": False,
                "reason": "compaction already queued for this session"}
    await daemon.bus.publish(Event(
        kind=EventKind.COMPACTION_REQUIRED,
        payload={"reason": args.get("reason", "manual")},
    ))
    daemon.om.mark_compaction_queued()
    return {"queued": True}


async def _finalize_compaction(daemon: "Daemon", args: dict) -> dict:
    sid = daemon.om.current_session_id
    if not sid:
        raise ValueError("no active session to compact")
    await daemon.db.save_compaction_summary(sid, args["summary"])
    await daemon.db.close_session(sid)
    daemon.om.current_session_id = None
    await daemon.db.log_event(
        kind="compaction", summary="OM compacted its context",
        details={"session_id": sid[:8]},
    )
    asyncio.create_task(daemon.bot.system_post(
        "🔄 Context compacted. Summary saved, next turn starts fresh."
    ))
    return {"finalized": True}


async def _get_orchestrator_stats(daemon: "Daemon", args: dict) -> dict:
    stats = await daemon.db.orchestrator_stats(
        daemon.om.current_session_id
    )
    max_tokens = daemon.cfg.max_session_tokens
    warn_tokens = daemon.cfg.compaction_warn_tokens
    tokens = stats["session_tokens_est"]
    stats["max_session_tokens"] = max_tokens
    stats["compaction_warn_tokens"] = warn_tokens
    stats["tokens_pct_of_hard"] = (
        round(100 * tokens / max_tokens, 1) if max_tokens else None
    )
    stats["tokens_pct_of_warn"] = (
        round(100 * tokens / warn_tokens, 1) if warn_tokens else None
    )
    stats["approaching_compaction"] = tokens >= warn_tokens
    stats["current_session_id"] = (
        daemon.om.current_session_id[:8]
        if daemon.om.current_session_id else None
    )
    stats["rate_limits"] = await format_rate_limits(daemon.db)
    return stats


# ---------- Scheduled follow-ups ----------

def _validate_schedule_condition(condition_raw) -> str | None:
    """Validate the optional condition arg of schedule_followup. Returns
    a JSON string to persist, or ``None`` if no condition was supplied.
    Raises ``ValueError`` for malformed input."""
    if not condition_raw:
        return None
    if isinstance(condition_raw, str):
        try:
            cond_obj = json.loads(condition_raw)
        except Exception as e:
            raise ValueError(f"condition must be valid JSON: {e}")
    else:
        cond_obj = condition_raw
    if not isinstance(cond_obj, dict) or not cond_obj.get("type"):
        raise ValueError(
            "condition must be a JSON object with a 'type' field, "
            "e.g. {\"type\": \"worker_not_reported\", "
            "\"worker_id\": \"abc12345\"}"
        )
    ctype = cond_obj["type"]
    if ctype not in ("worker_not_reported",):
        raise ValueError(
            f"unknown condition type {ctype!r}. "
            f"Supported: worker_not_reported"
        )
    if (ctype == "worker_not_reported"
            and not cond_obj.get("worker_id")):
        raise ValueError(
            "worker_not_reported condition requires a worker_id field"
        )
    return json.dumps(cond_obj)


async def _schedule_followup(daemon: "Daemon", args: dict) -> dict:
    pending_count = await daemon.db.count_pending_scheduled_events()
    if pending_count >= MAX_PENDING_SCHEDULED_PER_SESSION:
        raise ValueError(
            f"Too many pending scheduled events ({pending_count}); "
            f"cap is {MAX_PENDING_SCHEDULED_PER_SESSION}. "
            f"Cancel some with cancel_scheduled_event."
        )
    fire_at_iso = parse_fire_at(
        args.get("fire_at_iso"),
        args.get("delay_minutes"),
    )
    condition_json = _validate_schedule_condition(args.get("condition"))
    sched_id = await daemon.db.schedule_event(
        fire_at_iso=fire_at_iso,
        reason=args["reason"],
        project_id=args.get("project_id"),
        created_by_session_id=daemon.om.current_session_id,
        condition_json=condition_json,
    )
    await daemon.db.log_event(
        kind="scheduled",
        project_id=args.get("project_id"),
        summary=(f"scheduled followup #{sched_id} for "
                 f"{fire_at_iso[:16]}: {args['reason'][:150]}"),
    )
    # Let the user know so the OM's mind is a bit less opaque.
    try:
        fire_local = (datetime.fromisoformat(fire_at_iso)
                      .astimezone()
                      .strftime('%a %b %d %I:%M %p %Z'))
    except Exception:
        fire_local = fire_at_iso
    cond_note = " _(conditional)_" if condition_json else ""
    msg = (f"📅 OM scheduled a follow-up for **{fire_local}**"
           f"{cond_note} — reason: _{args['reason'][:300]}_ "
           f"(id #{sched_id})")
    if args.get("project_id"):
        asyncio.create_task(daemon.bot.system_post_to_project(
            args["project_id"], msg
        ))
    else:
        asyncio.create_task(daemon.bot.system_post(msg))
    return {
        "scheduled_id": sched_id,
        "fire_at_iso": fire_at_iso,
        "conditional": bool(condition_json),
    }


async def _cancel_scheduled_event(daemon: "Daemon", args: dict) -> dict:
    cancelled = await daemon.db.cancel_scheduled_event(
        int(args["schedule_id"]),
        args.get("reason", "no reason given"),
    )
    return {"cancelled": cancelled}


async def _list_scheduled_events(daemon: "Daemon", args: dict) -> dict:
    return {"pending": await daemon.db.pending_scheduled_events()}


# ---------- Registry ----------

OM_TOOL_HANDLERS: dict[str, Handler] = {
    "spawn_worker": _spawn_worker,
    "send_to_worker": _send_to_worker,
    "get_worker_status": _get_worker_status,
    "tail_worker_output": _tail_worker_output,
    "list_workers": _list_workers,
    "kill_worker": _kill_worker,
    "respond_to_worker_decision": _respond_to_worker_decision,
    "escalate_to_pm": _escalate_to_pm,
    "load_project_context": _load_project_context,
    "save_project_note": _save_project_note,
    "list_projects": _list_projects,
    "scaffold_project_directory": _scaffold_project_directory,
    "register_project": _register_project,
    "set_project_worker_cap": _set_project_worker_cap,
    "archive_project": _archive_project,
    "reply_to_user": _reply_to_user,
    "send_file_to_user": _send_file_to_user,
    "zip_project": _zip_project,
    "request_compaction": _request_compaction,
    "finalize_compaction": _finalize_compaction,
    "get_orchestrator_stats": _get_orchestrator_stats,
    "schedule_followup": _schedule_followup,
    "cancel_scheduled_event": _cancel_scheduled_event,
    "list_scheduled_events": _list_scheduled_events,
}


async def dispatch_om_tool(daemon: "Daemon", tool: str,
                            args: dict) -> dict:
    handler = OM_TOOL_HANDLERS.get(tool)
    if handler is None:
        raise ValueError(f"unknown OM tool: {tool}")
    return await handler(daemon, args)
