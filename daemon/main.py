"""Entry point: wire up Discord bot, IPC server, OM runner, worker runner.

Run with: ``python -m daemon.main``

The :class:`Daemon` is a thin coordinator. Heavy lifting lives in
focused modules:

- :mod:`daemon.dispatch_om` / :mod:`daemon.dispatch_worker` — IPC tool
  handlers, one async function per tool.
- :mod:`daemon.paths` — path validation for register/scaffold/send_file.
- :mod:`daemon.scheduling` — scheduled-event firing time + condition eval.
- :mod:`daemon.project_notes` — project context loading + note writing.
- :mod:`daemon.rate_limits` — formatting rate-limit observations.
- :mod:`daemon.om_runner` / :mod:`daemon.worker_runner` — subprocess
  lifecycle for the OM and workers.
"""
from __future__ import annotations

import asyncio
import logging
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiosqlite
import yaml

from .bus import Event, EventBus, EventKind, IPCServer
from .config import DaemonConfig, load_config
from .db import DB
from .discord_bot import BotsManager
from .dispatch_om import dispatch_om_tool
from .dispatch_worker import dispatch_worker_tool
from .om_runner import OMRunner
from .paths import ProjectRootCache
from .scheduling import eval_schedule_condition
from .worker_runner import WorkerRunner

# Worker purge: drop terminal workers (completed/failed/killed) older
# than this. Active workers (running/paused/exited_no_summary) are
# never purged. Runs at most once per interval.
WORKER_PURGE_INACTIVE_HOURS = 24
WORKER_PURGE_INTERVAL_S = 60 * 60  # once an hour

# Tool calls that result in the user seeing something (either directly,
# via a daemon-generated ack post, or via a queued event). If none of
# these fired during a FROM_USER turn, the user got nothing — and we
# fire a REPLY_DROPPED_CHECK nudge.
REACHED_USER_TOOLS = {
    "mcp__orchestrator-om__reply_to_user",
    "mcp__orchestrator-om__send_file_to_user",
    "mcp__orchestrator-om__escalate_to_pm",
    "mcp__orchestrator-om__spawn_worker",       # daemon auto-posts ack
    "mcp__orchestrator-om__send_to_worker",     # daemon auto-posts ack
    "mcp__orchestrator-om__register_project",   # daemon auto-creates ch
    "mcp__orchestrator-om__finalize_compaction",  # daemon auto-posts
    "mcp__orchestrator-om__request_compaction",   # queues compaction
    "mcp__orchestrator-om__schedule_followup",    # daemon auto-posts
}


class Daemon:
    def __init__(self, cfg: DaemonConfig):
        self.cfg = cfg
        self.db = DB(cfg.workspace / "state.db")
        self.bus = EventBus()
        self.bot = BotsManager(cfg, self.bus, self.db)
        self.om = OMRunner(cfg, self.db, self.bot)
        self.workers = WorkerRunner(cfg, self.db, self.bus, self.bot)
        self.ipc = IPCServer(cfg.ipc_socket, self.bus, self._dispatch_tool)
        # Skills dir for reusable task templates the OM can load.
        self.skills_dir = cfg.workspace / "skills"
        self.skills_dir.mkdir(parents=True, exist_ok=True)
        # Cache of resolved project roots — used by send_file path
        # validation. Rebuilds opportunistically.
        self._project_root_cache = ProjectRootCache(self.db.path)
        # Tracks last worker-purge time. Worker purge runs at most once
        # per WORKER_PURGE_INTERVAL_S inside the periodic watcher.
        self._last_worker_purge_ts: float = 0.0

    async def start(self) -> None:
        await self.db.init()
        # Reconcile orphan workers from any prior daemon run. If the
        # daemon was restarted while workers were running, their
        # in-memory subprocess handles / monitor tasks / session ids
        # are gone — there's no way to recover them, so flip those DB
        # rows to 'killed' so they don't tie up state=running slots
        # forever and confuse OM.
        orphans = await self.db.reconcile_orphan_workers()
        if orphans:
            logging.warning(
                "Reconciled %d orphan worker(s) from prior daemon run "
                "(marked killed)", orphans,
            )
        await self._load_projects_from_yaml()
        await self._restore_om_session()
        await self.om.setup()
        await self.ipc.start()
        asyncio.create_task(self._consume_events())
        asyncio.create_task(self._periodic_watcher())
        await self.bot.start()

    async def _load_projects_from_yaml(self) -> None:
        projects_dir = self.cfg.workspace / "projects"
        if not projects_dir.exists():
            return
        for yml in projects_dir.glob("*.yaml"):
            data = yaml.safe_load(yml.read_text())
            if not data or not data.get("project_id"):
                continue
            await self.db.register_project(
                project_id=data["project_id"],
                path=data["path"],
                description=data.get("description", ""),
            )

    async def _restore_om_session(self) -> None:
        sid = await self.db.get_active_om_session()
        if sid:
            self.om.current_session_id = sid
            logging.info("Restored OM session %s from DB", sid[:8])
        else:
            logging.info("No active OM session to restore; starting fresh")

    # ---------- Event loop ----------

    async def _consume_events(self) -> None:
        while True:
            event = await self.bus.consume()
            try:
                await self._handle_one_event(event)
            except Exception as e:
                logging.exception("event processing failed")
                await self.bot.system_post(f"⚠️ daemon error: {e}")

    async def _handle_one_event(self, event: Event) -> None:
        # Drop stale COMPACTION_REQUIRED events: if no active session
        # needs compacting (because a prior compaction already
        # finalized or force-closed), this event is stale —
        # processing it would compact a fresh session that just opened,
        # with nothing to summarize.
        if event.kind is EventKind.COMPACTION_REQUIRED:
            if not self.om.current_session_id:
                logging.info(
                    "Dropping stale COMPACTION_REQUIRED — "
                    "no active session to compact"
                )
                return
        if event.kind is EventKind.DISCORD_MESSAGE:
            p = event.payload
            text_preview = p.get("text", "")[:200]
            await self.db.log_event(
                kind="discord_message",
                project_id=p.get("project_context"),
                summary=f"{self.cfg.user_name}: {text_preview}",
                details={"text": p.get("text", "")[:500]},
            )
        await self.om.process_event(event)

        # Reply-dropped nudge: if the user sent a message and OM's
        # turn ended without any user-facing tool call, they got
        # nothing. Synthesize a nudge so OM can recover. Only fires
        # for DISCORD_MESSAGE events (not nudges themselves — that
        # prevents cascades).
        if (event.kind is EventKind.DISCORD_MESSAGE
                and self.om.last_turn_succeeded
                and not any(t in REACHED_USER_TOOLS
                             for t in self.om.last_turn_tool_calls)):
            p = event.payload
            logging.info(
                "REPLY_DROPPED_CHECK: FROM_USER turn ended without "
                "user-facing tool. tools=%s",
                self.om.last_turn_tool_calls,
            )
            await self.bus.publish(Event(
                kind=EventKind.REPLY_DROPPED_CHECK,
                payload={
                    "original_text": p.get("text", ""),
                    "original_project_context": p.get("project_context"),
                    "original_channel_id": p.get("channel_id"),
                    "tool_calls": list(self.om.last_turn_tool_calls),
                },
            ))

        if await self.om.should_compact():
            await self.bus.publish(Event(
                kind=EventKind.COMPACTION_REQUIRED, payload={}
            ))
            self.om.mark_compaction_queued()
            logging.info(
                "Queued COMPACTION_REQUIRED for session %s",
                (self.om.current_session_id or "?")[:8],
            )
        elif await self.om.should_warn_compaction():
            tokens = await self.db.current_session_tokens(
                self.om.current_session_id or ""
            )
            logging.info(
                "Firing COMPACTION_IMMINENT: %d/%d tokens",
                tokens, self.cfg.max_session_tokens,
            )
            await self.bus.publish(Event(
                kind=EventKind.COMPACTION_IMMINENT,
                payload={
                    "tokens": tokens,
                    "max_tokens": self.cfg.max_session_tokens,
                    "warn_tokens": self.cfg.compaction_warn_tokens,
                },
            ))

    async def _periodic_watcher(self) -> None:
        while True:
            try:
                await asyncio.sleep(60)
                await self._periodic_tick()
            except Exception:
                logging.error(
                    "periodic_watcher iteration failed:\n%s",
                    traceback.format_exc(),
                )

    async def _periodic_tick(self) -> None:
        await self.workers.sweep_stuck_workers(self.cfg.stuck_timeout_s)
        # Periodic purge of old terminal workers (and their events).
        # Bounds DB growth — without this, the workers and
        # worker_events tables grow forever.
        now_ts = time.monotonic()
        if (now_ts - self._last_worker_purge_ts
                >= WORKER_PURGE_INTERVAL_S):
            self._last_worker_purge_ts = now_ts
            purged = await self.db.purge_old_workers(
                WORKER_PURGE_INACTIVE_HOURS
            )
            if purged.get("workers_purged", 0) > 0:
                logging.info(
                    "Purged %d old workers + %d events (>%dh idle)",
                    purged["workers_purged"],
                    purged["events_purged"],
                    WORKER_PURGE_INACTIVE_HOURS,
                )
        # Mid-flight quiet-detection: surface workers that haven't
        # talked to the daemon in a while so OM can check up on them
        # BEFORE the 30-min stuck timeout.
        quiet_workers = await self.workers.sweep_quiet_workers(
            self.cfg.worker_quiet_threshold_s
        )
        for q in quiet_workers:
            await self.bus.publish(Event(
                kind=EventKind.WORKER_QUIET, payload=q,
            ))
            await self.db.log_event(
                kind="worker_quiet",
                project_id=q.get("project_id"),
                summary=(f"worker {q['worker_id']} quiet for "
                         f"{q['quiet_for_seconds']}s "
                         f"(subprocess_alive="
                         f"{q['subprocess_alive']})"),
                details={
                    "last_tools": q.get("last_tools"),
                    "turn_count": q.get("turn_count"),
                },
            )
        await self._fire_due_scheduled_events()
        await self._maybe_age_compact_session()

    async def _maybe_age_compact_session(self) -> None:
        if not self.om.current_session_id:
            return
        async with aiosqlite.connect(self.db.path) as db:
            async with db.execute(
                "SELECT started_at FROM om_sessions WHERE session_id=?",
                (self.om.current_session_id,),
            ) as cur:
                row = await cur.fetchone()
        if not row:
            return
        started = datetime.fromisoformat(row[0])
        age = datetime.now(timezone.utc) - started
        if (age > timedelta(hours=self.cfg.max_session_hours)
                and self.om.current_session_id
                not in self.om._compaction_queued_sessions):
            logging.info(
                "Session %s is %s old, queueing compaction",
                self.om.current_session_id[:8], age,
            )
            await self.bus.publish(Event(
                kind=EventKind.COMPACTION_REQUIRED,
                payload={"reason": f"session aged {age}"},
            ))
            self.om.mark_compaction_queued()

    async def _fire_due_scheduled_events(self) -> None:
        import json as _json
        due = await self.db.due_scheduled_events()
        for item in due:
            sched_id = int(item["id"])
            condition_raw = item.get("condition_json")
            # If a condition is set, evaluate it. If the condition is
            # met (i.e., we should NOT fire — the thing we were
            # waiting on already happened), drop the event silently.
            # Audit-trail it via cancel rather than fire.
            if condition_raw:
                try:
                    condition = _json.loads(condition_raw)
                except Exception:
                    logging.exception(
                        "Bad condition_json on scheduled event %s; "
                        "firing anyway", sched_id,
                    )
                    condition = None
                if condition:
                    should_fire, drop_reason = await eval_schedule_condition(
                        self.db, condition, item,
                    )
                    if not should_fire:
                        logging.info(
                            "Scheduled followup %s dropped (condition "
                            "not met): %s", sched_id, drop_reason,
                        )
                        await self.db.cancel_scheduled_event(
                            sched_id,
                            f"condition not met: {drop_reason}",
                        )
                        continue
            logging.info("Firing scheduled_followup id=%s reason=%r",
                         sched_id, item.get("reason", "")[:80])
            await self.db.mark_scheduled_event_fired(sched_id)
            await self.db.log_event(
                kind="scheduled_fired",
                project_id=item.get("project_id"),
                summary=(f"scheduled followup #{sched_id} fired: "
                         f"{item.get('reason', '')[:150]}"),
            )
            await self.bus.publish(Event(
                kind=EventKind.SCHEDULED_FOLLOWUP,
                payload={
                    "id": sched_id,
                    "reason": item.get("reason", ""),
                    "project_id": item.get("project_id"),
                    "fire_at": item.get("fire_at"),
                    "created_at": item.get("created_at"),
                },
            ))

    # ---------- IPC dispatch ----------

    async def _dispatch_tool(self, tool: str, args: dict,
                              context: dict) -> dict:
        role = context.get("role")
        if role == "om":
            return await dispatch_om_tool(self, tool, args)
        if role == "worker":
            return await dispatch_worker_tool(
                self, tool, args, context["worker_id"]
            )
        raise ValueError(f"unknown role: {role}")

    # ---------- Side-effect helpers used by handlers ----------

    async def _post_spawn_and_thread(self, worker_id: str, project_id: str,
                                      task_preview: str,
                                      engine: str = "claude") -> None:
        try:
            engine_emoji = "🤖" if engine == "codex" else "⚙️"
            engine_tag = " *(codex)*" if engine == "codex" else ""
            msg = await self.bot.system_post_to_project(
                project_id,
                f"{engine_emoji} Worker `{worker_id}`{engine_tag} "
                f"spawned: {task_preview}"
            )
            if msg is None:
                return
            thread_id = await self.bot.create_worker_thread(msg, worker_id)
            if thread_id:
                await self.db.set_worker_thread(worker_id, thread_id)
        except Exception:
            logging.exception("post_spawn_and_thread failed for %s",
                                worker_id)

    async def _post_escalation(self, esc_id: str, args: dict) -> None:
        lines = [
            f"🔔 **Escalation `{esc_id}`** ({args.get('urgency', 'fyi')})",
            args["summary"],
        ]
        if ctx := args.get("context"):
            lines.append(f"\n{ctx}")
        options = args.get("options") or []
        if options:
            lines.append("\nOptions (tap a reaction to choose):")
            for i, o in enumerate(options, 1):
                lines.append(
                    f"  {i}. **{o.get('label')}** — {o.get('consequences', '')}"
                )
        if rec := args.get("recommendation"):
            lines.append(f"\n_OM recommends:_ {rec}")
        related_worker = args.get("related_worker_id")
        project_id = None
        if related_worker:
            w = await self.db.get_worker(related_worker)
            if w:
                project_id = w["project_id"]
        text = "\n".join(lines)
        if project_id:
            posted_msg = await self.bot.post_to_project(project_id, text)
        else:
            posted_msg = await self.bot.post(text)
        # Save message ID so reactions can resolve it, then seed option
        # reactions.
        if posted_msg and options:
            try:
                await self.db.set_escalation_message(
                    esc_id, int(posted_msg.id),
                    int(posted_msg.channel.id),
                )
                asyncio.create_task(self.bot.add_option_reactions(
                    posted_msg, len(options)
                ))
            except Exception:
                logging.exception(
                    "Failed to seed reactions for escalation %s",
                    esc_id,
                )


def main() -> None:
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cfg = load_config()
    d = Daemon(cfg)
    try:
        asyncio.run(d.start())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
