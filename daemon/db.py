"""SQLite state."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  path TEXT NOT NULL,
  description TEXT,
  registered_at TEXT NOT NULL,
  last_active_at TEXT,
  status_channel_id INTEGER,
  max_concurrent_workers INTEGER
);

CREATE TABLE IF NOT EXISTS workers (
  worker_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  task TEXT NOT NULL,
  model TEXT,
  state TEXT NOT NULL,
  spawned_at TEXT NOT NULL,
  ended_at TEXT,
  pid INTEGER,
  session_id TEXT,
  last_activity_at TEXT,
  summary TEXT,
  thread_id INTEGER,
  last_heartbeat_at TEXT,
  engine TEXT NOT NULL DEFAULT 'claude'
);
CREATE INDEX IF NOT EXISTS idx_workers_state ON workers(state);
CREATE INDEX IF NOT EXISTS idx_workers_project ON workers(project_id);

CREATE TABLE IF NOT EXISTS worker_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id TEXT REFERENCES workers(worker_id),
  kind TEXT NOT NULL,
  severity TEXT,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_worker ON worker_events(worker_id);

CREATE TABLE IF NOT EXISTS escalations (
  escalation_id TEXT PRIMARY KEY,
  summary TEXT NOT NULL,
  context TEXT,
  options TEXT,
  recommendation TEXT,
  urgency TEXT,
  related_worker_id TEXT,
  created_at TEXT NOT NULL,
  resolved_at TEXT,
  resolution TEXT,
  message_id INTEGER,
  channel_id INTEGER
);
CREATE INDEX IF NOT EXISTS idx_escalations_unresolved
  ON escalations(resolved_at) WHERE resolved_at IS NULL;
-- idx_escalations_msgid is created in the migrations block below, AFTER
-- the ALTER TABLE that adds message_id (existing DBs won't have the
-- column yet at SCHEMA execution time).

CREATE TABLE IF NOT EXISTS om_turns (
  turn_id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  trigger_event TEXT NOT NULL,
  trigger_detail TEXT,
  input_tokens INTEGER,
  output_tokens INTEGER,
  cost_estimate REAL,
  tool_calls TEXT,
  started_at TEXT NOT NULL,
  ended_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_turns_session ON om_turns(session_id);

CREATE TABLE IF NOT EXISTS om_sessions (
  session_id TEXT PRIMARY KEY,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  succeeded_by TEXT,
  turn_count INTEGER DEFAULT 0,
  estimated_tokens INTEGER DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'active',
  compaction_summary TEXT
);

CREATE TABLE IF NOT EXISTS event_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp TEXT NOT NULL,
  kind TEXT NOT NULL,
  project_id TEXT,
  summary TEXT NOT NULL,
  details TEXT
);
CREATE INDEX IF NOT EXISTS idx_event_log_ts ON event_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_event_log_proj ON event_log(project_id);

CREATE TABLE IF NOT EXISTS rate_limits (
  rate_limit_type TEXT PRIMARY KEY,
  status TEXT,
  resets_at INTEGER,
  overage_status TEXT,
  is_using_overage INTEGER DEFAULT 0,
  observed_at TEXT NOT NULL,
  observed_by TEXT
);

-- OM-scheduled follow-up events. `fire_at` is UTC ISO; the periodic
-- watcher fires any rows past this time whose `fired_at` is still NULL.
CREATE TABLE IF NOT EXISTS scheduled_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fire_at TEXT NOT NULL,
  reason TEXT NOT NULL,
  project_id TEXT,
  created_at TEXT NOT NULL,
  created_by_session_id TEXT,
  fired_at TEXT,
  cancelled_at TEXT,
  cancel_reason TEXT,
  condition_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_scheduled_pending
  ON scheduled_events(fire_at) WHERE fired_at IS NULL AND cancelled_at IS NULL;
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _start_of_day_utc() -> str:
    now = datetime.now(timezone.utc)
    sod = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return sod.isoformat()


class DB:
    def __init__(self, path: Path):
        self.path = path

    def _connect(self):
        """Open a connection with a sensible busy timeout. SQLite's
        default behavior under contention is to raise SQLITE_BUSY
        immediately; with the daemon's pattern of many short-lived
        connections from concurrent tasks (OM streaming writes,
        worker spawn, watcher sweep, fire-and-forget rate-limit
        updates), occasional locks would surface as 'database is
        locked' OperationalErrors. Setting busy_timeout makes
        SQLite poll-and-retry for up to 5s before giving up, which
        is enough for any realistic contention to clear while
        still failing fast on genuine deadlocks.
        """
        # aiosqlite.connect supports a `timeout` kwarg which sets the
        # busy_timeout at connection time — cleaner than a follow-up
        # PRAGMA because it applies before the first execute.
        return aiosqlite.connect(self.path, timeout=5.0)

    async def init(self) -> None:
        async with self._connect() as db:
            await db.execute("PRAGMA journal_mode=WAL")
            # WAL persists at the database level, but busy_timeout is
            # per-connection. We set it via the timeout kwarg above
            # for every connection. This PRAGMA here is belt-and-
            # suspenders for older aiosqlite versions where the kwarg
            # behavior may differ.
            await db.execute("PRAGMA busy_timeout=5000")
            await db.executescript(SCHEMA)
            for migration in [
                "ALTER TABLE om_sessions ADD COLUMN state TEXT NOT NULL DEFAULT 'active'",
                "ALTER TABLE om_sessions ADD COLUMN compaction_summary TEXT",
                "ALTER TABLE projects ADD COLUMN status_channel_id INTEGER",
                "ALTER TABLE workers ADD COLUMN thread_id INTEGER",
                "ALTER TABLE workers ADD COLUMN last_heartbeat_at TEXT",
                "ALTER TABLE escalations ADD COLUMN message_id INTEGER",
                "ALTER TABLE escalations ADD COLUMN channel_id INTEGER",
                "ALTER TABLE workers ADD COLUMN continuations_count INTEGER DEFAULT 0",
                "ALTER TABLE projects ADD COLUMN max_concurrent_workers INTEGER",
                "ALTER TABLE scheduled_events ADD COLUMN condition_json TEXT",
                "ALTER TABLE workers ADD COLUMN engine TEXT NOT NULL "
                "DEFAULT 'claude'",
                # Index on message_id, created here (post-ALTER) so it
                # works on both fresh and existing databases.
                "CREATE INDEX IF NOT EXISTS idx_escalations_msgid "
                "ON escalations(message_id)",
            ]:
                try:
                    await db.execute(migration)
                except Exception:
                    pass
            await db.commit()

    # --- projects ---
    async def register_project(self, project_id: str, path: str,
                                description: str,
                                status_channel_id: int | None = None,
                                max_concurrent_workers: int | None = None) -> None:
        async with self._connect() as db:
            # Preserve fields that already exist if the caller passed
            # None (re-registering an existing project shouldn't blow
            # away its channel or its worker cap).
            existing_channel: int | None = None
            existing_cap: int | None = None
            async with db.execute(
                "SELECT status_channel_id, max_concurrent_workers "
                "FROM projects WHERE project_id=?",
                (project_id,),
            ) as cur:
                row = await cur.fetchone()
                if row:
                    existing_channel = row[0]
                    existing_cap = row[1]
            if status_channel_id is None:
                status_channel_id = existing_channel
            if max_concurrent_workers is None:
                max_concurrent_workers = existing_cap
            await db.execute(
                "INSERT OR REPLACE INTO projects "
                "(project_id, path, description, registered_at, "
                "status_channel_id, max_concurrent_workers) "
                "VALUES (?,?,?,?,?,?)",
                (project_id, path, description, utcnow(),
                 status_channel_id, max_concurrent_workers),
            )
            await db.commit()

    async def set_project_worker_cap(self, project_id: str,
                                       max_workers: int | None) -> bool:
        """Set per-project max concurrent workers. None = use global
        default. Returns True if updated, False if project not found."""
        async with self._connect() as db:
            cur = await db.execute(
                "UPDATE projects SET max_concurrent_workers=? "
                "WHERE project_id=?",
                (max_workers, project_id),
            )
            await db.commit()
            return cur.rowcount > 0

    async def count_running_workers_for_project(self, project_id: str) -> int:
        async with self._connect() as db:
            async with db.execute(
                "SELECT COUNT(*) FROM workers "
                "WHERE project_id=? AND state='running'",
                (project_id,),
            ) as cur:
                row = await cur.fetchone()
                return int(row[0]) if row else 0

    async def set_project_channel(self, project_id: str,
                                    channel_id: int) -> None:
        async with self._connect() as db:
            await db.execute(
                "UPDATE projects SET status_channel_id=? WHERE project_id=?",
                (channel_id, project_id),
            )
            await db.commit()

    async def get_project(self, project_id: str) -> dict[str, Any] | None:
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM projects WHERE project_id=?", (project_id,)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def get_project_by_channel(self, channel_id: int) -> dict[str, Any] | None:
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM projects WHERE status_channel_id=?",
                (channel_id,),
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def list_projects(self) -> list[dict[str, Any]]:
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM projects ORDER BY project_id") as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def archive_project(self, project_id: str) -> dict:
        """Remove project from the DB along with its workers, worker
        events, escalations, and pending scheduled events. Refuses if
        any worker is still in 'running' state — caller must kill them
        first. Filesystem under the project's path is left alone;
        re-registering with the same path restores it.

        Returns counts of what was cleaned up so the caller can
        report meaningfully."""
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            # Refuse if anything is running.
            async with db.execute(
                "SELECT worker_id FROM workers "
                "WHERE project_id=? AND state='running'",
                (project_id,),
            ) as cur:
                running = [r["worker_id"] for r in await cur.fetchall()]
            if running:
                return {
                    "archived": False,
                    "error": (f"project '{project_id}' has "
                              f"{len(running)} running worker(s): "
                              f"{', '.join(running[:5])}. Kill them "
                              f"first via kill_worker, then retry."),
                    "running_workers": running,
                }
            # Capture the channel ID before deleting so the caller can
            # delete the Discord channel afterwards.
            async with db.execute(
                "SELECT status_channel_id FROM projects WHERE project_id=?",
                (project_id,),
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return {"archived": False,
                            "error": f"unknown project: {project_id}"}
                channel_id = row["status_channel_id"]
            # Cascade delete in dependency order. SQLite's REFERENCES
            # without ON DELETE CASCADE means we have to do this
            # manually; that's fine because we want explicit control.
            # Get worker IDs first so we can clean events + escalations.
            async with db.execute(
                "SELECT worker_id FROM workers WHERE project_id=?",
                (project_id,),
            ) as cur:
                worker_ids = [r["worker_id"] for r in await cur.fetchall()]
            events_purged = 0
            escalations_purged = 0
            workers_purged = 0
            for i in range(0, len(worker_ids), 500):
                batch = worker_ids[i:i + 500]
                ph = ",".join(["?"] * len(batch))
                cur = await db.execute(
                    f"DELETE FROM worker_events "
                    f"WHERE worker_id IN ({ph})", batch,
                )
                events_purged += cur.rowcount
                cur = await db.execute(
                    f"DELETE FROM escalations "
                    f"WHERE related_worker_id IN ({ph})", batch,
                )
                escalations_purged += cur.rowcount
                cur = await db.execute(
                    f"DELETE FROM workers "
                    f"WHERE worker_id IN ({ph})", batch,
                )
                workers_purged += cur.rowcount
            # Cancel pending scheduled events tied to this project.
            cur = await db.execute(
                "UPDATE scheduled_events SET cancelled_at=?, "
                "cancel_reason='project archived' "
                "WHERE project_id=? AND fired_at IS NULL "
                "AND cancelled_at IS NULL",
                (utcnow(), project_id),
            )
            scheduled_cancelled = cur.rowcount
            cur = await db.execute(
                "DELETE FROM projects WHERE project_id=?",
                (project_id,),
            )
            project_deleted = cur.rowcount > 0
            await db.commit()
            return {
                "archived": project_deleted,
                "channel_id": channel_id,
                "workers_purged": workers_purged,
                "events_purged": events_purged,
                "escalations_purged": escalations_purged,
                "scheduled_cancelled": scheduled_cancelled,
            }

    # --- workers ---
    async def record_worker_spawn(self, worker_id: str, project_id: str,
                                   task: str, model: str, pid: int,
                                   engine: str = "claude") -> None:
        async with self._connect() as db:
            now = utcnow()
            await db.execute(
                "INSERT INTO workers (worker_id, project_id, task, model, state, "
                "spawned_at, pid, last_activity_at, engine) "
                "VALUES (?,?,?,?, 'running', ?,?,?,?)",
                (worker_id, project_id, task, model, now, pid, now, engine),
            )
            await db.commit()

    async def update_worker_state(self, worker_id: str, state: str,
                                   summary: str | None = None) -> None:
        async with self._connect() as db:
            now = utcnow()
            terminal = state in ("completed", "failed", "killed")
            await db.execute(
                "UPDATE workers SET state=?, last_activity_at=?, "
                "ended_at=CASE WHEN ? THEN ? ELSE ended_at END, "
                "summary=COALESCE(?, summary) WHERE worker_id=?",
                (state, now, terminal, now if terminal else None, summary, worker_id),
            )
            await db.commit()

    async def touch_worker(self, worker_id: str) -> None:
        async with self._connect() as db:
            await db.execute(
                "UPDATE workers SET last_activity_at=? WHERE worker_id=?",
                (utcnow(), worker_id),
            )
            await db.commit()

    async def set_worker_thread(self, worker_id: str,
                                  thread_id: int) -> None:
        async with self._connect() as db:
            await db.execute(
                "UPDATE workers SET thread_id=? WHERE worker_id=?",
                (thread_id, worker_id),
            )
            await db.commit()

    async def touch_worker_heartbeat(self, worker_id: str) -> None:
        async with self._connect() as db:
            await db.execute(
                "UPDATE workers SET last_heartbeat_at=? WHERE worker_id=?",
                (utcnow(), worker_id),
            )
            await db.commit()

    async def bump_continuations(self, worker_id: str) -> int:
        """Increment continuation counter and return the new value."""
        async with self._connect() as db:
            await db.execute(
                "UPDATE workers SET continuations_count="
                "COALESCE(continuations_count,0)+1 WHERE worker_id=?",
                (worker_id,),
            )
            await db.commit()
            async with db.execute(
                "SELECT continuations_count FROM workers WHERE worker_id=?",
                (worker_id,),
            ) as cur:
                row = await cur.fetchone()
                return int(row[0]) if row else 0

    async def get_worker(self, worker_id: str) -> dict[str, Any] | None:
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM workers WHERE worker_id=?", (worker_id,)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def list_workers(self, project_id: str | None = None,
                            state: str | None = None) -> list[dict[str, Any]]:
        q = "SELECT * FROM workers WHERE 1=1"
        args: list[Any] = []
        if project_id:
            q += " AND project_id=?"
            args.append(project_id)
        if state:
            q += " AND state=?"
            args.append(state)
        q += " ORDER BY spawned_at DESC"
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(q, args) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def reconcile_orphan_workers(self) -> int:
        """Called on daemon startup. Any worker still marked 'running'
        in the DB is an orphan from a previous run — the daemon's
        in-memory state (subprocess handles, sessions, monitor tasks)
        was lost when it restarted. We can't recover them, so mark
        them killed with a clear reason. Returns the count reconciled."""
        async with self._connect() as db:
            now = utcnow()
            cur = await db.execute(
                "UPDATE workers SET state='killed', "
                "ended_at=COALESCE(ended_at, ?), "
                "summary=COALESCE(summary, ?) || ' [orphaned: daemon "
                "restarted before this worker finished; subprocess "
                "state lost]' "
                "WHERE state IN ('running', 'exited_no_summary')",
                (now, ""),
            )
            await db.commit()
            return cur.rowcount

    # --- worker events ---
    async def record_worker_event(self, worker_id: str, kind: str,
                                   payload: dict, severity: str | None = None) -> int:
        async with self._connect() as db:
            cur = await db.execute(
                "INSERT INTO worker_events (worker_id, kind, severity, payload, "
                "created_at) VALUES (?,?,?,?,?)",
                (worker_id, kind, severity, json.dumps(payload), utcnow()),
            )
            await db.commit()
            return cur.lastrowid  # type: ignore[return-value]

    async def get_worker_completion_details(self, worker_id: str) -> dict:
        """Return the artifacts/rejected_artifacts/open_items captured
        in a worker's most recent completion event. Empty lists if the
        worker hasn't completed (e.g., running, failed, killed)."""
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT payload FROM worker_events "
                "WHERE worker_id=? AND kind='completion' "
                "ORDER BY id DESC LIMIT 1",
                (worker_id,),
            ) as cur:
                row = await cur.fetchone()
        if not row:
            return {"artifacts": [], "rejected_artifacts": [], "open_items": []}
        try:
            payload = json.loads(row["payload"])
        except Exception:
            return {"artifacts": [], "rejected_artifacts": [], "open_items": []}
        return {
            "artifacts": payload.get("artifacts", []) or [],
            "rejected_artifacts": payload.get("rejected_artifacts", []) or [],
            "open_items": payload.get("open_items", []) or [],
        }

    # --- escalations ---
    async def record_escalation(self, escalation_id: str, summary: str,
                                 context: str, options: list[dict] | None,
                                 recommendation: str | None, urgency: str,
                                 related_worker_id: str | None) -> None:
        async with self._connect() as db:
            await db.execute(
                "INSERT INTO escalations (escalation_id, summary, context, options, "
                "recommendation, urgency, related_worker_id, created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (escalation_id, summary, context,
                 json.dumps(options) if options else None,
                 recommendation, urgency, related_worker_id, utcnow()),
            )
            await db.commit()

    async def set_escalation_message(self, escalation_id: str,
                                       message_id: int, channel_id: int) -> None:
        async with self._connect() as db:
            await db.execute(
                "UPDATE escalations SET message_id=?, channel_id=? "
                "WHERE escalation_id=?",
                (message_id, channel_id, escalation_id),
            )
            await db.commit()

    async def get_escalation_by_message(self, message_id: int) -> dict[str, Any] | None:
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM escalations WHERE message_id=?",
                (message_id,),
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def resolve_escalation(self, escalation_id: str, resolution: str) -> None:
        async with self._connect() as db:
            await db.execute(
                "UPDATE escalations SET resolved_at=?, resolution=? WHERE escalation_id=?",
                (utcnow(), resolution, escalation_id),
            )
            await db.commit()

    # --- OM sessions / turns ---
    async def start_om_session(self, session_id: str) -> None:
        async with self._connect() as db:
            await db.execute(
                "INSERT OR IGNORE INTO om_sessions (session_id, started_at) VALUES (?,?)",
                (session_id, utcnow()),
            )
            await db.commit()

    async def record_om_turn(self, session_id: str, trigger_event: str,
                              trigger_detail: str, input_tokens: int,
                              output_tokens: int, tool_calls: list[str],
                              started_at: str, ended_at: str) -> None:
        async with self._connect() as db:
            await db.execute(
                "INSERT INTO om_turns (session_id, trigger_event, trigger_detail, "
                "input_tokens, output_tokens, tool_calls, started_at, ended_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (session_id, trigger_event, trigger_detail, input_tokens,
                 output_tokens, json.dumps(tool_calls), started_at, ended_at),
            )
            await db.execute(
                "UPDATE om_sessions SET turn_count=turn_count+1, "
                "estimated_tokens=estimated_tokens+? WHERE session_id=?",
                (input_tokens + output_tokens, session_id),
            )
            await db.commit()

    async def current_session_tokens(self, session_id: str) -> int:
        async with self._connect() as db:
            async with db.execute(
                "SELECT estimated_tokens FROM om_sessions WHERE session_id=?",
                (session_id,),
            ) as cur:
                row = await cur.fetchone()
                return int(row[0]) if row else 0

    async def close_session(self, session_id: str,
                             succeeded_by: str | None = None) -> None:
        async with self._connect() as db:
            await db.execute(
                "UPDATE om_sessions SET ended_at=?, succeeded_by=?, state='closed' "
                "WHERE session_id=?",
                (utcnow(), succeeded_by, session_id),
            )
            await db.commit()

    async def set_session_state(self, session_id: str, state: str) -> None:
        async with self._connect() as db:
            await db.execute(
                "UPDATE om_sessions SET state=? WHERE session_id=?",
                (state, session_id),
            )
            await db.commit()

    async def save_compaction_summary(self, session_id: str, summary: str) -> None:
        async with self._connect() as db:
            await db.execute(
                "UPDATE om_sessions SET compaction_summary=? WHERE session_id=?",
                (summary, session_id),
            )
            await db.commit()

    async def get_compaction_summary(self, session_id: str) -> str | None:
        async with self._connect() as db:
            async with db.execute(
                "SELECT compaction_summary FROM om_sessions WHERE session_id=?",
                (session_id,),
            ) as cur:
                row = await cur.fetchone()
                return row[0] if row and row[0] else None

    async def latest_closed_session_summary(self) -> str | None:
        async with self._connect() as db:
            async with db.execute(
                "SELECT compaction_summary FROM om_sessions "
                "WHERE state='closed' AND compaction_summary IS NOT NULL "
                "ORDER BY ended_at DESC LIMIT 1"
            ) as cur:
                row = await cur.fetchone()
                return row[0] if row else None

    async def get_active_om_session(self) -> str | None:
        async with self._connect() as db:
            async with db.execute(
                "SELECT session_id FROM om_sessions "
                "WHERE ended_at IS NULL AND state='active' "
                "ORDER BY started_at DESC LIMIT 1"
            ) as cur:
                row = await cur.fetchone()
                return row[0] if row else None

    async def session_started_at(self, session_id: str) -> str | None:
        async with self._connect() as db:
            async with db.execute(
                "SELECT started_at FROM om_sessions WHERE session_id=?",
                (session_id,),
            ) as cur:
                row = await cur.fetchone()
                return row[0] if row else None

    # --- rate limits ---

    async def update_rate_limit(self, info: dict,
                                  observed_by: str) -> None:
        rl_type = info.get("rateLimitType")
        if not rl_type:
            return
        resets_at = info.get("resetsAt")
        resets_int = int(resets_at) if resets_at is not None else None
        status = info.get("status")
        overage_status = info.get("overageStatus")
        is_using_overage = 1 if info.get("isUsingOverage") else 0
        async with self._connect() as db:
            await db.execute(
                "INSERT INTO rate_limits (rate_limit_type, status, resets_at, "
                "overage_status, is_using_overage, observed_at, observed_by) "
                "VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(rate_limit_type) DO UPDATE SET "
                "  status=excluded.status, "
                "  resets_at=excluded.resets_at, "
                "  overage_status=excluded.overage_status, "
                "  is_using_overage=excluded.is_using_overage, "
                "  observed_at=excluded.observed_at, "
                "  observed_by=excluded.observed_by",
                (rl_type, status, resets_int, overage_status,
                 is_using_overage, utcnow(), observed_by),
            )
            await db.commit()

    async def get_rate_limits(self) -> list[dict[str, Any]]:
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM rate_limits ORDER BY rate_limit_type"
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def claude_rate_limit_pressure(self) -> dict | None:
        """Returns a non-None summary if Claude is currently in a
        rate-limit state that warrants offloading new worker spawns to
        Codex. Returns None if Claude is fine.

        We treat anything other than status='allowed' as pressure on
        the 5-hour window — most commonly 'allowed_warning' (~90% of
        the 5-hour window consumed). The 7-day window is intentionally
        excluded: it can sit at warning for days, and auto-routing
        across that whole period would be too aggressive. The window
        is considered active if resets_at is in the future (or unset,
        in which case we trust the most recent observation).
        """
        import time as _time
        now = int(_time.time())
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM rate_limits "
                "WHERE rate_limit_type = 'five_hour' "
                "AND status IS NOT NULL AND status != 'allowed' "
                "AND (resets_at IS NULL OR resets_at > ?) "
                "ORDER BY rate_limit_type",
                (now,),
            ) as cur:
                rows = [dict(r) for r in await cur.fetchall()]
        if not rows:
            return None
        # Build a compact summary: which types are in trouble and when
        # they reset, suitable for logging and a Discord notice.
        summary = {
            "types": [],
            "details": rows,
        }
        for r in rows:
            summary["types"].append({
                "type": r.get("rate_limit_type"),
                "status": r.get("status"),
                "resets_at": r.get("resets_at"),
            })
        return summary

    # --- event log ---

    async def log_event(self, kind: str, summary: str,
                          project_id: str | None = None,
                          details: dict | None = None) -> None:
        async with self._connect() as db:
            await db.execute(
                "INSERT INTO event_log (timestamp, kind, project_id, summary, details) "
                "VALUES (?,?,?,?,?)",
                (utcnow(), kind, project_id, summary,
                 json.dumps(details) if details else None),
            )
            await db.execute(
                "DELETE FROM event_log WHERE id < ("
                "SELECT MIN(id) FROM (SELECT id FROM event_log "
                "ORDER BY id DESC LIMIT 1000))"
            )
            await db.commit()

    async def recent_events(self, limit: int = 50,
                              project_id: str | None = None,
                              since_iso: str | None = None) -> list[dict[str, Any]]:
        q = "SELECT * FROM event_log WHERE 1=1"
        args: list[Any] = []
        if project_id:
            q += " AND (project_id=? OR project_id IS NULL)"
            args.append(project_id)
        if since_iso:
            q += " AND timestamp >= ?"
            args.append(since_iso)
        q += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(q, args) as cur:
                rows = await cur.fetchall()
                return list(reversed([dict(r) for r in rows]))

    # --- scheduled events ---

    async def schedule_event(self, fire_at_iso: str, reason: str,
                              project_id: str | None,
                              created_by_session_id: str | None,
                              condition_json: str | None = None) -> int:
        async with self._connect() as db:
            cur = await db.execute(
                "INSERT INTO scheduled_events "
                "(fire_at, reason, project_id, created_at, "
                "created_by_session_id, condition_json) "
                "VALUES (?,?,?,?,?,?)",
                (fire_at_iso, reason, project_id, utcnow(),
                 created_by_session_id, condition_json),
            )
            await db.commit()
            return cur.lastrowid  # type: ignore[return-value]

    async def cancel_scheduled_event(self, schedule_id: int,
                                       cancel_reason: str) -> bool:
        async with self._connect() as db:
            cur = await db.execute(
                "UPDATE scheduled_events SET cancelled_at=?, cancel_reason=? "
                "WHERE id=? AND fired_at IS NULL AND cancelled_at IS NULL",
                (utcnow(), cancel_reason, schedule_id),
            )
            await db.commit()
            return cur.rowcount > 0

    async def pending_scheduled_events(self, limit: int = 100) -> list[dict[str, Any]]:
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM scheduled_events "
                "WHERE fired_at IS NULL AND cancelled_at IS NULL "
                "ORDER BY fire_at ASC LIMIT ?",
                (limit,),
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def due_scheduled_events(self) -> list[dict[str, Any]]:
        now = utcnow()
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM scheduled_events "
                "WHERE fired_at IS NULL AND cancelled_at IS NULL "
                "AND fire_at <= ? "
                "ORDER BY fire_at ASC",
                (now,),
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def mark_scheduled_event_fired(self, schedule_id: int) -> None:
        async with self._connect() as db:
            await db.execute(
                "UPDATE scheduled_events SET fired_at=? WHERE id=?",
                (utcnow(), schedule_id),
            )
            await db.commit()

    async def count_pending_scheduled_events(self) -> int:
        async with self._connect() as db:
            async with db.execute(
                "SELECT COUNT(*) FROM scheduled_events "
                "WHERE fired_at IS NULL AND cancelled_at IS NULL"
            ) as cur:
                row = await cur.fetchone()
                return int(row[0]) if row else 0

    async def purge_old_workers(self, hours: int) -> dict:
        """Remove terminal-state workers (completed, failed, killed)
        whose last activity was older than `hours` hours ago. Cascades
        to worker_events. Active states (running, paused,
        exited_no_summary) are never purged.

        Returns a dict with counts of purged workers + events."""
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(hours=int(hours))).isoformat()
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            # Find purgeable worker IDs first so we can also clean their
            # events. last_activity_at falls back to ended_at then
            # spawned_at if the worker never reported.
            async with db.execute(
                "SELECT worker_id FROM workers "
                "WHERE state IN ('completed','failed','killed') "
                "AND COALESCE(last_activity_at, ended_at, spawned_at) < ?",
                (cutoff,),
            ) as cur:
                ids = [r["worker_id"] for r in await cur.fetchall()]
            if not ids:
                return {"workers_purged": 0, "events_purged": 0}
            # SQLite has a limit on parameter count; chunk the deletes
            # at 500 per batch.
            workers_purged = 0
            events_purged = 0
            for i in range(0, len(ids), 500):
                batch = ids[i:i + 500]
                placeholders = ",".join(["?"] * len(batch))
                cur = await db.execute(
                    f"DELETE FROM worker_events "
                    f"WHERE worker_id IN ({placeholders})",
                    batch,
                )
                events_purged += cur.rowcount
                cur = await db.execute(
                    f"DELETE FROM workers "
                    f"WHERE worker_id IN ({placeholders})",
                    batch,
                )
                workers_purged += cur.rowcount
            await db.commit()
            return {
                "workers_purged": workers_purged,
                "events_purged": events_purged,
            }

    # --- aggregated orchestrator stats ---

    async def get_open_escalations(self,
                                     project_id: str | None = None
                                     ) -> list[dict[str, Any]]:
        """Open escalations are the canonical 'user must decide' surface."""
        q = ("SELECT e.*, w.project_id AS worker_project_id "
             "FROM escalations e "
             "LEFT JOIN workers w ON e.related_worker_id = w.worker_id "
             "WHERE e.resolved_at IS NULL")
        args: list[Any] = []
        if project_id:
            q += (" AND (w.project_id = ? OR "
                  "(w.project_id IS NULL AND ? IS NULL))")
            args.extend([project_id, project_id])
        q += " ORDER BY e.created_at DESC"
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(q, args) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def project_summaries(self) -> list[dict[str, Any]]:
        """Per-project rollup for /projects: each project + active worker
        count + total worker count + last activity timestamp."""
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT p.project_id, p.path, p.description, "
                "p.status_channel_id, p.max_concurrent_workers, "
                "  (SELECT COUNT(*) FROM workers w "
                "   WHERE w.project_id=p.project_id AND w.state='running'"
                "  ) AS active_workers, "
                "  (SELECT COUNT(*) FROM workers w "
                "   WHERE w.project_id=p.project_id "
                "  ) AS total_workers, "
                "  (SELECT MAX(spawned_at) FROM workers w "
                "   WHERE w.project_id=p.project_id "
                "  ) AS last_worker_at "
                "FROM projects p ORDER BY p.project_id"
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def escalations_today(self) -> int:
        today = _start_of_day_utc()
        async with self._connect() as db:
            async with db.execute(
                "SELECT COUNT(*) FROM escalations WHERE created_at >= ?",
                (today,),
            ) as cur:
                row = await cur.fetchone()
                return int(row[0]) if row else 0

    async def orchestrator_stats(self, session_id: str | None) -> dict[str, Any]:
        today = _start_of_day_utc()
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row

            session_turns = 0
            session_tokens_est = 0
            session_started_at: str | None = None
            if session_id:
                async with db.execute(
                    "SELECT turn_count, estimated_tokens, started_at "
                    "FROM om_sessions WHERE session_id=?",
                    (session_id,),
                ) as cur:
                    row = await cur.fetchone()
                    if row:
                        session_turns = int(row["turn_count"] or 0)
                        session_tokens_est = int(row["estimated_tokens"] or 0)
                        session_started_at = row["started_at"]

            async with db.execute(
                "SELECT COUNT(*) FROM workers WHERE state='running'"
            ) as cur:
                row = await cur.fetchone()
                active_workers = int(row[0]) if row else 0

            async with db.execute(
                "SELECT COUNT(*) FROM workers WHERE spawned_at >= ?",
                (today,),
            ) as cur:
                row = await cur.fetchone()
                workers_spawned_today = int(row[0]) if row else 0

            async with db.execute(
                "SELECT COUNT(*) FROM event_log "
                "WHERE kind='compaction' AND timestamp >= ?",
                (today,),
            ) as cur:
                row = await cur.fetchone()
                compactions_today = int(row[0]) if row else 0

            async with db.execute(
                "SELECT COUNT(*) FROM worker_events "
                "WHERE severity IN ('warning','error') AND created_at >= ?",
                (today,),
            ) as cur:
                row = await cur.fetchone()
                warnings_today = int(row[0]) if row else 0

            async with db.execute(
                "SELECT COUNT(*) FROM escalations WHERE resolved_at IS NULL"
            ) as cur:
                row = await cur.fetchone()
                open_escalations = int(row[0]) if row else 0

            async with db.execute(
                "SELECT COUNT(*) FROM escalations WHERE created_at >= ?",
                (today,),
            ) as cur:
                row = await cur.fetchone()
                escalations_today = int(row[0]) if row else 0

            async with db.execute(
                "SELECT COUNT(*) FROM scheduled_events "
                "WHERE fired_at IS NULL AND cancelled_at IS NULL"
            ) as cur:
                row = await cur.fetchone()
                pending_scheduled = int(row[0]) if row else 0

        return {
            "session_turns": session_turns,
            "session_tokens_est": session_tokens_est,
            "session_started_at": session_started_at,
            "active_workers": active_workers,
            "workers_spawned_today": workers_spawned_today,
            "compactions_today": compactions_today,
            "warnings_today": warnings_today,
            "open_escalations": open_escalations,
            "escalations_today": escalations_today,
            "pending_scheduled_events": pending_scheduled,
        }