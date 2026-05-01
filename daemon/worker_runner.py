"""Worker runner: spawns claude / codex CLI workers and tracks their
lifecycle.

This module owns the WorkerRunner class — subprocess spawn, monitor
tasks, decision-request mediation, kill / sweep / quiet-detection.
Pure helpers (artifact validation, input/output summarization, event
scanning, tail-output parsing) live in
:mod:`daemon.worker_helpers` and :mod:`daemon.worker_output`.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import traceback
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .bus import Event, EventBus, EventKind
from .config import DaemonConfig
from .db import DB
from .worker_helpers import (
    RECENT_TOOLS_TO_KEEP,
    scan_worker_events,
    summarize_input,
    summarize_output,
    truncate,
    validate_artifacts,
)
from .worker_output import tail_output_claude, tail_output_codex

log = logging.getLogger(__name__)

STREAM_BUFFER_LIMIT = 10 * 1024 * 1024
HEARTBEAT_MIN_INTERVAL_S = 10
# When a worker hits its max-turns limit, daemon launches a tiny extra
# subprocess to give it room for one final mark_task_paused call.
CONTINUATION_TURN_BUDGET = 5
# Hard cap on continuation cycles per worker. Past this, force failure.
MAX_CONTINUATIONS_PER_WORKER = 3
# Default per-project cap on concurrent running workers if the project
# row's max_concurrent_workers is NULL. Set conservatively — most
# projects benefit from low parallelism (avoid file races, easier to
# follow what's happening). Override per-project via OM's
# set_project_worker_cap tool.
DEFAULT_PROJECT_WORKER_CAP = 3

# Backwards-compat aliases for any internal callers that still use the
# underscored names. Remove these once no callers remain.
_validate_artifacts = validate_artifacts
_truncate = truncate
_summarize_input = summarize_input
_summarize_output = summarize_output
_scan_worker_events = scan_worker_events


class WorkerRunner:
    def __init__(self, cfg: DaemonConfig, db: DB,
                  bus: EventBus | None = None, bot=None):
        self.cfg = cfg
        self.db = db
        self.bus = bus
        self.bot = bot
        self._tasks: dict[str, asyncio.Task] = {}
        self._pending_decisions: dict[str, asyncio.Future] = {}
        self._sessions: dict[str, str] = {}
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._last_heartbeat: dict[str, float] = {}
        # Per-worker turn budget tracking, used to detect max-turns hits.
        self._turn_budget: dict[str, int] = {}
        self._turn_count: dict[str, int] = {}
        # Workers we've already fired a WORKER_QUIET event for in their
        # current run. Reset when the worker exits (so a resumed worker
        # can be quiet-warned again).
        self._quiet_warned: set[str] = set()

    def bind_bus(self, bus: EventBus) -> None:
        self.bus = bus

    async def spawn(self, project_id: str, task: str, model: str = "sonnet",
                     allow_edits: bool = True, max_turns: int = 50,
                     enable_agent_teams: bool = False,
                     engine: str = "claude") -> dict:
        if engine not in ("claude", "codex"):
            raise ValueError(
                f"unknown engine {engine!r}. Must be 'claude' or 'codex'."
            )
        project = await self.db.get_project(project_id)
        if not project:
            raise ValueError(f"Unknown project: {project_id}")

        # Auto-routing: if requested engine is Claude AND Claude is in
        # rate-limit pressure (typically the 90% allowed_warning state
        # of the 5-hour window), reroute to Codex so we preserve the
        # remaining ~10% of Claude budget for OM itself. OM runs on
        # Claude and can't fall back, so reserving headroom for it
        # keeps the orchestrator functional during the warning window.
        # If user explicitly requested Codex, no rerouting needed.
        # If Codex is requested, just proceed.
        auto_rerouted = False
        original_engine = engine
        pressure_summary: dict | None = None
        if engine == "claude":
            pressure_summary = await self.db.claude_rate_limit_pressure()
            if pressure_summary:
                engine = "codex"
                auto_rerouted = True
                log.info(
                    "Auto-rerouting spawn to Codex due to Claude rate "
                    "pressure: %s", pressure_summary["types"],
                )

        # Per-project worker cap. NULL in the project row falls back
        # to the global default. The cap counts state='running' only —
        # paused/exited_no_summary workers don't consume slots.
        cap = project.get("max_concurrent_workers")
        if cap is None:
            cap = DEFAULT_PROJECT_WORKER_CAP
        running_count = await self.db.count_running_workers_for_project(
            project_id
        )
        if running_count >= int(cap):
            raise ValueError(
                f"Project '{project_id}' is at its worker cap "
                f"({running_count}/{cap} running). Either kill an "
                f"existing worker (kill_worker), wait for one to "
                f"complete, or raise the cap with "
                f"set_project_worker_cap if more parallelism is safe "
                f"for this project."
            )

        worker_id = uuid.uuid4().hex[:8]
        worker_dir = self.cfg.workspace / "workers" / worker_id
        worker_dir.mkdir(parents=True, exist_ok=True)

        # Engine-specific preamble + launch.
        if engine == "claude":
            mcp_path = self._write_mcp_config(worker_dir, worker_id)
            full_task = self._render_preamble(
                "worker_preamble.md", task,
            )
            (worker_dir / "task.md").write_text(full_task)
            proc = await self._launch_claude(
                worker_id=worker_id, cwd=project["path"], mcp_path=mcp_path,
                model=model, allow_edits=allow_edits, max_turns=max_turns,
                enable_agent_teams=enable_agent_teams, prompt=full_task,
                resume_session_id=None, worker_dir=worker_dir,
            )
            monitor_coro = self._monitor(worker_id, proc, worker_dir)
        else:
            # codex: per-worker CODEX_HOME with isolated MCP config.
            # Workers can call mark_task_complete et al. via MCP just
            # like Claude workers. The synthesis fallback in
            # _monitor_codex still kicks in if a worker exits without
            # calling a completion tool.
            codex_home = self._write_codex_home(worker_dir, worker_id)
            full_task = self._render_preamble(
                "worker_preamble_codex.md", task,
            )
            (worker_dir / "task.md").write_text(full_task)
            proc = await self._launch_codex(
                worker_id=worker_id, cwd=project["path"],
                model=model, allow_edits=allow_edits,
                prompt=full_task, worker_dir=worker_dir,
                codex_home=codex_home,
            )
            monitor_coro = self._monitor_codex(worker_id, proc, worker_dir)

        self._processes[worker_id] = proc
        self._turn_budget[worker_id] = int(max_turns)
        self._turn_count[worker_id] = 0
        self._quiet_warned.discard(worker_id)
        await self.db.record_worker_spawn(
            worker_id, project_id, task, model, proc.pid, engine=engine,
        )
        self._tasks[worker_id] = asyncio.create_task(monitor_coro)
        log.info("Worker %s spawned pid=%s project=%s engine=%s%s",
                 worker_id, proc.pid, project_id, engine,
                 " (auto-rerouted from claude)" if auto_rerouted else "")
        return {
            "worker_id": worker_id,
            "engine": engine,
            "requested_engine": original_engine,
            "auto_rerouted": auto_rerouted,
            "pressure": pressure_summary if auto_rerouted else None,
        }

    async def send(self, worker_id: str, message: str,
                    max_turns: int = 50) -> bool:
        worker = await self.db.get_worker(worker_id)
        if not worker:
            raise ValueError(f"Unknown worker: {worker_id}")

        session_id = self._sessions.get(worker_id)
        if not session_id:
            raise ValueError(
                f"Worker {worker_id} has no recorded session_id — "
                f"it may not have initialized yet, or the daemon was "
                f"restarted (session IDs are in-memory only)."
            )

        if worker["state"] == "running":
            raise ValueError(
                f"Worker {worker_id} is still running. Wait for it to "
                f"finish before sending a follow-up."
            )

        project = await self.db.get_project(worker["project_id"])
        assert project is not None

        worker_dir = self.cfg.workspace / "workers" / worker_id
        mcp_path = worker_dir / "mcp.json"

        proc = await self._launch_claude(
            worker_id=worker_id, cwd=project["path"], mcp_path=mcp_path,
            model=worker["model"] or self.cfg.worker_default_model,
            allow_edits=True, max_turns=max_turns,
            enable_agent_teams=self.cfg.enable_agent_teams,
            prompt=message, resume_session_id=session_id,
            worker_dir=worker_dir,
        )
        self._processes[worker_id] = proc
        self._turn_budget[worker_id] = int(max_turns)
        self._turn_count[worker_id] = 0
        self._quiet_warned.discard(worker_id)
        await self.db.update_worker_state(worker_id, "running")
        self._tasks[worker_id] = asyncio.create_task(
            self._monitor(worker_id, proc, worker_dir)
        )
        log.info("Worker %s resumed pid=%s session=%s budget=%d",
                 worker_id, proc.pid, session_id[:8], max_turns)
        return True

    def _render_preamble(self, preamble_filename: str, task: str) -> str:
        """Read a worker preamble file and substitute ``{user_name}``
        and ``{task}`` placeholders. Falls back to appending the task
        under a ``## Task`` heading if the preamble has no ``{task}``
        placeholder (so unstructured preambles still work)."""
        preamble_path = (self.cfg.workspace / "prompts"
                          / preamble_filename)
        preamble = preamble_path.read_text() if preamble_path.exists() else ""
        preamble = preamble.replace("{user_name}", self.cfg.user_name)
        if "{task}" in preamble:
            return preamble.replace("{task}", task)
        return f"{preamble}\n\n## Task\n{task}"

    def _write_mcp_config(self, worker_dir: Path, worker_id: str) -> Path:
        mcp_config = {
            "mcpServers": {
                "orchestrator-worker": {
                    "type": "stdio",
                    "command": sys.executable,
                    "args": ["-m", "mcp_servers.worker_tools"],
                    "cwd": str(self.cfg.workspace),
                    "env": {
                        "ORCH_IPC_SOCKET": str(self.cfg.ipc_socket),
                        "ORCH_WORKER_ID": worker_id,
                        "PYTHONPATH": str(self.cfg.workspace),
                    },
                }
            }
        }
        mcp_path = worker_dir / "mcp.json"
        mcp_path.write_text(json.dumps(mcp_config, indent=2))
        return mcp_path

    def _write_codex_home(self, worker_dir: Path,
                           worker_id: str) -> Path:
        """Build a per-worker CODEX_HOME directory. Codex reads
        ~/.codex/config.toml + auth.json for MCP setup and credentials;
        we override CODEX_HOME at launch time to point at this isolated
        directory so we can register our orchestrator MCP server
        per-worker without polluting the user's main Codex config.

        We symlink auth.json from the user's main ~/.codex/ so the
        worker uses the same Plus subscription credentials. We write
        a fresh config.toml declaring our orchestrator MCP server.
        """
        codex_home = worker_dir / ".codex_home"
        codex_home.mkdir(parents=True, exist_ok=True)

        # Symlink auth so the worker uses the user's Plus login. If
        # auth.json doesn't exist (user hasn't run `codex login`), the
        # Codex subprocess will print a clear auth error and exit —
        # the daemon will surface that as a worker failure.
        auth_target = Path.home() / ".codex" / "auth.json"
        auth_link = codex_home / "auth.json"
        if not auth_link.exists() and not auth_link.is_symlink():
            if auth_target.exists():
                auth_link.symlink_to(auth_target)
            else:
                log.warning(
                    "Codex auth.json missing at %s. Codex worker %s "
                    "will fail to authenticate. Run `codex login` "
                    "as the daemon's user to fix.",
                    auth_target, worker_id,
                )

        # TOML has no variable interpolation; values must be quoted
        # literals. json.dumps gives us correct escaping for ASCII
        # paths (which is what we have on Linux/WSL).
        def q(value) -> str:
            return json.dumps(str(value))

        config_toml = (
            "# Auto-generated by Tensai daemon. Do not edit.\n"
            "# Per-worker Codex config with orchestrator MCP enabled.\n"
            "\n"
            "[mcp_servers.orchestrator-worker]\n"
            f"command = {q(sys.executable)}\n"
            'args = ["-m", "mcp_servers.worker_tools"]\n'
            f"cwd = {q(self.cfg.workspace)}\n"
            "startup_timeout_sec = 15.0\n"
            "tool_timeout_sec = 60.0\n"
            "required = false\n"
            "\n"
            "[mcp_servers.orchestrator-worker.env]\n"
            f"ORCH_IPC_SOCKET = {q(self.cfg.ipc_socket)}\n"
            f"ORCH_WORKER_ID = {q(worker_id)}\n"
            f"PYTHONPATH = {q(self.cfg.workspace)}\n"
        )
        (codex_home / "config.toml").write_text(config_toml)
        return codex_home

    async def _launch_claude(self, *, worker_id: str, cwd: str,
                              mcp_path: Path, model: str, allow_edits: bool,
                              max_turns: int, enable_agent_teams: bool,
                              prompt: str, resume_session_id: str | None,
                              worker_dir: Path):
        tools = "mcp__orchestrator-worker,Bash,Read,Write,Edit,Glob,Grep"
        if not allow_edits:
            tools = "mcp__orchestrator-worker,Read,Glob,Grep"

        args = [
            "claude", "-p",
            "--model", model,
            "--mcp-config", str(mcp_path),
            "--allowedTools", tools,
            "--permission-mode", "acceptEdits",
            "--output-format", "stream-json",
            "--verbose",
            "--max-turns", str(max_turns),
        ]
        if resume_session_id:
            args += ["--resume", resume_session_id]
        args.append(prompt)

        env = os.environ.copy()
        env.pop("ANTHROPIC_API_KEY", None)
        if enable_agent_teams:
            env["CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS"] = "1"

        stderr_path = worker_dir / "stderr.log"
        stderr_fh = open(stderr_path, "ab")
        try:
            return await asyncio.create_subprocess_exec(
                *args, cwd=cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=stderr_fh,
                env=env, limit=STREAM_BUFFER_LIMIT,
            )
        finally:
            stderr_fh.close()

    async def _launch_codex(self, *, worker_id: str, cwd: str,
                              model: str, allow_edits: bool,
                              prompt: str, worker_dir: Path,
                              codex_home: Path):
        """Launch a Codex worker subprocess with isolated MCP config.

        Codex CLI doesn't take a max-turns flag the way Claude does; it
        runs until the agent decides it's done or the user (us) kills
        it. We rely on the daemon-level stuck-worker sweep to bound
        runaway workers.

        Codex events are JSONL on stdout — one event per state change.
        Item types we care about: agent_message, command_execution,
        file_change, reasoning. Top-level types: thread.started,
        turn.started, turn.completed, turn.failed, item.started,
        item.completed, error.

        codex_home points at a per-worker .codex_home/ directory with
        a config.toml that registers our orchestrator-worker MCP
        server, plus a symlinked auth.json for ChatGPT Plus auth.
        """
        args = ["codex", "exec", "--json"]
        if allow_edits:
            # workspace-write sandbox + on-request approvals — closest
            # equivalent to Claude's acceptEdits permission mode.
            args.append("--full-auto")
        # We don't enforce git for our projects; many user repos are
        # WSL-mounted from Windows and may not be a git repo at all.
        args.append("--skip-git-repo-check")
        # --ephemeral skips Codex's own rollout/session log. The
        # config.toml under CODEX_HOME is still loaded.
        args.append("--ephemeral")
        # Working directory for the worker.
        args += ["--cd", cwd]
        # Model translation: the OM may pass "sonnet" out of habit. If so,
        # let codex pick its default rather than failing on an unknown
        # model name. Otherwise pass through.
        if model and model not in ("sonnet", "opus", "haiku"):
            args += ["--model", model]
        # Prompt as final positional arg.
        args.append(prompt)

        env = os.environ.copy()
        # Codex uses ~/.codex/auth.json (ChatGPT Plus). Don't pass the
        # OpenAI API key — that would bill API instead of Plus.
        env.pop("OPENAI_API_KEY", None)
        env.pop("CODEX_API_KEY", None)
        # Per-worker config + auth, isolated from the user's main ~/.codex.
        env["CODEX_HOME"] = str(codex_home)

        stderr_path = worker_dir / "stderr.log"
        stderr_fh = open(stderr_path, "ab")
        try:
            return await asyncio.create_subprocess_exec(
                *args, cwd=cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=stderr_fh,
                env=env, limit=STREAM_BUFFER_LIMIT,
            )
        finally:
            stderr_fh.close()

    async def _monitor(self, worker_id: str,
                        proc: asyncio.subprocess.Process,
                        worker_dir: Path) -> None:
        events_file = worker_dir / "events.ndjson"
        stream_error: str | None = None
        terminated_by_completion_tool = False
        result_num_turns: int | None = None
        try:
            assert proc.stdout is not None
            while True:
                try:
                    line = await proc.stdout.readline()
                except ValueError as e:
                    log.warning("Worker %s: stream buffer overflow (%s); "
                                "dropping oversized line", worker_id, e)
                    stream_error = f"stream buffer overflow: {e}"
                    try:
                        while True:
                            chunk = await asyncio.wait_for(
                                proc.stdout.read(1024 * 1024), timeout=1.0
                            )
                            if not chunk or b"\n" in chunk:
                                break
                    except asyncio.TimeoutError:
                        pass
                    continue
                if not line:
                    break
                with events_file.open("ab") as f:
                    f.write(line)
                try:
                    msg = json.loads(line)
                except Exception:
                    continue
                if msg.get("type") == "system" and msg.get("subtype") == "init":
                    sid = msg.get("session_id")
                    if sid:
                        self._sessions[worker_id] = sid
                elif msg.get("type") == "assistant":
                    # Tracking only; authoritative count comes from the
                    # result event's num_turns. Our manual count tends
                    # to overcount because claude CLI emits separate
                    # assistant messages for thinking blocks vs tool
                    # use blocks.
                    content = msg.get("message", {}).get("content", [])
                    if content:
                        self._turn_count[worker_id] = (
                            self._turn_count.get(worker_id, 0) + 1
                        )
                elif msg.get("type") == "rate_limit_event":
                    info = msg.get("rate_limit_info", {}) or {}
                    if info.get("rateLimitType"):
                        asyncio.create_task(self.db.update_rate_limit(
                            info, observed_by=f"worker:{worker_id}"
                        ))
                elif msg.get("type") == "result":
                    # Authoritative turn count from claude CLI itself.
                    result_num_turns = int(msg.get("num_turns") or 0)
                    w = await self.db.get_worker(worker_id)
                    if w and w["state"] == "running":
                        # Worker subprocess is wrapping up but didn't
                        # call mark_task_complete/failed/paused. Mark
                        # this distinct from a real completion so we
                        # know to launch a continuation summary turn.
                        await self.db.update_worker_state(
                            worker_id, "exited_no_summary",
                            summary="(exited without completion tool call)",
                        )
                    else:
                        # State already changed via tool call (complete,
                        # failed, paused). Don't fire continuation.
                        terminated_by_completion_tool = True
            rc = await proc.wait()
            # Clear the quiet-warned flag now that the worker has exited;
            # if it gets resumed (manually or via continuation) it can be
            # quiet-warned again on a new staleness threshold crossing.
            self._quiet_warned.discard(worker_id)
            authoritative_turns = (result_num_turns
                                    if result_num_turns is not None
                                    else self._turn_count.get(worker_id, 0))
            log.info("Worker %s exited rc=%s turns=%d (manual=%d) budget=%d",
                     worker_id, rc, authoritative_turns,
                     self._turn_count.get(worker_id, 0),
                     self._turn_budget.get(worker_id, 0))

            # Generalized continuation trigger: if the worker exited
            # without calling a completion tool AND we have a session
            # to resume, launch a final summary turn — regardless of
            # whether the cause was max-turns or a voluntary stop.
            # The continuation prompt distinguishes the two cases so
            # the worker knows the situation.
            budget = self._turn_budget.get(worker_id, 0)
            hit_max_turns = (budget > 0
                              and authoritative_turns >= budget
                              and not terminated_by_completion_tool)
            should_continue = (not terminated_by_completion_tool
                                and self._sessions.get(worker_id) is not None)

            if should_continue:
                exit_reason = ("max-turns" if hit_max_turns
                                else "voluntary stop without completion call")
                log.warning("Worker %s exited without summary (%s, "
                            "turns=%d/%d), launching continuation",
                            worker_id, exit_reason, authoritative_turns, budget)
                continuations = await self.db.bump_continuations(worker_id)
                if continuations > MAX_CONTINUATIONS_PER_WORKER:
                    reason = (f"exited without summary AND exhausted "
                              f"{MAX_CONTINUATIONS_PER_WORKER} "
                              f"continuation cycles; force-failing")
                    log.error("Worker %s: %s", worker_id, reason)
                    await self.db.update_worker_state(
                        worker_id, "failed", summary=reason,
                    )
                    await self._publish_death(
                        worker_id, "failure", reason
                    )
                    return
                # Spawn the continuation subprocess. This runs as a
                # detached task so the monitor can exit cleanly.
                asyncio.create_task(self._launch_continuation(
                    worker_id, authoritative_turns, budget,
                    hit_max_turns=hit_max_turns,
                ))
                return

            if rc != 0:
                w = await self.db.get_worker(worker_id)
                if w and w["state"] == "running":
                    reason = f"subprocess exited rc={rc}"
                    if stream_error:
                        reason += f" (also: {stream_error})"
                    await self.db.update_worker_state(
                        worker_id, "failed", summary=reason,
                    )
                    await self._publish_death(worker_id, "failure", reason)
        except asyncio.CancelledError:
            proc.kill()
            raise
        except Exception:
            log.error("Worker monitor crashed for %s:\n%s",
                       worker_id, traceback.format_exc())
            try:
                w = await self.db.get_worker(worker_id)
                if w and w["state"] == "running":
                    await self.db.update_worker_state(
                        worker_id, "failed",
                        summary="monitor crashed — worker subprocess "
                                "may still be running, check PID",
                    )
                    await self._publish_death(
                        worker_id, "failure",
                        "monitor crashed unexpectedly",
                    )
            except Exception:
                log.exception("Also failed to mark worker failed")

    async def _launch_continuation(self, worker_id: str,
                                     turns_used: int,
                                     budget_used: int,
                                     hit_max_turns: bool = True) -> None:
        """Launch a final-summary subprocess after a worker exits without
        calling a completion tool. Worker gets CONTINUATION_TURN_BUDGET
        extra turns to call mark_task_paused (or mark_task_complete if
        it can wrap up in time).

        hit_max_turns: True if the worker hit its turn budget; False if
        it stopped voluntarily mid-task. Affects the prompt — voluntary
        stops likely mean the worker thought it was done but forgot to
        call mark_task_complete."""
        worker = await self.db.get_worker(worker_id)
        if not worker:
            log.error("Continuation: worker %s not found", worker_id)
            return
        session_id = self._sessions.get(worker_id)
        if not session_id:
            reason = (f"exited without summary ({turns_used}/{budget_used} "
                      f"turns) but no session id available for continuation")
            await self.db.update_worker_state(
                worker_id, "failed", summary=reason,
            )
            await self._publish_death(worker_id, "failure", reason)
            return

        project = await self.db.get_project(worker["project_id"])
        if not project:
            log.error("Continuation: project %s gone", worker["project_id"])
            return

        worker_dir = self.cfg.workspace / "workers" / worker_id
        mcp_path = worker_dir / "mcp.json"

        if hit_max_turns:
            header = (
                f"[MAX_TURNS_REACHED] You used your full turn budget "
                f"({turns_used}/{budget_used}) and the orchestrator "
                f"terminated you. You have {CONTINUATION_TURN_BUDGET} "
                f"extra turns ONLY to summarize where you are.\n\n"
                f"Required: call mark_task_paused with:\n"
                f"  - progress_summary: what you accomplished so far\n"
                f"  - what_remains: concrete description of remaining "
                f"work\n"
                f"  - artifacts: files you wrote (real paths, no "
                f"placeholders)\n"
                f"  - recommended_next_turns: int — how many more turns "
                f"you'd need if continued (be honest; estimate "
                f"generously)\n\n"
                f"If you can finish in the next "
                f"{CONTINUATION_TURN_BUDGET - 1} turns instead, you may "
                f"call mark_task_complete normally — but only if you're "
                f"confident. Otherwise mark_task_paused.\n\n"
                f"DO NOT start new work in this continuation. Report "
                f"state, don't do more."
            )
        else:
            header = (
                f"[EXITED_WITHOUT_SUMMARY] You exited at "
                f"{turns_used}/{budget_used} turns without calling any "
                f"completion tool. The orchestrator does not know what "
                f"you accomplished. You have {CONTINUATION_TURN_BUDGET} "
                f"extra turns to fix this.\n\n"
                f"Most common cause: you finished the task but forgot "
                f"to call mark_task_complete before stopping. If that's "
                f"what happened, call mark_task_complete now with:\n"
                f"  - summary: what you accomplished\n"
                f"  - artifacts: files you wrote (real paths)\n"
                f"  - open_items: anything left for follow-up\n\n"
                f"If you stopped because you got stuck or hit a "
                f"problem, call mark_task_failed with reason and "
                f"what_was_tried.\n\n"
                f"If the work is genuinely incomplete and you'd want "
                f"to continue, call mark_task_paused with progress_"
                f"summary, what_remains, recommended_next_turns, and "
                f"artifacts.\n\n"
                f"Pick exactly ONE of those three tools and call it "
                f"this turn. Do not start new work."
            )

        proc = await self._launch_claude(
            worker_id=worker_id, cwd=project["path"], mcp_path=mcp_path,
            model=worker["model"] or self.cfg.worker_default_model,
            allow_edits=True, max_turns=CONTINUATION_TURN_BUDGET,
            enable_agent_teams=self.cfg.enable_agent_teams,
            prompt=header, resume_session_id=session_id,
            worker_dir=worker_dir,
        )
        self._processes[worker_id] = proc
        self._turn_budget[worker_id] = CONTINUATION_TURN_BUDGET
        self._turn_count[worker_id] = 0
        self._quiet_warned.discard(worker_id)
        await self.db.update_worker_state(worker_id, "running")
        self._tasks[worker_id] = asyncio.create_task(
            self._monitor(worker_id, proc, worker_dir)
        )
        log.info("Worker %s continuation launched pid=%s budget=%d "
                 "(cause=%s)",
                 worker_id, proc.pid, CONTINUATION_TURN_BUDGET,
                 "max_turns" if hit_max_turns else "voluntary_stop")

    async def _monitor_codex(self, worker_id: str,
                              proc: asyncio.subprocess.Process,
                              worker_dir: Path) -> None:
        """Monitor a Codex worker. Differences from Claude monitor:
        - No MCP, so workers can't call mark_task_complete. The daemon
          synthesizes a completion event from the worker's final
          agent_message text and the file_change paths it observed.
        - No session resume support yet. If the worker exits without
          finishing, we don't relaunch a continuation turn — we mark
          it failed. This is a Phase 1 limitation.
        - Codex emits one item per state change; turn count comes from
          counting turn.started events.
        """
        events_file = worker_dir / "events.ndjson"
        last_agent_text: str = ""
        file_changes: set[str] = set()
        saw_failure: bool = False
        error_text: str = ""
        turn_count: int = 0
        try:
            assert proc.stdout is not None
            while True:
                try:
                    line = await proc.stdout.readline()
                except ValueError as e:
                    log.warning("Codex worker %s: stream buffer overflow "
                                "(%s); dropping oversized line",
                                worker_id, e)
                    continue
                if not line:
                    break
                with events_file.open("ab") as f:
                    f.write(line)
                try:
                    msg = json.loads(line)
                except Exception:
                    continue
                mtype = msg.get("type")
                if mtype == "thread.started":
                    tid = msg.get("thread_id")
                    if tid:
                        self._sessions[worker_id] = tid
                elif mtype == "turn.started":
                    turn_count += 1
                    self._turn_count[worker_id] = turn_count
                elif mtype == "turn.completed":
                    # Token usage is in msg.get("usage"). We don't
                    # currently track per-worker token usage; could
                    # surface it through get_worker_status later.
                    pass
                elif mtype == "turn.failed":
                    saw_failure = True
                    err = msg.get("error", {}) or {}
                    error_text = (err.get("message")
                                   or json.dumps(err)
                                   or "(turn failed)")[:500]
                elif mtype == "error":
                    saw_failure = True
                    error_text = (msg.get("message", "")
                                   or "(unspecified codex error)")[:500]
                elif mtype == "item.completed":
                    item = msg.get("item", {}) or {}
                    itype = item.get("type")
                    if itype == "agent_message":
                        txt = item.get("text", "") or ""
                        if txt:
                            last_agent_text = txt
                    elif itype == "file_change":
                        for ch in item.get("changes", []) or []:
                            path = ch.get("path")
                            # Codex creates a 0-byte .codex marker file
                            # in workspace roots; not a real artifact.
                            if path and not path.endswith("/.codex"):
                                file_changes.add(path)
            rc = await proc.wait()
            self._quiet_warned.discard(worker_id)
            log.info("Codex worker %s exited rc=%s turns=%d files=%d",
                     worker_id, rc, turn_count, len(file_changes))

            # Synthesize completion. Don't override if worker was
            # killed externally — its state is already terminal.
            w = await self.db.get_worker(worker_id)
            if not w or w["state"] != "running":
                return

            if rc != 0 or saw_failure:
                reason = error_text or f"codex exited rc={rc}"
                await self.db.update_worker_state(
                    worker_id, "failed",
                    summary=f"(codex failure) {reason[:500]}",
                )
                if self.bus:
                    await self.bus.publish(Event(
                        kind=EventKind.WORKER_TOOL_CALL,
                        payload={
                            "worker_id": worker_id, "kind": "failure",
                            "payload": {
                                "reason": reason,
                                "summary": reason,
                                "synthetic": True,
                                "engine": "codex",
                                "note": ("Codex worker failed; "
                                          "synthesized by daemon from "
                                          "subprocess exit / error event."),
                            },
                        },
                    ))
            else:
                # Treat as success. Final agent_message is the summary;
                # observed file_change paths become artifacts.
                artifacts = sorted(file_changes)
                valid, rejected = _validate_artifacts(artifacts, worker_id)
                summary = (last_agent_text
                            or "(codex worker exited cleanly with no "
                              "final message)")
                await self.db.update_worker_state(
                    worker_id, "completed",
                    summary=summary[:2000],
                )
                if self.bus:
                    await self.bus.publish(Event(
                        kind=EventKind.WORKER_TOOL_CALL,
                        payload={
                            "worker_id": worker_id, "kind": "completion",
                            "payload": {
                                "summary": summary,
                                "artifacts": valid,
                                "rejected_artifacts": rejected,
                                "synthetic": True,
                                "engine": "codex",
                                "note": ("Codex worker completion; "
                                          "synthesized by daemon from "
                                          "final agent_message + "
                                          "observed file_change items."),
                            },
                        },
                    ))
        finally:
            self._processes.pop(worker_id, None)
            self._tasks.pop(worker_id, None)

    async def _publish_death(self, worker_id: str, kind: str,
                              reason: str) -> None:
        if not self.bus:
            return
        await self.bus.publish(Event(
            kind=EventKind.WORKER_TOOL_CALL,
            payload={
                "worker_id": worker_id, "kind": kind,
                "payload": {
                    "reason": reason, "summary": reason,
                    "synthetic": True,
                    "note": ("This event was synthesized by the daemon "
                             "because the worker terminated without calling "
                             "mark_task_complete or mark_task_failed."),
                },
            },
        ))

    async def kill(self, worker_id: str, reason: str) -> bool:
        proc = self._processes.get(worker_id)
        if not proc:
            # Subprocess handle is missing — likely the daemon restarted
            # since this worker was spawned, or it already exited and
            # the entry was cleaned up. Either way, the worker is not
            # something we can kill anymore. Transition the DB state
            # so callers (e.g. sweep_stuck_workers) don't keep retrying
            # on the same stuck row forever.
            w = await self.db.get_worker(worker_id)
            if w and w["state"] in ("running", "exited_no_summary"):
                log.warning(
                    "kill(%s): no subprocess handle in memory; "
                    "marking row killed (reason=%s). This is normal "
                    "if the daemon was restarted while the worker was "
                    "running.", worker_id, reason,
                )
                await self.db.update_worker_state(
                    worker_id, "killed",
                    summary=(f"killed (orphan, no subprocess): {reason}"),
                )
                await self._publish_death(
                    worker_id, "failure",
                    f"killed (orphan, no subprocess): {reason}",
                )
                return True
            return False
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        await self.db.update_worker_state(
            worker_id, "killed", summary=f"killed: {reason}"
        )
        await self._publish_death(worker_id, "failure",
                                    f"killed: {reason}")
        return True

    # --- Worker tool-call handlers ---

    async def handle_report(self, worker_id: str, severity: str,
                             message: str, artifacts: list[str]) -> dict:
        await self.db.record_worker_event(
            worker_id, "report",
            {"severity": severity, "message": message, "artifacts": artifacts},
            severity=severity,
        )
        await self.db.touch_worker(worker_id)
        asyncio.create_task(self._post_heartbeat(
            worker_id, severity, message, artifacts
        ))
        return {"received": True}

    async def _post_heartbeat(self, worker_id: str, severity: str,
                                message: str, artifacts: list[str]) -> None:
        if not self.bot:
            return
        now = asyncio.get_event_loop().time()
        last = self._last_heartbeat.get(worker_id, 0.0)
        if (now - last) < HEARTBEAT_MIN_INTERVAL_S and severity == "info":
            return
        self._last_heartbeat[worker_id] = now

        worker = await self.db.get_worker(worker_id)
        if not worker or not worker.get("thread_id"):
            return

        emoji = {"info": "💬", "warning": "⚠️", "error": "🔴"}.get(severity, "·")
        text = f"{emoji} {message[:500]}"
        if artifacts:
            text += f"\n📎 {', '.join(artifacts[:5])}"
        await self.bot.post_to_thread(int(worker["thread_id"]), text)
        await self.db.touch_worker_heartbeat(worker_id)

    async def handle_decision_request(self, worker_id: str, question: str,
                                       options: list[dict], context: str,
                                       recommendation: str) -> dict:
        await self.db.record_worker_event(
            worker_id, "decision_request",
            {"question": question, "options": options,
             "context": context, "recommendation": recommendation},
        )
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_decisions[worker_id] = fut
        try:
            return await fut
        except Exception:
            self._pending_decisions.pop(worker_id, None)
            raise

    def resolve_decision(self, worker_id: str, chosen_option: str,
                          reasoning: str) -> bool:
        fut = self._pending_decisions.pop(worker_id, None)
        if not fut or fut.done():
            return False
        fut.set_result({"chosen_option": chosen_option, "reasoning": reasoning})
        return True

    async def handle_completion(self, worker_id: str, summary: str,
                                 artifacts: list[str],
                                 open_items: list[str]) -> dict:
        valid_artifacts, rejected = _validate_artifacts(
            artifacts or [], worker_id
        )
        await self.db.update_worker_state(
            worker_id, "completed", summary=summary
        )
        await self.db.record_worker_event(
            worker_id, "completion",
            {"summary": summary, "artifacts": valid_artifacts,
             "rejected_artifacts": rejected, "open_items": open_items},
        )
        worker = await self.db.get_worker(worker_id)
        if worker and worker.get("thread_id") and self.bot:
            text = f"✅ **Completed**\n{summary[:1500]}"
            if valid_artifacts:
                text += f"\n📎 artifacts: {', '.join(valid_artifacts[:10])}"
            if rejected:
                text += (f"\n\n⚠️ Rejected {len(rejected)} invalid artifact "
                         f"entries (not real paths): `{rejected[:3]}`")
            asyncio.create_task(self.bot.post_to_thread(
                int(worker["thread_id"]), text
            ))
        result = {"acknowledged": True}
        if rejected:
            result["rejected_artifacts"] = rejected
            result["rejected_reason"] = (
                "Artifact entries must be absolute paths to existing files. "
                "Placeholder strings like 'report (inline)' are rejected."
            )
        return result

    async def handle_failure(self, worker_id: str, reason: str,
                              what_was_tried: str) -> dict:
        await self.db.update_worker_state(
            worker_id, "failed", summary=f"FAILED: {reason}"
        )
        await self.db.record_worker_event(
            worker_id, "failure",
            {"reason": reason, "what_was_tried": what_was_tried},
        )
        worker = await self.db.get_worker(worker_id)
        if worker and worker.get("thread_id") and self.bot:
            text = (f"❌ **Failed**\n{reason[:1000]}\n\n"
                    f"**What was tried:**\n{what_was_tried[:1000]}")
            asyncio.create_task(self.bot.post_to_thread(
                int(worker["thread_id"]), text
            ))
        return {"acknowledged": True}

    async def handle_paused(self, worker_id: str, progress_summary: str,
                              what_remains: str, artifacts: list[str],
                              recommended_next_turns: int) -> dict:
        """Worker hit max-turns and called mark_task_paused. Marks the
        worker 'paused' and records the structured summary."""
        valid_artifacts, rejected = _validate_artifacts(
            artifacts or [], worker_id
        )
        await self.db.update_worker_state(
            worker_id, "paused",
            summary=f"PAUSED: {progress_summary[:500]}",
        )
        await self.db.record_worker_event(
            worker_id, "paused",
            {
                "progress_summary": progress_summary,
                "what_remains": what_remains,
                "artifacts": valid_artifacts,
                "rejected_artifacts": rejected,
                "recommended_next_turns": int(recommended_next_turns or 0),
            },
        )
        worker = await self.db.get_worker(worker_id)
        if worker and worker.get("thread_id") and self.bot:
            text = (
                f"⏸️ **Paused (hit max-turns)**\n"
                f"**Progress:** {progress_summary[:1000]}\n\n"
                f"**Remaining:** {what_remains[:1000]}\n\n"
                f"**Recommended next turns:** {recommended_next_turns}"
            )
            if valid_artifacts:
                text += f"\n📎 artifacts: {', '.join(valid_artifacts[:10])}"
            if rejected:
                text += (f"\n\n⚠️ Rejected {len(rejected)} invalid "
                         f"artifact entries: `{rejected[:3]}`")
            asyncio.create_task(self.bot.post_to_thread(
                int(worker["thread_id"]), text
            ))
        result = {"acknowledged": True}
        if rejected:
            result["rejected_artifacts"] = rejected
        return result

    # --- Status introspection ---

    async def get_worker_status_detail(self, worker_id: str) -> dict:
        worker = await self.db.get_worker(worker_id)
        if not worker:
            raise ValueError(f"Unknown worker_id: {worker_id}")
        events_file = (self.cfg.workspace / "workers" / worker_id
                        / "events.ndjson")
        scan = _scan_worker_events(events_file)
        return {
            "worker_id": worker_id,
            "project_id": worker.get("project_id"),
            "state": worker["state"],
            "task_preview": (worker.get("task") or "")[:200],
            "spawned_at": worker.get("spawned_at"),
            "ended_at": worker.get("ended_at"),
            "last_activity_at": worker.get("last_activity_at"),
            "last_heartbeat_at": worker.get("last_heartbeat_at"),
            "summary": worker.get("summary"),
            "thread_id": worker.get("thread_id"),
            "turn_count": scan["turn_count"],
            "tool_calls_total": scan["tool_calls_total"],
            "last_tools": scan["last_tools"],
            "last_text_preview": scan["last_text_preview"],
        }

    # --- Periodic maintenance ---

    async def sweep_stuck_workers(self, idle_seconds: int) -> list[str]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=idle_seconds)
        running = await self.db.list_workers(state="running")
        killed = []
        for w in running:
            last_activity = w.get("last_activity_at")
            if not last_activity:
                continue
            try:
                last = datetime.fromisoformat(last_activity)
            except Exception:
                continue
            if last < cutoff:
                log.warning("Stuck worker %s (no activity since %s), killing",
                             w["worker_id"], last_activity)
                await self.kill(
                    w["worker_id"],
                    f"stuck: no activity for {idle_seconds}s"
                )
                killed.append(w["worker_id"])
        return killed

    async def sweep_quiet_workers(self, quiet_seconds: int) -> list[dict]:
        """Find running workers whose last orchestrator-tool-call activity
        is older than quiet_seconds. Returns a list of {worker_id, ...}
        dicts to be turned into WORKER_QUIET events. Won't re-fire for a
        worker already warned in its current run (cleared on exit)."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=quiet_seconds)
        running = await self.db.list_workers(state="running")
        warned: list[dict] = []
        for w in running:
            wid = w["worker_id"]
            if wid in self._quiet_warned:
                continue
            last_activity = w.get("last_activity_at")
            if not last_activity:
                continue
            try:
                last = datetime.fromisoformat(last_activity)
            except Exception:
                continue
            if last >= cutoff:
                continue
            # Build a recent-activity snapshot so OM has signal beyond
            # "is quiet". Read last 5 tool names from events.ndjson if
            # available.
            last_tools = await self._recent_tool_names(wid, n=5)
            proc = self._processes.get(wid)
            pid_alive = proc is not None and proc.returncode is None
            warned.append({
                "worker_id": wid,
                "project_id": w.get("project_id"),
                "thread_id": w.get("thread_id"),
                "task_preview": (w.get("task") or "")[:300],
                "last_activity_at": last_activity,
                "quiet_for_seconds": int(
                    (datetime.now(timezone.utc) - last).total_seconds()
                ),
                "turn_count": self._turn_count.get(wid, 0),
                "turn_budget": self._turn_budget.get(wid, 0),
                "last_tools": last_tools,
                "subprocess_alive": pid_alive,
            })
            self._quiet_warned.add(wid)
            log.info("Worker %s flagged quiet (no activity for %ds)",
                     wid, int((datetime.now(timezone.utc) - last)
                              .total_seconds()))
        return warned

    async def _recent_tool_names(self, worker_id: str, n: int) -> list[str]:
        """Read the last N tool_use names from the worker's events.ndjson."""
        events_file = (self.cfg.workspace / "workers"
                        / worker_id / "events.ndjson")
        if not events_file.exists():
            return []
        names: list[str] = []
        try:
            with events_file.open("rb") as f:
                # Tail-read by reading whole file (worker events files
                # are small, max few MB). For larger ones we'd seek.
                lines = f.readlines()
            for line in reversed(lines):
                try:
                    msg = json.loads(line)
                except Exception:
                    continue
                if msg.get("type") != "assistant":
                    continue
                content = msg.get("message", {}).get("content", []) or []
                for block in content:
                    if block.get("type") == "tool_use":
                        names.append(block.get("name", "?"))
                        if len(names) >= n:
                            return list(reversed(names[-n:]))
        except Exception:
            log.exception("Failed to read events file for %s", worker_id)
        return list(reversed(names))

    async def tail_output(self, worker_id: str, n: int = 20) -> dict:
        """Return recent tool calls + their results, ordered oldest to
        newest. Engine-aware:
        - Claude workers: pairs tool_use + tool_result by tool_use_id.
        - Codex workers: pairs item.started + item.completed by item id,
          for command_execution + file_change items only (not
          agent_message or reasoning).

        Each entry has tool, input_summary, output_summary, is_error,
        in_progress (the most recent entry may have no result yet —
        useful for 'what is the worker waiting on right now')."""
        events_file = (self.cfg.workspace / "workers"
                        / worker_id / "events.ndjson")
        if not events_file.exists():
            return {"entries": [], "note": "no events file (worker "
                                            "may not have started)"}
        worker = await self.db.get_worker(worker_id)
        engine = (worker.get("engine") if worker else "claude") or "claude"
        if engine == "codex":
            return tail_output_codex(events_file, n)
        return tail_output_claude(events_file, n)