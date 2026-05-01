"""OM runner."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

from .config import DaemonConfig
from .db import DB, utcnow
from .bus import Event, EventKind
from .discord_bot import BotsManager
from .event_format import format_event, format_events_for_seed

log = logging.getLogger(__name__)

OM_SUBPROCESS_TIMEOUT_S = 300


# Backwards-compat alias for callers that still use the legacy name.
_format_events_for_seed = format_events_for_seed


class OMRunner:
    def __init__(self, cfg: DaemonConfig, db: DB, bot: BotsManager):
        self.cfg = cfg
        self.db = db
        self.bot = bot
        self.current_session_id: str | None = None
        self._warned_sessions: set[str] = set()
        # Sessions that have already had a COMPACTION_REQUIRED event
        # published. Prevents stacking duplicate compaction events when
        # multiple turns occur while tokens are still over the hard
        # threshold (the published event hasn't been consumed yet, so
        # current_session_id hasn't reset and should_compact keeps
        # returning True).
        self._compaction_queued_sessions: set[str] = set()
        self._mcp_config_path = cfg.om_workspace_dir / "om_mcp.json"
        # Reset on every turn and read by main.py's dispatcher to decide
        # whether to fire a REPLY_DROPPED_CHECK nudge.
        self.last_turn_tool_calls: list[str] = []
        self.last_turn_succeeded: bool = False
        # Rate-limit warnings dedup: maps (rl_type, resets_at_iso) ->
        # warning_emitted (always True; presence is the signal). This
        # prevents the spam where every OM turn re-warns about the same
        # rate-limit window. New windows (different resets_at) emit
        # again. Pruned opportunistically when entries become stale.
        self._rate_limit_warned: dict[tuple[str, str], bool] = {}

    async def setup(self) -> None:
        self.cfg.om_workspace_dir.mkdir(parents=True, exist_ok=True)
        (self.cfg.workspace / "logs").mkdir(parents=True, exist_ok=True)
        mcp_config = {
            "mcpServers": {
                "orchestrator-om": {
                    "type": "stdio",
                    "command": sys.executable,
                    "args": ["-m", "mcp_servers.om_tools"],
                    "cwd": str(self.cfg.workspace),
                    "env": {
                        "ORCH_IPC_SOCKET": str(self.cfg.ipc_socket),
                        "PYTHONPATH": str(self.cfg.workspace),
                    },
                }
            }
        }
        self._mcp_config_path.write_text(json.dumps(mcp_config, indent=2))
        log.info("OM MCP config written to %s", self._mcp_config_path)
        self._render_om_claude_md()

    def _render_om_claude_md(self) -> None:
        """Render om-workspace/CLAUDE.md from CLAUDE.md.template,
        substituting the configured user_name. The rendered file is
        what the OM subprocess loads (CLAUDE.md is the convention
        Claude Code reads on startup); the template is what's
        source-controlled.

        Written every daemon startup so config changes propagate
        without manual intervention. Skips silently if no template
        is present — useful for advanced setups that prefer to
        author CLAUDE.md directly."""
        template_path = self.cfg.om_workspace_dir / "CLAUDE.md.template"
        rendered_path = self.cfg.om_workspace_dir / "CLAUDE.md"
        if not template_path.exists():
            log.info(
                "No CLAUDE.md.template at %s; leaving CLAUDE.md as-is",
                template_path,
            )
            return
        rendered = (template_path.read_text()
                    .replace("{{user_name}}", self.cfg.user_name))
        rendered_path.write_text(rendered)
        log.info("Rendered OM CLAUDE.md from template (user_name=%r)",
                 self.cfg.user_name)

    async def process_event(self, event: Event) -> None:
        try:
            await self._process_event_inner(event)
        except Exception:
            log.error("process_event crashed:\n%s", traceback.format_exc())
            raise

    async def _process_event_inner(self, event: Event) -> None:
        channel_id = self._channel_for_event(event)
        channel = self.bot.get_channel(channel_id) if channel_id else None
        typing_ctx = channel.typing() if channel else contextlib.nullcontext()
        async with typing_ctx:
            await self._run_claude_subprocess(event)

    def _channel_for_event(self, event: Event) -> int | None:
        p = event.payload
        if event.kind is EventKind.DISCORD_MESSAGE:
            cid = p.get("channel_id")
            if cid:
                return int(cid)
        if event.kind is EventKind.REPLY_DROPPED_CHECK:
            cid = p.get("original_channel_id")
            if cid:
                return int(cid)
        return self.cfg.orchestrator_channel_id

    def _now_preamble(self) -> str:
        now = datetime.now().astimezone()
        return f"[NOW: {now.strftime('%A, %B %d, %Y at %I:%M %p %Z')}]\n"

    def _prune_rate_limit_warned(self) -> None:
        """Drop dedup entries whose reset time is already in the past.
        These windows have closed; if the rate limit comes back for a
        new window, a fresh warning is appropriate. Bounded — keeps
        the dict from accreting one entry per (type, window) over a
        long-running session."""
        now_iso = datetime.now(timezone.utc).isoformat()
        stale = [k for k in self._rate_limit_warned
                  if k[1] and k[1] < now_iso]
        for k in stale:
            self._rate_limit_warned.pop(k, None)

    async def _safe_update_rate_limit(self, info: dict) -> None:
        """Fire-and-forget wrapper around db.update_rate_limit. The
        rate_limits table is informational; if a write fails we log
        and move on rather than letting an unhandled task exception
        spam the daemon log. With busy_timeout in place, real failures
        here should be rare."""
        try:
            await self.db.update_rate_limit(info, observed_by="om")
        except Exception:
            log.exception("update_rate_limit failed; continuing")

    async def _run_claude_subprocess(self, event: Event) -> None:
        started_at = utcnow()
        session_before = self.current_session_id
        is_compaction = event.kind is EventKind.COMPACTION_REQUIRED
        # Reset turn-tracking state; will be updated at end if successful
        self.last_turn_tool_calls = []
        self.last_turn_succeeded = False

        base_prompt = (self._now_preamble()
                        + format_event(event, self.cfg.user_name))

        if self.current_session_id is None:
            summary = await self.db.latest_closed_session_summary()
            recent = await self.db.recent_events(limit=60)
            event_block = _format_events_for_seed(recent)

            parts = []
            if summary:
                parts.append(
                    "[COMPACTION_SUMMARY] The following is a summary of "
                    "your previous session, carried forward after "
                    "compaction. Treat it as your working memory of "
                    "prior events.\n\n" + summary
                )
            if event_block:
                parts.append(
                    "[RECENT EVENTS] Durable log of recent events, "
                    "preserved independent of your session. Use these "
                    "to double-check anything the summary might have "
                    "missed.\n\n" + event_block
                )
            if parts:
                prompt = "\n\n---\n\n".join(parts) + "\n\n---\n\n" + base_prompt
                log.info("Seeded fresh session with summary=%d chars, "
                         "events=%d chars",
                         len(summary) if summary else 0, len(event_block))
            else:
                prompt = base_prompt
        else:
            prompt = base_prompt

        if is_compaction and self.current_session_id:
            await self.db.set_session_state(self.current_session_id,
                                             "compacting")

        args = [
            "claude", "-p",
            "--model", self.cfg.om_model,
            "--mcp-config", str(self._mcp_config_path),
            "--allowedTools", "mcp__orchestrator-om,Read,Glob",
            "--permission-mode", "acceptEdits",
            "--output-format", "stream-json",
            "--verbose",
        ]
        if self.current_session_id:
            args += ["--resume", self.current_session_id]
        args.append(prompt)

        log.info("OM invocation: session=%s event=%s",
                 self.current_session_id, event.kind.value)

        stamp = int(time.time())
        stderr_path = self.cfg.workspace / "logs" / f"om-stderr-{stamp}.log"
        stdout_path = self.cfg.workspace / "logs" / f"om-stdout-{stamp}.log"

        with open(stderr_path, "wb") as stderr_fh:
            proc = await asyncio.create_subprocess_exec(
                *args,
                cwd=str(self.cfg.om_workspace_dir),
                stdout=asyncio.subprocess.PIPE,
                stderr=stderr_fh,
                env=self._subprocess_env(),
                limit=10 * 1024 * 1024,
            )
            log.info("OM subprocess pid=%s stderr=%s", proc.pid, stderr_path)

        tool_calls: list[str] = []
        input_tokens = 0
        output_tokens = 0
        new_session_id: str | None = self.current_session_id
        saw_result = False

        with open(stdout_path, "wb") as stdout_fh:
            assert proc.stdout is not None

            async def read_stdout():
                nonlocal tool_calls, input_tokens, output_tokens
                nonlocal new_session_id, saw_result
                assert proc.stdout is not None
                async for line in proc.stdout:
                    stdout_fh.write(line)
                    try:
                        msg = json.loads(line)
                    except Exception:
                        continue
                    kind = msg.get("type")
                    if kind == "system" and msg.get("subtype") == "init":
                        sid = msg.get("session_id")
                        if sid:
                            new_session_id = sid
                    elif kind == "rate_limit_event":
                        info = msg.get("rate_limit_info", {}) or {}
                        if info.get("rateLimitType"):
                            asyncio.create_task(
                                self._safe_update_rate_limit(info)
                            )
                        status = info.get("status", "unknown")
                        if status != "allowed":
                            rl_type = info.get("rateLimitType", "unknown")
                            reset_ts = info.get("resetsAt")
                            reset_iso = ""
                            reset_str = ""
                            if reset_ts:
                                reset_iso = datetime.fromtimestamp(
                                    reset_ts, timezone.utc
                                ).isoformat()
                                reset_str = f" (resets {reset_iso})"
                            # Dedup key: same window of the same type
                            # only warns once per session lifetime. A
                            # new resets_at means a new window, which
                            # warrants a fresh warning.
                            dedup_key = (rl_type, reset_iso)
                            if dedup_key not in self._rate_limit_warned:
                                self._rate_limit_warned[dedup_key] = True
                                # Prune entries whose reset has already
                                # passed — they can't fire again.
                                self._prune_rate_limit_warned()
                                asyncio.create_task(self.bot.system_post(
                                    f"⚠️ Rate limit status=`{status}` "
                                    f"type=`{rl_type}`{reset_str}. OM may "
                                    f"degrade or fail until reset."
                                ))
                            log.warning("rate limit non-allowed: %s", info)
                    elif kind == "assistant":
                        for block in msg.get("message", {}).get("content", []):
                            if block.get("type") == "tool_use":
                                tool_calls.append(block.get("name", "?"))
                    elif kind == "result":
                        saw_result = True
                        sid = msg.get("session_id")
                        if sid:
                            new_session_id = sid
                        usage = msg.get("usage", {})
                        input_tokens = int(usage.get("input_tokens", 0) or 0)
                        output_tokens = int(usage.get("output_tokens", 0) or 0)

            try:
                await asyncio.wait_for(read_stdout(),
                                        timeout=OM_SUBPROCESS_TIMEOUT_S)
            except asyncio.TimeoutError:
                log.error("OM subprocess timed out after %ss, killing",
                          OM_SUBPROCESS_TIMEOUT_S)
                proc.kill()
                await proc.wait()
                log.error("stderr head:\n%s", _head(stderr_path, 2000))
                return

            rc = await proc.wait()

        ended_at = utcnow()

        if rc != 0:
            log.error("OM subprocess rc=%s\n"
                      "--- stderr head ---\n%s\n"
                      "--- stdout head ---\n%s",
                      rc, _head(stderr_path, 2000), _head(stdout_path, 2000))
            return

        if not saw_result:
            log.error("OM rc=0 but no `result` message.\n"
                      "stdout head:\n%s\nstderr head:\n%s",
                      _head(stdout_path, 2000), _head(stderr_path, 2000))
            return

        log.info("OM turn done: tool_calls=%s tokens=%d+%d session=%s",
                 tool_calls, input_tokens, output_tokens,
                 (new_session_id or "?")[:8])

        # Record for reply-dropped detection in main.py
        self.last_turn_tool_calls = list(tool_calls)
        self.last_turn_succeeded = True

        finalized_during_turn = (
            session_before is not None and self.current_session_id is None
        )

        if finalized_during_turn:
            log.info("Session was closed mid-turn via finalize_compaction; "
                     "not restoring old session id.")
            if session_before:
                await self.db.record_om_turn(
                    session_before, event.kind.value,
                    json.dumps(event.payload)[:500],
                    input_tokens, output_tokens, tool_calls,
                    started_at, ended_at,
                )
            return

        if new_session_id and new_session_id != self.current_session_id:
            await self.db.start_om_session(new_session_id)
            self.current_session_id = new_session_id

        if self.current_session_id:
            await self.db.record_om_turn(
                self.current_session_id, event.kind.value,
                json.dumps(event.payload)[:500],
                input_tokens, output_tokens, tool_calls,
                started_at, ended_at,
            )

        if is_compaction and self.current_session_id:
            sid_before = self.current_session_id
            sess_summary = await self.db.get_compaction_summary(sid_before)
            if sess_summary is None:
                log.warning(
                    "Compaction turn completed without finalize_compaction. "
                    "Force-closing session %s. Tools called: %s",
                    sid_before[:8], tool_calls,
                )
                fallback = ("[Compaction summary missing — OM failed to call "
                            "finalize_compaction. Historical context "
                            "unavailable.]")
                await self.db.save_compaction_summary(sid_before, fallback)
                await self.db.close_session(sid_before)
                self.current_session_id = None
                asyncio.create_task(self.bot.system_post(
                    "⚠️ Compaction ran but OM didn't call finalize_compaction. "
                    "Session force-closed. Next turn starts with minimal context."
                ))

    def _subprocess_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env.pop("ANTHROPIC_API_KEY", None)
        if self.cfg.enable_agent_teams:
            env["CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS"] = "1"
        return env

    async def should_compact(self) -> bool:
        if not self.current_session_id:
            return False
        # Don't republish if this session already has a COMPACTION_REQUIRED
        # in flight (queued or being processed). The flag is cleared when
        # finalize_compaction runs, the post-turn force-close runs, or the
        # session is otherwise reset to None.
        if self.current_session_id in self._compaction_queued_sessions:
            return False
        tokens = await self.db.current_session_tokens(self.current_session_id)
        return tokens >= self.cfg.max_session_tokens

    def mark_compaction_queued(self) -> None:
        """Record that a COMPACTION_REQUIRED event has been published
        for the current session, so should_compact won't republish it
        on subsequent turns until compaction resolves."""
        if self.current_session_id:
            self._compaction_queued_sessions.add(self.current_session_id)

    async def should_warn_compaction(self) -> bool:
        if not self.current_session_id:
            return False
        if self.current_session_id in self._warned_sessions:
            return False
        tokens = await self.db.current_session_tokens(self.current_session_id)
        if tokens < self.cfg.compaction_warn_tokens:
            return False
        if tokens >= self.cfg.max_session_tokens:
            return False
        self._warned_sessions.add(self.current_session_id)
        return True