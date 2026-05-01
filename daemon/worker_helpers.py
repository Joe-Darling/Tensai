"""Pure helpers for worker output handling.

Used by :class:`daemon.worker_runner.WorkerRunner` and friends. Kept
separate so they're easy to unit-test and don't drag the whole runner
into anything that just wants, say, a tool-output summary.

- ``validate_artifacts`` — accepts the artifact list a worker passes
  to ``mark_task_complete``, splitting it into ``(valid, rejected)``.
  Rejects placeholder strings (``"(inline)"``, ``"see summary"``),
  non-absolute paths, and paths that don't exist.

- ``summarize_input`` / ``summarize_output`` / ``truncate`` — render a
  worker's tool calls compactly so the OM can triage what a worker is
  doing without slurping the full event stream.

- ``scan_worker_events`` — single-pass scan of a worker's
  ``events.ndjson`` to compute turn count, total tool calls, the last
  few tool names, the most recent assistant text, and the last event
  timestamp.

- ``strip_codex_bash_wrapper`` — Codex wraps shell commands as
  ``bash -lc "..."``; this peels that wrapper off so the surfaced
  command is what the worker actually ran.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

# How many tool names to keep on the worker's "recent tool calls" list.
RECENT_TOOLS_TO_KEEP = 5

# Placeholder substrings that signal a worker passed a literal string
# instead of a real artifact path.
ARTIFACT_PLACEHOLDER_MARKERS = (
    "(inline", "inline)", "see summary", "see above",
    "generated-inline", "report (", "report)",
)


def validate_artifacts(artifacts: list[str],
                        worker_id: str) -> tuple[list[str], list[str]]:
    """Split an artifact list into ``(valid, rejected)``. Rejects:

    - non-strings or empty strings
    - placeholder phrases
    - non-absolute paths
    - paths that don't exist on disk

    Logs the rejection list at WARNING so the daemon log shows when a
    worker fluffed its completion."""
    valid: list[str] = []
    rejected: list[str] = []
    for a in artifacts:
        if not isinstance(a, str) or not a.strip():
            rejected.append(str(a))
            continue
        s = a.strip()
        if any(bad in s.lower() for bad in ARTIFACT_PLACEHOLDER_MARKERS):
            rejected.append(s)
            continue
        p = Path(s)
        if not p.is_absolute():
            rejected.append(s)
            continue
        if not p.exists():
            rejected.append(s)
            continue
        valid.append(str(p.resolve()))
    if rejected:
        log.warning(
            "Worker %s: rejected %d invalid artifact entries: %s",
            worker_id, len(rejected), rejected,
        )
    return valid, rejected


def truncate(s: str, head: int, tail: int = 0) -> str:
    """Truncate ``s`` to roughly ``head`` chars from the start,
    optionally keeping ``tail`` chars from the end with an elision
    marker between."""
    if not s:
        return ""
    if len(s) <= head + tail + 20:
        return s
    if tail <= 0:
        return s[:head] + f"... ({len(s) - head} more chars)"
    return (s[:head] + f"\n... ({len(s) - head - tail} chars elided) ...\n"
            + s[-tail:])


def summarize_input(name: str, inp: dict) -> str:
    """Compact one-line summary of a tool's input. Tailored for the
    common Claude Code tools so the OM gets useful triage info rather
    than a JSON dump."""
    if not isinstance(inp, dict):
        return truncate(str(inp), 200)
    if name == "Read":
        path = inp.get("file_path") or inp.get("path") or "?"
        offset = inp.get("offset")
        limit = inp.get("limit")
        rng = ""
        if offset is not None or limit is not None:
            rng = f" lines {offset or 0}–{(offset or 0) + (limit or 0)}"
        return f"{path}{rng}"
    if name in ("Write", "create_file"):
        path = inp.get("file_path") or inp.get("path") or "?"
        body = inp.get("content") or inp.get("file_text") or ""
        line_count = body.count("\n") + 1 if body else 0
        return f"{path} ({line_count} lines)"
    if name in ("Edit", "str_replace"):
        path = inp.get("file_path") or inp.get("path") or "?"
        old = inp.get("old_string") or inp.get("old_str") or ""
        return f"{path} (replacing ~{len(old)} chars)"
    if name in ("Bash", "bash_tool"):
        cmd = inp.get("command") or ""
        return truncate(cmd, 200)
    if name == "Grep":
        pattern = inp.get("pattern", "")
        path = inp.get("path", "")
        return f"pattern={pattern!r} in {path or '.'}"
    if name == "Glob":
        return inp.get("pattern", "?")
    if name == "WebSearch":
        return inp.get("query", "?")
    if name == "WebFetch":
        return inp.get("url", "?")
    if name == "TodoWrite":
        todos = inp.get("todos") or []
        return f"{len(todos)} todos"
    return truncate(json.dumps(inp, default=str), 200)


def summarize_output(text: str, is_error: bool) -> str:
    """Trim a tool result to something readable. Errors get more
    context (head + tail) since the OM cares why something failed;
    successes get head-only."""
    if not text:
        return "(empty result)"
    prefix = "ERROR: " if is_error else ""
    if is_error:
        return prefix + truncate(text, 400, tail=200)
    return prefix + truncate(text, 500)


def strip_codex_bash_wrapper(cmd: str) -> str:
    """Codex commands are typically wrapped as ``/bin/bash -lc "..."``.
    Strip the wrapper so the surfaced command is what the worker
    actually ran."""
    if not cmd:
        return cmd
    for prefix in ("/bin/bash -lc ", "bash -lc "):
        if cmd.startswith(prefix):
            inner = cmd[len(prefix):].strip()
            if (len(inner) >= 2 and inner[0] == inner[-1]
                    and inner[0] in ('"', "'")):
                return inner[1:-1]
            return inner
    return cmd


def scan_worker_events(events_file: Path) -> dict:
    """Single-pass summary of a worker's ``events.ndjson``. Returns
    turn count, total tool calls, last few tool names, last assistant
    text preview, and last event timestamp.

    Resilient to malformed lines and missing files (returns sensible
    zero values)."""
    turn_count = 0
    tool_calls_total = 0
    all_tool_names: list[str] = []
    last_text_preview = ""
    last_event_at: str | None = None
    try:
        with events_file.open("rb") as f:
            for raw in f:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                ts = msg.get("timestamp")
                if ts:
                    last_event_at = ts
                if msg.get("type") == "assistant":
                    content = msg.get("message", {}).get("content", [])
                    if content:
                        turn_count += 1
                    for block in content:
                        bt = block.get("type")
                        if bt == "tool_use":
                            name = block.get("name", "?")
                            tool_calls_total += 1
                            all_tool_names.append(name)
                        elif bt == "text":
                            text = block.get("text") or ""
                            if text.strip():
                                last_text_preview = text[:300]
    except FileNotFoundError:
        pass
    except Exception:
        log.exception("Failed to scan events file %s", events_file)
    last_tools = all_tool_names[-RECENT_TOOLS_TO_KEEP:]
    last_tools = [
        n.replace("mcp__orchestrator-worker__", "")
        for n in last_tools
    ]
    return {
        "turn_count": turn_count,
        "tool_calls_total": tool_calls_total,
        "last_tools": last_tools,
        "last_text_preview": last_text_preview,
        "last_event_at": last_event_at,
    }
