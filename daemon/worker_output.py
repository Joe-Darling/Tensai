"""Engine-aware tail of a worker's recent tool calls.

The OM uses ``tail_worker_output`` to triage what a worker has been
doing without scanning the entire ``events.ndjson``. Two parsers, one
per engine, since Claude and Codex emit different stream-json shapes:

- **Claude** pairs ``tool_use`` (assistant message) with
  ``tool_result`` (user message) by ``tool_use_id``. Orchestrator MCP
  calls are filtered out — they're worker→OM chatter, not the
  worker's actual environment work.

- **Codex** emits ``item.started`` and ``item.completed`` events with
  a nested item carrying its own type and id. We pair them by id and
  surface only ``command_execution`` and ``file_change`` items
  (skipping ``agent_message`` and ``reasoning``, which aren't
  "tools").

Both functions return ``{entries: [...], total_tool_calls_in_log,
showing_last}`` with each entry having: ``tool``, ``input_summary``,
``output_summary``, ``is_error``, ``in_progress``. The
``in_progress`` flag is true for the most recent entry if its result
hasn't been observed yet — useful for "what is the worker waiting on
right now?".
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from .worker_helpers import (
    strip_codex_bash_wrapper,
    summarize_input,
    summarize_output,
    truncate,
)

log = logging.getLogger(__name__)

TAIL_OUTPUT_DEFAULT = 20
TAIL_OUTPUT_CAP = 50


def _read_events(events_file: Path) -> list[bytes] | None:
    try:
        with events_file.open("rb") as f:
            return f.readlines()
    except Exception:
        log.exception("Failed to read events file %s", events_file)
        return None


def tail_output_claude(events_file: Path, n: int) -> dict:
    lines = _read_events(events_file)
    if lines is None:
        return {"entries": [], "note": "couldn't read events file"}
    tool_uses: dict[str, dict] = {}
    tool_results: dict[str, dict] = {}
    ordered_use_ids: list[str] = []
    for line in lines:
        try:
            msg = json.loads(line)
        except Exception:
            continue
        mtype = msg.get("type")
        if mtype == "assistant":
            for block in msg.get("message", {}).get("content", []) or []:
                if block.get("type") != "tool_use":
                    continue
                name = block.get("name", "?")
                # Skip orchestrator MCP calls; they go to the daemon
                # not the worker's environment.
                if name.startswith("mcp__orchestrator-"):
                    continue
                use_id = block.get("id")
                if not use_id:
                    continue
                tool_uses[use_id] = {
                    "name": name,
                    "input": block.get("input") or {},
                }
                ordered_use_ids.append(use_id)
        elif mtype == "user":
            for block in msg.get("message", {}).get("content", []) or []:
                if block.get("type") != "tool_result":
                    continue
                use_id = block.get("tool_use_id")
                if not use_id:
                    continue
                raw = block.get("content")
                text_chunks: list[str] = []
                if isinstance(raw, str):
                    text_chunks.append(raw)
                elif isinstance(raw, list):
                    for sub in raw:
                        if (isinstance(sub, dict)
                                and sub.get("type") == "text"):
                            text_chunks.append(sub.get("text", ""))
                tool_results[use_id] = {
                    "is_error": bool(block.get("is_error")),
                    "text": "\n".join(text_chunks),
                }
    cap = max(1, min(int(n or TAIL_OUTPUT_DEFAULT), TAIL_OUTPUT_CAP))
    relevant = ordered_use_ids[-cap:]
    entries: list[dict] = []
    for use_id in relevant:
        use = tool_uses[use_id]
        res = tool_results.get(use_id)
        entries.append({
            "tool": use["name"],
            "input_summary": summarize_input(use["name"], use["input"]),
            "output_summary": (summarize_output(res["text"], res["is_error"])
                                if res else None),
            "is_error": bool(res and res["is_error"]),
            "in_progress": res is None,
        })
    return {
        "entries": entries,
        "total_tool_calls_in_log": len(ordered_use_ids),
        "showing_last": len(entries),
    }


def tail_output_codex(events_file: Path, n: int) -> dict:
    lines = _read_events(events_file)
    if lines is None:
        return {"entries": [], "note": "couldn't read events file"}
    item_starts: dict[str, dict] = {}
    item_completes: dict[str, dict] = {}
    ordered_ids: list[str] = []
    for line in lines:
        try:
            msg = json.loads(line)
        except Exception:
            continue
        mtype = msg.get("type")
        if mtype not in ("item.started", "item.completed"):
            continue
        item = msg.get("item", {}) or {}
        itype = item.get("type")
        if itype not in ("command_execution", "file_change"):
            continue
        iid = item.get("id")
        if not iid:
            continue
        if mtype == "item.started":
            if iid not in item_starts:
                ordered_ids.append(iid)
            item_starts[iid] = item
        else:
            # item.completed — overrides start info for finalized data
            # (output, exit_code, etc).
            if iid not in item_starts and iid not in item_completes:
                ordered_ids.append(iid)
            item_completes[iid] = item
    cap = max(1, min(int(n or TAIL_OUTPUT_DEFAULT), TAIL_OUTPUT_CAP))
    relevant = ordered_ids[-cap:]
    entries: list[dict] = []
    for iid in relevant:
        completed = item_completes.get(iid)
        started = item_starts.get(iid)
        item = completed or started
        if not item:
            continue
        itype = item.get("type")
        in_progress = (completed is None
                        and (started or {}).get("status") != "completed")
        if itype == "command_execution":
            entries.append(_codex_command_entry(item, completed,
                                                  in_progress))
        elif itype == "file_change":
            entries.append(_codex_file_change_entry(item, completed,
                                                     in_progress))
    return {
        "entries": entries,
        "total_tool_calls_in_log": len(ordered_ids),
        "showing_last": len(entries),
    }


def _codex_command_entry(item: dict, completed: dict | None,
                          in_progress: bool) -> dict:
    cmd = item.get("command") or ""
    cmd = strip_codex_bash_wrapper(cmd)
    output = item.get("aggregated_output") or ""
    exit_code = item.get("exit_code")
    is_error = exit_code is not None and exit_code != 0
    output_summary = None
    if completed is not None:
        if exit_code is not None:
            output_summary = (f"exit={exit_code} | "
                                + summarize_output(output, is_error))
        else:
            output_summary = summarize_output(output, is_error)
    return {
        "tool": "Bash",
        "input_summary": truncate(cmd, 200),
        "output_summary": output_summary,
        "is_error": is_error,
        "in_progress": in_progress,
    }


def _codex_file_change_entry(item: dict, completed: dict | None,
                               in_progress: bool) -> dict:
    changes = item.get("changes") or []
    paths = ", ".join(
        f"{c.get('kind', '?')}: {c.get('path', '?')}"
        for c in changes
    )
    return {
        "tool": "FileChange",
        "input_summary": truncate(paths, 200),
        "output_summary": (item.get("status") if completed else None),
        "is_error": False,
        "in_progress": in_progress,
    }
