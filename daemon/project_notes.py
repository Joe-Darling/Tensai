"""Project-context loading and project-note persistence.

Project notes are flat-file markdown blocks under
``om-workspace/notes/<project_id>.md``, formatted as::

    ### [<category>] <iso-timestamp>
    <body>

This module reads them, writes new ones (with optional in-place
replacement of the latest entry of a given category), and assembles
the broader ``load_project_context`` response.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from .config import DaemonConfig
from .db import DB

NOTE_PATTERN = re.compile(
    r"^### \[(\w+)\] (.+?)$(.*?)(?=^### \[|\Z)",
    re.MULTILINE | re.DOTALL,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_notes(text: str) -> list[dict]:
    entries = []
    for m in NOTE_PATTERN.finditer(text):
        entries.append({
            "category": m.group(1),
            "timestamp": m.group(2).strip(),
            "body": m.group(3).strip(),
        })
    return entries


async def load_project_context(cfg: DaemonConfig, db: DB,
                                project_id: str, include: list[str],
                                notes_limit: int,
                                notes_since: str | None,
                                workers_limit: int = 10) -> dict:
    project = await db.get_project(project_id)
    if not project:
        raise ValueError(f"unknown project: {project_id}")
    result: dict = {}

    if "claude_md" in include:
        project_path = Path(project["path"])
        cmd_path = project_path / "CLAUDE.md"
        result["claude_md"] = cmd_path.read_text() if cmd_path.exists() else ""

    if "notes" in include:
        notes_path = (cfg.om_workspace_dir / "notes"
                      / f"{project_id}.md")
        if notes_path.exists():
            entries = parse_notes(notes_path.read_text())
            if notes_since:
                entries = [e for e in entries
                            if e["timestamp"] >= notes_since]
            if notes_limit:
                entries = entries[-notes_limit:]
            result["notes"] = entries
            result["notes_total"] = len(entries)
        else:
            result["notes"] = []
            result["notes_total"] = 0

    if "workers" in include:
        result["active_workers"] = await db.list_workers(
            project_id=project_id, state="running"
        )
        cap = max(1, min(int(workers_limit or 10), 50))
        recent = (await db.list_workers(project_id=project_id))[:cap]
        # Enrich each recent worker with its completion artifacts /
        # rejected_artifacts / open_items so OM can answer "what has a
        # worker produced on this project lately" without needing the
        # (lossy) compaction summary or the (start-of-session-only)
        # event log.
        enriched: list[dict] = []
        for w in recent:
            row = dict(w)
            det = await db.get_worker_completion_details(w["worker_id"])
            row["artifacts"] = det["artifacts"]
            row["rejected_artifacts"] = det["rejected_artifacts"]
            row["open_items"] = det["open_items"]
            enriched.append(row)
        result["recent_workers"] = enriched

    return result


def save_project_note(cfg: DaemonConfig, project_id: str, note: str,
                        category: str, replace_latest: bool) -> None:
    notes_dir = cfg.om_workspace_dir / "notes"
    notes_dir.mkdir(parents=True, exist_ok=True)
    f = notes_dir / f"{project_id}.md"

    if replace_latest and f.exists():
        text = f.read_text()
        entries = parse_notes(text)
        last_of_cat = None
        for i in range(len(entries) - 1, -1, -1):
            if entries[i]["category"] == category:
                last_of_cat = i
                break
        if last_of_cat is not None:
            entries[last_of_cat] = {
                "category": category,
                "timestamp": _now(),
                "body": note,
            }
            rebuilt = []
            for e in entries:
                rebuilt.append(
                    f"### [{e['category']}] {e['timestamp']}\n{e['body']}"
                )
            f.write_text("\n\n".join(rebuilt) + "\n")
            return

    with f.open("a") as fh:
        fh.write(f"\n### [{category}] {_now()}\n{note}\n")
