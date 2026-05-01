"""Format rate-limit observations for the OM's get_orchestrator_stats.

The daemon stores raw ``rate_limit_event`` rows from the claude CLI's
stream-json output. This module reshapes them into the JSON the OM
sees, including per-type "minutes until reset" math and an explicit
``available`` flag when no event has been observed yet.

The CLI's ``rate_limit_event`` reports window-reset time and
allow/warn/reject status — it does NOT include % of quota consumed.
That caveat is surfaced in the response so the OM doesn't infer more
than is there.
"""
from __future__ import annotations

from datetime import datetime, timezone

from .db import DB


async def format_rate_limits(db: DB) -> dict:
    rows = await db.get_rate_limits()
    if not rows:
        return {
            "available": False,
            "note": ("No rate-limit events observed yet. One will be "
                     "captured on the next OM turn or worker spawn."),
        }
    out: dict[str, dict] = {}
    now = datetime.now(timezone.utc)
    for row in rows:
        rl_type = row["rate_limit_type"]
        resets_at = row.get("resets_at")
        resets_in_minutes: int | None = None
        resets_at_iso: str | None = None
        reset_passed = False
        if resets_at:
            reset_dt = datetime.fromtimestamp(
                int(resets_at), tz=timezone.utc
            )
            resets_at_iso = reset_dt.isoformat()
            delta = reset_dt - now
            resets_in_minutes = int(delta.total_seconds() // 60)
            reset_passed = delta.total_seconds() < 0
        out[rl_type] = {
            "status": row.get("status"),
            "resets_at": resets_at_iso,
            "resets_in_minutes": resets_in_minutes,
            "reset_already_passed": reset_passed,
            "is_using_overage": bool(row.get("is_using_overage")),
            "overage_status": row.get("overage_status"),
            "observed_at": row.get("observed_at"),
            "observed_by": row.get("observed_by"),
        }
    return {
        "available": True,
        "by_type": out,
        "note": ("rate_limit_event only reports window-reset time and "
                 "allow/warn/reject status; it does NOT report the % "
                 "of quota consumed. For that run /usage interactively."),
    }
