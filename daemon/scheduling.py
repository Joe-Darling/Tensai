"""Helpers for OM-scheduled follow-up events.

Two pieces:

- ``parse_fire_at`` — accepts either ``fire_at_iso`` or
  ``delay_minutes`` and returns a normalized ISO-UTC string. Rejects
  obviously bad inputs (negative, more than 30 days out, less than
  ~30 seconds out).

- ``eval_schedule_condition`` — evaluates the optional ``condition``
  attached to a scheduled event at firing time. Returns
  ``(should_fire, drop_reason)`` so callers can audit-trail dropped
  events as cancellations rather than fires.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from .db import DB

log = logging.getLogger(__name__)

MAX_FUTURE_DAYS = 30
MIN_FUTURE_SECONDS = 30

# Cap on simultaneously-pending scheduled events. The OM can otherwise
# DoS itself by piling up reminders faster than it consumes them.
MAX_PENDING_SCHEDULED_PER_SESSION = 50


def parse_fire_at(fire_at_iso: str | None,
                   delay_minutes: float | None) -> str:
    """Returns ISO-UTC fire time. Exactly one of the two args must be
    given. Rejects zero/negative and absurdly-far-future values."""
    now = datetime.now(timezone.utc)
    if fire_at_iso and delay_minutes is not None:
        raise ValueError(
            "Pass EITHER fire_at_iso OR delay_minutes, not both."
        )
    if fire_at_iso:
        try:
            fire_dt = datetime.fromisoformat(fire_at_iso)
            if fire_dt.tzinfo is None:
                fire_dt = fire_dt.replace(tzinfo=timezone.utc)
            else:
                fire_dt = fire_dt.astimezone(timezone.utc)
        except ValueError as e:
            raise ValueError(f"bad fire_at_iso: {e}")
    elif delay_minutes is not None:
        if delay_minutes <= 0:
            raise ValueError("delay_minutes must be > 0")
        fire_dt = now + timedelta(minutes=float(delay_minutes))
    else:
        raise ValueError("Must pass fire_at_iso or delay_minutes")

    max_future = now + timedelta(days=MAX_FUTURE_DAYS)
    if fire_dt > max_future:
        raise ValueError("fire_at is >30 days in the future; rejected")
    if fire_dt <= now + timedelta(seconds=MIN_FUTURE_SECONDS):
        raise ValueError(
            "fire_at must be at least ~30 seconds in the future"
        )
    return fire_dt.isoformat()


async def eval_schedule_condition(db: DB, condition: dict,
                                    item: dict) -> tuple[bool, str]:
    """Evaluate a scheduled-event condition. Returns
    ``(should_fire, drop_reason)``. The reason is human-readable text
    used as the cancel reason when the event is dropped.

    Supported condition types:

    - ``worker_not_reported``: fire only if ``worker_id`` has had no
      activity since the schedule was created. Useful for "check on
      worker if it's silent for N min."
    """
    ctype = condition.get("type")
    if ctype == "worker_not_reported":
        worker_id = condition.get("worker_id")
        if not worker_id:
            return True, "no worker_id specified, firing"
        worker = await db.get_worker(worker_id)
        if not worker:
            return True, "worker gone"
        if worker.get("state") in ("completed", "failed", "killed"):
            return True, f"worker is {worker['state']}"
        scheduled_at = item.get("created_at")
        last_act = worker.get("last_activity_at")
        if scheduled_at and last_act and last_act > scheduled_at:
            return False, (
                f"worker {worker_id} reported activity "
                f"({last_act}) after schedule was created "
                f"({scheduled_at})"
            )
        return True, "worker still silent"

    log.warning(
        "Unknown schedule condition type %r; firing anyway", ctype,
    )
    return True, f"unknown condition type {ctype!r}"
