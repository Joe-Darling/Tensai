"""Low-level Discord posting primitives.

The OM bot and the daemon bot share the same primitives for sending
text and files; the dual-bot logic lives in
:class:`daemon.discord_bot.BotsManager` which picks a primary client
and an optional fallback. These helpers are the actual Discord API
calls.

All text posting auto-chunks to fit Discord's ~2000-char-per-message
limit. File uploads enforce Discord's 10 MiB cap.
"""
from __future__ import annotations

import logging
from pathlib import Path

import discord

from .config import DaemonConfig

log = logging.getLogger(__name__)

DISCORD_CHUNK_LIMIT = 1900
DISCORD_MAX_UPLOAD_BYTES = 10 * 1024 * 1024


def chunk(s: str, n: int) -> list[str]:
    """Split ``s`` into pieces of at most ``n`` chars. Returns ``[""]``
    for empty input so callers can always iterate."""
    return [s[i:i + n] for i in range(0, len(s), n)] or [""]


async def post_to_channel(bot: discord.Client, channel_id: int,
                            text: str, mention: bool
                            ) -> discord.Message | None:
    channel = bot.get_channel(int(channel_id))
    if not channel:
        log.warning("Channel %s not found by bot %s",
                     channel_id, getattr(bot, "user", "?"))
        return None
    prefix = "<@me> " if mention else ""
    last_msg = None
    try:
        for piece in chunk(prefix + text, DISCORD_CHUNK_LIMIT):
            last_msg = await channel.send(piece)  # type: ignore[union-attr]
    except discord.Forbidden as e:
        log.error("Forbidden posting to channel %s as bot %s: %s",
                  channel_id, getattr(bot, "user", "?"), e)
        raise
    return last_msg


async def post_to_channel_with_fallback(
    primary: discord.Client,
    fallback: discord.Client | None,
    channel_id: int,
    text: str,
    mention: bool,
) -> discord.Message | None:
    """Try ``primary`` first; on Forbidden or missing channel, fall
    back to ``fallback``. Handles the case where the daemon bot doesn't
    have channel permissions or wasn't ready yet."""
    try:
        result = await post_to_channel(primary, channel_id, text, mention)
        if result is not None:
            return result
    except discord.Forbidden:
        log.warning(
            "Primary bot %s lacks permission for channel %s; falling "
            "back to %s. Grant Send Messages in that channel to fix.",
            getattr(primary, "user", "?"), channel_id,
            getattr(fallback, "user", "?") if fallback else "(none)",
        )
    except Exception:
        log.exception("Primary bot post failed; trying fallback")
    if fallback is None or fallback is primary:
        return None
    return await post_to_channel(fallback, channel_id, text, mention)


async def post_to_thread(bot: discord.Client, thread_id: int,
                          text: str) -> bool:
    try:
        thread = bot.get_channel(thread_id)
        if thread is None:
            try:
                thread = await bot.fetch_channel(thread_id)
            except Exception:
                log.warning("Thread %s not fetchable", thread_id)
                return False
        for piece in chunk(text, DISCORD_CHUNK_LIMIT):
            await thread.send(piece)  # type: ignore[union-attr]
        return True
    except discord.NotFound:
        log.warning("Thread %s was deleted", thread_id)
        return False
    except Exception:
        log.exception("Failed to post to thread %s", thread_id)
        return False


async def post_file(bot: discord.Client, cfg: DaemonConfig, db,
                     project_id: str | None, file_path: str,
                     message: str) -> dict:
    fp = Path(file_path)
    if not fp.exists():
        return {"sent": False,
                "error": f"file does not exist: {file_path}"}
    if not fp.is_file():
        return {"sent": False,
                "error": f"path is not a file: {file_path}"}
    size = fp.stat().st_size
    if size > DISCORD_MAX_UPLOAD_BYTES:
        return {"sent": False,
                "error": (f"file is {size / 1024 / 1024:.1f} MiB, "
                          f"exceeds Discord's 10 MiB limit.")}

    channel = None
    if project_id:
        project = await db.get_project(project_id)
        if project and project.get("status_channel_id"):
            channel = bot.get_channel(int(project["status_channel_id"]))
    if channel is None:
        channel = bot.get_channel(cfg.orchestrator_channel_id)
    if channel is None:
        return {"sent": False,
                "error": "no channel available for posting"}

    try:
        discord_file = discord.File(str(fp), filename=fp.name)
        await channel.send(  # type: ignore[union-attr]
            content=message[:DISCORD_CHUNK_LIMIT] if message else None,
            file=discord_file,
        )
        return {"sent": True, "size_bytes": size}
    except Exception as e:
        log.exception("Failed to send file %s", file_path)
        return {"sent": False, "error": f"{type(e).__name__}: {e}"}
