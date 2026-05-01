"""Slash commands for the OM bot.

Three commands, all scope-aware (project channel = that project, main
channel = global). All responses are ephemeral so the channel stays
clean — they're for the user's own situational awareness, not for
OM-user dialogue.

- ``/workers`` — workers in scope, with state filters.
- ``/actions`` — open escalations in scope.
- ``/projects`` — registered projects with active worker counts.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import discord
from discord import app_commands

from .discord_post import DISCORD_CHUNK_LIMIT, chunk

if TYPE_CHECKING:
    from .discord_bot import _OMBot

WORKER_STATE_EMOJI = {
    "running": "⚙️",
    "paused": "⏸️",
    "exited_no_summary": "❓",
    "completed": "✅",
    "failed": "❌",
    "killed": "🚫",
}

URGENCY_EMOJI = {
    "blocking": "🔴",
    "high": "🟠",
    "fyi": "🟡",
    "low": "⚪",
}


def format_relative_age(iso: str | None) -> str:
    if not iso:
        return "never"
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return iso
    delta = datetime.now(timezone.utc) - dt
    sec = int(delta.total_seconds())
    if sec < 60:
        return f"{sec}s ago"
    if sec < 3600:
        return f"{sec // 60}m ago"
    if sec < 86400:
        return f"{sec // 3600}h ago"
    return f"{sec // 86400}d ago"


def register_slash_commands(tree: app_commands.CommandTree,
                              bot: "_OMBot") -> None:
    """Register /workers, /actions, /projects on the command tree."""

    @tree.command(name="workers",
                   description="List workers in this scope (project or global)")
    @app_commands.describe(
        active="Show only currently running workers (state='running')",
        paused="Show only paused workers (waiting on OM to resume)",
        limit="Max workers to show (default 25, max 50)",
    )
    async def workers_cmd(
        interaction: discord.Interaction,
        active: bool = False,
        paused: bool = False,
        limit: int = 25,
    ):
        await _handle_workers_cmd(
            bot, interaction, active=active, paused=paused, limit=limit,
        )

    @tree.command(name="actions",
                   description="What's pending your input (open escalations)")
    async def actions_cmd(interaction: discord.Interaction):
        await _handle_actions_cmd(bot, interaction)

    @tree.command(name="projects",
                   description="List all registered projects")
    async def projects_cmd(interaction: discord.Interaction):
        await _handle_projects_cmd(bot, interaction)


async def _handle_workers_cmd(bot: "_OMBot",
                                interaction: discord.Interaction,
                                active: bool = False,
                                paused: bool = False,
                                limit: int = 25) -> None:
    await interaction.response.defer(ephemeral=True)
    project_id = await bot.project_id_for_channel(interaction.channel_id)
    scope_label = (f"project `{project_id}`" if project_id
                   else "all projects (global scope)")
    cap = max(1, min(int(limit or 25), 50))

    workers = await bot.db.list_workers(project_id=project_id)

    if active or paused:
        # Explicit state filters — show only what was requested.
        wanted_states: set[str] = set()
        if active:
            wanted_states.add("running")
        if paused:
            wanted_states.add("paused")
        interesting = [w for w in workers
                       if w.get("state") in wanted_states]
        if active and paused:
            list_label = "running or paused"
        elif active:
            list_label = "running"
        else:
            list_label = "paused"
    else:
        # Default: in-progress (incl. paused, exited_no_summary) plus
        # terminal workers from the last 6h for context.
        in_progress_states = ("running", "paused", "exited_no_summary")
        cutoff = datetime.now(timezone.utc) - timedelta(hours=6)
        interesting = []
        for w in workers:
            state = w.get("state")
            if state in in_progress_states:
                interesting.append(w)
                continue
            spawned = w.get("spawned_at")
            if spawned:
                try:
                    if datetime.fromisoformat(spawned) >= cutoff:
                        interesting.append(w)
                except ValueError:
                    pass
        list_label = "active or recent"

    if not interesting:
        await interaction.followup.send(
            f"No {list_label} workers in {scope_label}.",
            ephemeral=True,
        )
        return

    total = len(interesting)
    interesting = interesting[:cap]
    lines = [f"**Workers in {scope_label}** ({total} {list_label}, "
             f"showing {len(interesting)}):\n"]
    for w in interesting:
        wid = w["worker_id"]
        state = w["state"]
        proj = w["project_id"]
        # Collapse all whitespace (newlines, tabs, multi-space) into
        # single spaces so each entry stays on a single visible line.
        # Without this, multi-line task descriptions break the layout
        # and make multiple workers run together visually.
        task = " ".join((w.get("task") or "").split())[:120]
        last = format_relative_age(w.get("last_activity_at"))
        thread_id = w.get("thread_id")
        thread_link = f" → <#{thread_id}>" if thread_id else ""
        emoji = WORKER_STATE_EMOJI.get(state, "·")
        lines.append(f"{emoji} `{wid}` [{proj}] **{state}** "
                     f"(active {last}){thread_link}\n  └ {task}")

    if total > len(interesting):
        lines.append(f"\n_...and {total - len(interesting)} more_ "
                     f"(use `limit` to see more)")

    text = "\n".join(lines)
    for piece in chunk(text, DISCORD_CHUNK_LIMIT):
        await interaction.followup.send(piece, ephemeral=True)


async def _handle_actions_cmd(bot: "_OMBot",
                                interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True)
    project_id = await bot.project_id_for_channel(interaction.channel_id)
    scope_label = (f"project `{project_id}`" if project_id
                   else "all projects (global scope)")

    open_escalations = await bot.db.get_open_escalations(
        project_id=project_id,
    )

    if not open_escalations:
        await interaction.followup.send(
            f"📭 Nothing pending your input in {scope_label}. "
            f"OM is either idle or making decisions on its own.",
            ephemeral=True,
        )
        return

    lines = [f"**🔔 {len(open_escalations)} action(s) pending in "
             f"{scope_label}:**\n"]
    for esc in open_escalations[:20]:
        esc_id = esc["escalation_id"]
        urgency = esc.get("urgency", "fyi")
        summary = (esc.get("summary") or "")[:300]
        created = format_relative_age(esc.get("created_at"))
        proj = esc.get("worker_project_id") or "(no project)"
        msg_link = ""
        ch_id = esc.get("channel_id")
        msg_id = esc.get("message_id")
        if ch_id and msg_id:
            guild_id = (interaction.guild_id
                        if interaction.guild_id else "@me")
            msg_link = (f" [jump](https://discord.com/channels/"
                        f"{guild_id}/{ch_id}/{msg_id})")
        urgency_emoji = URGENCY_EMOJI.get(urgency, "·")
        lines.append(f"{urgency_emoji} `{esc_id}` [{proj}] "
                     f"({created}){msg_link}\n  └ {summary}")

    if len(open_escalations) > 20:
        lines.append(f"\n_...and {len(open_escalations) - 20} more_")

    text = "\n".join(lines)
    for piece in chunk(text, DISCORD_CHUNK_LIMIT):
        await interaction.followup.send(piece, ephemeral=True)


async def _handle_projects_cmd(bot: "_OMBot",
                                 interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True)
    summaries = await bot.db.project_summaries()

    if not summaries:
        await interaction.followup.send(
            "No projects registered.", ephemeral=True,
        )
        return

    lines = [f"**📁 {len(summaries)} project(s) registered:**\n"]
    for p in summaries:
        pid = p["project_id"]
        active = p["active_workers"]
        total = p["total_workers"]
        last = format_relative_age(p.get("last_worker_at"))
        ch_id = p.get("status_channel_id")
        ch_link = f"<#{ch_id}>" if ch_id else "(no channel)"
        active_str = (f"🟢 {active} active"
                      if active else f"⚪ idle (last worker {last})")
        lines.append(f"{active_str} — `{pid}` {ch_link} "
                     f"({total} workers ever)")

    text = "\n".join(lines)
    for piece in chunk(text, DISCORD_CHUNK_LIMIT):
        await interaction.followup.send(piece, ephemeral=True)
