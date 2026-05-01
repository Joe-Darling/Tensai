"""Discord layer.

Two-bot architecture:

- **OM bot** (``DISCORD_TOKEN``) — identity for OM-originated messages
  (``reply_to_user``, ``send_file_to_user``, ``escalate_to_pm``).
  Listens for user messages and reactions.
- **Daemon bot** (``DAEMON_DISCORD_TOKEN``, optional) — identity for
  system-generated messages (worker spawn acks, completions, reports
  relayed to threads, compaction notices, scheduling confirmations,
  REPLY_DROPPED nudges). Write-only.

If ``DAEMON_DISCORD_TOKEN`` isn't set, falls back to single-bot mode
where the OM bot handles everything.

Slash commands ``/workers``, ``/actions``, ``/projects`` are registered
on the OM bot. Their scope is auto-detected from the channel — see
:mod:`daemon.discord_slash`.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re

import aiofiles
import discord
from discord import app_commands

from .bus import Event, EventBus, EventKind
from .config import DaemonConfig
from .discord_post import (
    post_file,
    post_to_channel,
    post_to_channel_with_fallback,
    post_to_thread,
)
from .discord_slash import register_slash_commands

log = logging.getLogger(__name__)

# Maximum chars to quote of a reply-target message when forwarding the
# context to the OM. Long quotes blow OM token budget; truncated quotes
# still convey "the user was responding to X."
MAX_REPLY_QUOTE_CHARS = 2000

# Numeric reaction emojis seeded on escalation messages so the user
# can tap to resolve them. Capped at 4 options.
OPTION_EMOJIS = ["1⃣", "2⃣", "3⃣", "4⃣"]


def _channel_name_from_project_id(project_id: str) -> str:
    """Convert a project_id to a Discord-channel-safe slug."""
    name = project_id.lower().replace("_", "-")
    name = re.sub(r"[^a-z0-9-]", "-", name)
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:90] or "project"


class _OMBot(discord.Client):
    """Listens for user messages + reactions; speaks as OM."""

    def __init__(self, cfg: DaemonConfig, bus: EventBus, db,
                  manager: "BotsManager"):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.reactions = True
        super().__init__(intents=intents)
        self.cfg = cfg
        self.bus = bus
        self.db = db
        self.manager = manager
        self.inbox_dir = cfg.workspace / "inbox"
        self.inbox_dir.mkdir(exist_ok=True)
        self._project_channel_cache: dict[int, str] = {}
        self.tree = app_commands.CommandTree(self)
        register_slash_commands(self.tree, self)
        # The default-user-name notice is posted on first ready only.
        # on_ready can fire multiple times across gateway reconnects;
        # this flag prevents the notice from repeating.
        self._posted_default_user_name_notice = False

    async def on_ready(self) -> None:
        log.info("OM bot ready as %s", self.user)
        await self._rebuild_channel_cache()
        try:
            synced = await self.tree.sync()
            log.info("Synced %d slash command(s)", len(synced))
        except Exception:
            log.exception("Failed to sync slash commands")
        if (not self.cfg.user_name_configured
                and not self._posted_default_user_name_notice):
            self._posted_default_user_name_notice = True
            await self._post_default_user_name_notice()

    async def _post_default_user_name_notice(self) -> None:
        """Tell the user how to personalize the name the orchestrator
        addresses them by. Posted once per daemon run when no
        ``[user] name`` is configured in daemon.toml."""
        channel = self.get_channel(self.cfg.orchestrator_channel_id)
        if channel is None:
            log.warning(
                "Skipping default-user-name notice — orchestrator "
                "channel %s not visible to OM bot",
                self.cfg.orchestrator_channel_id,
            )
            return
        try:
            await channel.send(  # type: ignore[union-attr]
                "👋 The orchestrator is using the default name "
                "**User** to address you. To personalize this, set "
                "`[user] name = \"YourName\"` in `daemon.toml` and "
                "restart the daemon. Once set, the name is "
                "substituted into the OM's behavioral spec and "
                "runtime messages."
            )
        except Exception:
            log.exception("Failed to post default-user-name notice")

    async def _rebuild_channel_cache(self) -> None:
        projects = await self.db.list_projects()
        self._project_channel_cache = {
            int(p["status_channel_id"]): p["project_id"]
            for p in projects
            if p.get("status_channel_id")
        }
        log.info("Project channel cache: %d entries",
                 len(self._project_channel_cache))

    async def project_id_for_channel(self, channel_id: int) -> str | None:
        if channel_id == self.cfg.orchestrator_channel_id:
            return None
        if channel_id in self._project_channel_cache:
            return self._project_channel_cache[channel_id]
        # Maybe it's a thread under a project channel.
        ch = self.get_channel(channel_id)
        parent_id = getattr(ch, "parent_id", None) if ch else None
        if parent_id and parent_id in self._project_channel_cache:
            return self._project_channel_cache[parent_id]
        # DB fallback.
        project = await self.db.get_project_by_channel(channel_id)
        if project:
            pid = project["project_id"]
            self._project_channel_cache[channel_id] = pid
            return pid
        return None

    async def on_message(self, msg: discord.Message) -> None:
        if msg.author.bot:
            return
        project_id = await self._project_for_message(msg)
        if project_id is False:
            # Channel is not the orchestrator channel, not a known
            # project channel, and not under a known project category.
            return
        attachment_paths, attachment_types = await self._save_attachments(msg)
        reply_to_content, reply_to_author = await self._fetch_reply_target(msg)

        await self.bus.publish(Event(
            kind=EventKind.DISCORD_MESSAGE,
            payload={
                "text": msg.content,
                "attachments": attachment_paths,
                "attachment_types": attachment_types,
                "author": str(msg.author),
                "message_id": str(msg.id),
                "project_context": project_id,
                "channel_id": msg.channel.id,
                "reply_to_content": reply_to_content,
                "reply_to_author": reply_to_author,
            },
        ))

    async def _project_for_message(self, msg: discord.Message
                                     ) -> str | None | bool:
        """Returns the project_id for ``msg``'s channel, ``None`` for
        the orchestrator channel (project-less), or ``False`` to mean
        "ignore this message — channel is not part of the system."""
        if msg.channel.id == self.cfg.orchestrator_channel_id:
            return None
        if msg.channel.id in self._project_channel_cache:
            return self._project_channel_cache[msg.channel.id]
        parent_id = getattr(msg.channel, "parent_id", None)
        if parent_id and parent_id in self._project_channel_cache:
            return self._project_channel_cache[parent_id]
        project = await self.db.get_project_by_channel(msg.channel.id)
        if project:
            project_id = project["project_id"]
            self._project_channel_cache[msg.channel.id] = project_id
            return project_id
        return False

    async def _save_attachments(self, msg: discord.Message
                                  ) -> tuple[list[str], list[str]]:
        attachment_paths: list[str] = []
        attachment_types: list[str] = []
        if not msg.attachments:
            return attachment_paths, attachment_types

        msg_dir = self.inbox_dir / str(msg.id)
        msg_dir.mkdir(exist_ok=True)
        text_extensions = (
            ".md", ".txt", ".log", ".py", ".js", ".ts", ".json",
            ".yaml", ".yml", ".toml", ".csv",
        )
        for att in msg.attachments:
            target = msg_dir / att.filename
            async with aiofiles.open(target, "wb") as f:
                await f.write(await att.read())
            attachment_paths.append(str(target))
            ct = att.content_type or ""
            if ct.startswith("image/"):
                kind = "image"
            elif ct.startswith("text/") or att.filename.endswith(text_extensions):
                kind = "text"
            else:
                kind = "binary"
            attachment_types.append(kind)
        return attachment_paths, attachment_types

    async def _fetch_reply_target(self, msg: discord.Message
                                    ) -> tuple[str | None, str | None]:
        if not (msg.reference and msg.reference.message_id):
            return None, None
        try:
            ref_msg = msg.reference.resolved
            if ref_msg is None:
                ref_msg = await msg.channel.fetch_message(
                    msg.reference.message_id
                )
            if ref_msg:
                content = ref_msg.content or ""
                if len(content) > MAX_REPLY_QUOTE_CHARS:
                    content = (content[:MAX_REPLY_QUOTE_CHARS]
                               + f"... (truncated, original "
                                 f"{len(ref_msg.content)} chars)")
                return content, str(ref_msg.author)
        except (discord.NotFound, discord.Forbidden) as e:
            log.warning("Couldn't fetch replied-to message %s: %s",
                        msg.reference.message_id, e)
        except Exception:
            log.exception("fetch_message failed")
        return None, None

    async def on_raw_reaction_add(
            self, payload: discord.RawReactionActionEvent) -> None:
        if payload.user_id == (self.user.id if self.user else 0):
            return
        # Also ignore reactions added by the daemon bot.
        daemon_user_id = self.manager.daemon_bot_user_id()
        if daemon_user_id and payload.user_id == daemon_user_id:
            return
        emoji_str = str(payload.emoji)
        if emoji_str not in OPTION_EMOJIS:
            return
        option_index = OPTION_EMOJIS.index(emoji_str)
        escalation = await self.db.get_escalation_by_message(
            payload.message_id
        )
        if not escalation:
            return
        if escalation.get("resolved_at"):
            log.info("Ignoring reaction on already-resolved escalation %s",
                     escalation["escalation_id"])
            return
        options_raw = escalation.get("options")
        options = json.loads(options_raw) if options_raw else []
        if option_index >= len(options):
            log.info(
                "Reaction %s out of range for escalation %s (has %d opts)",
                emoji_str, escalation["escalation_id"], len(options),
            )
            return
        chosen = options[option_index]
        chosen_label = chosen.get("label", f"option {option_index + 1}")
        log.info("Reaction resolution: escalation=%s option=%s",
                 escalation["escalation_id"], chosen_label)
        await self.db.resolve_escalation(
            escalation["escalation_id"],
            f"via reaction: {chosen_label}",
        )
        project_id = None
        related_worker = escalation.get("related_worker_id")
        if related_worker:
            w = await self.db.get_worker(related_worker)
            if w:
                project_id = w["project_id"]
        await self.bus.publish(Event(
            kind=EventKind.DISCORD_MESSAGE,
            payload={
                "text": (f"(via reaction) On escalation "
                         f"{escalation['escalation_id']}, I chose option "
                         f"{option_index + 1}: {chosen_label}"),
                "attachments": [],
                "attachment_types": [],
                "author": self.cfg.user_name,
                "message_id": str(payload.message_id),
                "project_context": project_id,
                "channel_id": payload.channel_id,
                "reply_to_content": None,
                "reply_to_author": None,
            },
        ))


class _DaemonBot(discord.Client):
    """Write-only bot for system-generated messages."""

    def __init__(self):
        intents = discord.Intents.default()
        # No message_content / reactions — this bot doesn't listen.
        super().__init__(intents=intents)

    async def on_ready(self) -> None:
        log.info("Daemon bot ready as %s", self.user)


class BotsManager:
    """Routes outbound messages to the right bot identity, manages
    both gateway connections, and exposes the API the rest of the
    daemon uses."""

    def __init__(self, cfg: DaemonConfig, bus: EventBus, db):
        self.cfg = cfg
        self.bus = bus
        self.db = db
        self.om_bot = _OMBot(cfg, bus, db, manager=self)
        self.daemon_bot: _DaemonBot | None = None
        if cfg.daemon_discord_token:
            self.daemon_bot = _DaemonBot()
            log.info("Dual-bot mode: separate daemon bot will be used "
                     "for system messages")
        else:
            log.info("Single-bot mode: OM bot handles all outbound "
                     "messages (set DAEMON_DISCORD_TOKEN to split)")

    def daemon_bot_user_id(self) -> int | None:
        if self.daemon_bot and self.daemon_bot.user:
            return self.daemon_bot.user.id
        return None

    def _system_client(self) -> discord.Client:
        """Bot client for system-generated messages. Daemon bot if
        available, OM bot as fallback."""
        return self.daemon_bot if self.daemon_bot else self.om_bot

    def _om_client(self) -> discord.Client:
        """Bot client for OM-originated messages."""
        return self.om_bot

    async def start(self) -> None:
        """Start both gateway connections concurrently. Returns when
        either disconnects (which is fatal either way)."""
        tasks = [asyncio.create_task(
            self.om_bot.start(self.cfg.discord_token)
        )]
        if self.daemon_bot:
            tasks.append(asyncio.create_task(
                self.daemon_bot.start(self.cfg.daemon_discord_token)
            ))
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
        for t in done:
            exc = t.exception()
            if exc:
                raise exc

    # --- OM-voiced posts (reply_to_user, send_file_to_user, escalate_to_pm) ---

    async def post(self, text: str,
                    mention: bool = False) -> discord.Message | None:
        return await post_to_channel(
            self._om_client(), self.cfg.orchestrator_channel_id,
            text, mention,
        )

    async def post_to_project(self, project_id: str, text: str,
                                mention: bool = False
                                ) -> discord.Message | None:
        project = await self.db.get_project(project_id)
        channel_id = project.get("status_channel_id") if project else None
        if channel_id:
            return await post_to_channel(
                self._om_client(), int(channel_id), text, mention,
            )
        return await self.post(text, mention=mention)

    async def post_file_to_project(self, project_id: str | None,
                                     file_path: str,
                                     message: str = "") -> dict:
        return await post_file(
            self._om_client(), self.cfg, self.db,
            project_id, file_path, message,
        )

    async def add_option_reactions(self, message: discord.Message,
                                     num_options: int) -> None:
        """Reactions on escalation messages — added by the OM bot since
        the message is OM-voiced."""
        capped = min(num_options, len(OPTION_EMOJIS))
        for i in range(capped):
            try:
                await message.add_reaction(OPTION_EMOJIS[i])
            except Exception:
                log.exception("Failed to add reaction %s",
                              OPTION_EMOJIS[i])
                break

    # --- Daemon-voiced posts (system metadata) ---

    async def system_post(self, text: str,
                            mention: bool = False
                            ) -> discord.Message | None:
        return await post_to_channel_with_fallback(
            self._system_client(),
            self.om_bot if self.daemon_bot else None,
            self.cfg.orchestrator_channel_id, text, mention,
        )

    async def system_post_to_project(self, project_id: str, text: str,
                                       mention: bool = False
                                       ) -> discord.Message | None:
        project = await self.db.get_project(project_id)
        channel_id = project.get("status_channel_id") if project else None
        if channel_id:
            return await post_to_channel_with_fallback(
                self._system_client(),
                self.om_bot if self.daemon_bot else None,
                int(channel_id), text, mention,
            )
        return await self.system_post(text, mention=mention)

    async def system_post_to_channel(self, channel_id: int, text: str,
                                       mention: bool = False) -> None:
        await post_to_channel_with_fallback(
            self._system_client(),
            self.om_bot if self.daemon_bot else None,
            int(channel_id), text, mention,
        )

    async def post_to_thread(self, thread_id: int, text: str) -> bool:
        """Worker reports relayed to threads — daemon-voiced. Falls
        back to OM bot if the daemon bot lacks permission or fails."""
        ok = await post_to_thread(self._system_client(), thread_id, text)
        if ok:
            return True
        if self.daemon_bot and self._system_client() is self.daemon_bot:
            log.warning("Daemon bot couldn't post to thread %s; "
                         "falling back to OM bot", thread_id)
            return await post_to_thread(self.om_bot, thread_id, text)
        return False

    async def create_worker_thread(self, message: discord.Message,
                                     worker_id: str) -> int | None:
        # Threads are created on the spawn-ack message, which was
        # posted by the daemon bot. Use the same client to create the
        # thread.
        try:
            thread = await message.create_thread(
                name=f"worker-{worker_id}",
                auto_archive_duration=1440,
            )
            log.info("Created thread %s for worker %s",
                     thread.id, worker_id)
            return thread.id
        except Exception:
            log.exception("Failed to create thread for worker %s",
                            worker_id)
            return None

    async def ensure_project_channel(self, project_id: str,
                                      description: str = ""
                                      ) -> int | None:
        """Channel auto-creation. Run via OM bot since it owns the
        listening side and needs to register the channel in its
        cache."""
        if not self.cfg.projects_category_id:
            return None

        bot = self._om_client()
        project = await self.db.get_project(project_id)
        if project and project.get("status_channel_id"):
            existing = bot.get_channel(int(project["status_channel_id"]))
            if existing:
                return int(project["status_channel_id"])

        category = bot.get_channel(self.cfg.projects_category_id)
        if not category or not isinstance(category, discord.CategoryChannel):
            log.error("projects_category_id %s is not a valid category",
                       self.cfg.projects_category_id)
            return None

        chan_name = _channel_name_from_project_id(project_id)

        for ch in category.channels:
            if isinstance(ch, discord.TextChannel) and ch.name == chan_name:
                log.info("Reusing existing channel %s for %s",
                          ch.id, project_id)
                await self.db.set_project_channel(project_id, ch.id)
                self.om_bot._project_channel_cache[ch.id] = project_id
                return ch.id

        try:
            topic = (description or
                     f"Status + chat for project {project_id}")[:1024]
            new_channel = await category.create_text_channel(
                name=chan_name,
                topic=topic,
                reason=(f"Tensai: auto-create channel for project "
                          f"{project_id}"),
            )
            await self.db.set_project_channel(project_id, new_channel.id)
            self.om_bot._project_channel_cache[new_channel.id] = project_id
            log.info("Created channel %s (%s) for project %s",
                      new_channel.id, chan_name, project_id)
            return new_channel.id
        except discord.Forbidden:
            log.error("Bot lacks Manage Channels permission; cannot "
                       "create channel for %s", project_id)
            return None
        except Exception:
            log.exception("Channel creation failed for %s", project_id)
            return None

    def get_channel(self, channel_id: int):
        """Generic get_channel — uses OM bot since both bots see the
        same channels (same guild)."""
        return self.om_bot.get_channel(channel_id)
