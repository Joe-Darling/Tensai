"""Load daemon.toml + .env. Fail fast on misconfig."""
from __future__ import annotations

import os
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class DaemonConfig:
    projects_root: Path
    workspace: Path
    ipc_socket: Path

    # Name to refer to the human user as. Substituted into the OM's
    # behavioral spec at startup and into runtime preambles so the
    # orchestrator addresses you naturally rather than as "the user".
    # Defaults to "User" if [user] name is missing or empty.
    user_name: str
    # True if [user] name was explicitly configured. False means the
    # default fallback is in use, in which case the daemon posts a
    # one-shot startup notice telling the user how to personalize it.
    user_name_configured: bool

    discord_token: str
    daemon_discord_token: str | None  # None = single-bot mode (legacy)
    orchestrator_channel_id: int
    projects_category_id: int

    om_model: str
    om_workspace_dir: Path
    max_session_tokens: int
    compaction_warn_tokens: int  # pre-hook trigger
    max_session_hours: int

    worker_default_model: str
    max_concurrent_workers: int
    stuck_timeout_s: int
    worker_quiet_threshold_s: int
    enable_agent_teams: bool

    daemon_log: Path
    om_turn_log: Path
    log_level: str


def _expand(p: str) -> Path:
    return Path(p).expanduser().resolve()


def load_config(config_path: Path | None = None) -> DaemonConfig:
    config_path = config_path or Path("daemon.toml")
    if not config_path.exists():
        sys.exit(f"Config file not found: {config_path}. "
                 f"Copy daemon.toml.example to daemon.toml and edit.")

    load_dotenv()

    if os.environ.get("ANTHROPIC_API_KEY"):
        sys.exit(
            "ERROR: ANTHROPIC_API_KEY is set in your environment. "
            "This daemon is designed to use your Max subscription via the "
            "claude CLI's OAuth login. If you proceed with an API key set, "
            "every OM turn and worker spawn will bill your API account. "
            "Unset it (`unset ANTHROPIC_API_KEY`) and re-run."
        )

    discord_token = os.environ.get("DISCORD_TOKEN")
    if not discord_token:
        sys.exit("ERROR: DISCORD_TOKEN not set in .env")
    # Optional second token for the daemon-events bot. If not set,
    # falls back to single-bot mode where the OM bot handles all
    # outbound messages (legacy behavior).
    daemon_discord_token = os.environ.get("DAEMON_DISCORD_TOKEN")

    with config_path.open("rb") as f:
        raw = tomllib.load(f)

    d = raw["daemon"]
    dc = raw["discord"]
    om = raw["om"]
    w = raw["workers"]
    lg = raw["logging"]
    user = raw.get("user", {})

    raw_user_name = str(user.get("name") or "").strip()
    user_name_configured = bool(raw_user_name)
    user_name = raw_user_name or "User"

    workspace = _expand(d["workspace"])
    max_tokens = int(om["max_session_tokens"])
    # Default warn threshold: 75% of max, unless explicitly set.
    warn_tokens = int(om.get("compaction_warn_tokens", int(max_tokens * 0.75)))
    # Guard against nonsensical config
    if warn_tokens >= max_tokens:
        warn_tokens = int(max_tokens * 0.75)

    return DaemonConfig(
        projects_root=_expand(d["projects_root"]),
        workspace=workspace,
        ipc_socket=Path(d["ipc_socket"]),
        user_name=user_name,
        user_name_configured=user_name_configured,
        discord_token=discord_token,
        daemon_discord_token=daemon_discord_token,
        orchestrator_channel_id=int(dc["orchestrator_channel_id"]),
        projects_category_id=int(dc.get("projects_category_id", 0)),
        om_model=om["model"],
        om_workspace_dir=workspace / om["workspace_dir"],
        max_session_tokens=max_tokens,
        compaction_warn_tokens=warn_tokens,
        max_session_hours=int(om["max_session_hours"]),
        worker_default_model=w["default_model"],
        max_concurrent_workers=int(w["max_concurrent"]),
        stuck_timeout_s=int(w["stuck_timeout"]),
        worker_quiet_threshold_s=int(w.get("quiet_threshold", 720)),
        enable_agent_teams=bool(w["enable_agent_teams"]),
        daemon_log=workspace / lg["daemon_log"],
        om_turn_log=workspace / lg["om_turn_log"],
        log_level=lg.get("level", "INFO"),
    )