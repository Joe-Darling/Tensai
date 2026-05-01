"""Path validation for OM tools.

Two policies enforced here:

- ``ensure_under_projects_root``: paths the OM passes to
  ``register_project`` / ``scaffold_project_directory`` must live under
  the configured projects_root (or be symlinked into it). Stops the OM
  from accidentally registering arbitrary filesystem locations.

- ``is_safe_send_path``: stricter allowlist + blocklist for files the
  OM wants to send to Discord. Refuses system/credential dirs and
  sensitive-looking filenames; capped at Discord's 10 MiB upload limit.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path

from .config import DaemonConfig

log = logging.getLogger(__name__)

DISCORD_MAX_UPLOAD_BYTES = 10 * 1024 * 1024

RISKY_BASENAMES = frozenset({
    ".env", ".env.local", ".env.production",
    ".pgpass", ".netrc", "credentials",
    "id_rsa", "id_ed25519", "id_ecdsa",
})


def resolve_for_validation(raw: str) -> tuple[Path, Path]:
    """Resolve ``raw`` into ``(input_path, parent_resolved)``.

    The second element is the resolved parent dir, used for the
    "is this under projects_root" check (we want the parent to exist;
    the input itself may not yet)."""
    input_path = Path(raw).expanduser()
    if not input_path.is_absolute():
        input_path = (Path.cwd() / input_path).resolve(strict=False)
    parent_resolved = input_path.parent.resolve()
    return input_path, parent_resolved


def ensure_under_projects_root(cfg: DaemonConfig, parent_resolved: Path,
                                display_path: Path) -> None:
    projects_root = cfg.projects_root.expanduser().resolve()
    try:
        parent_resolved.relative_to(projects_root)
    except ValueError:
        raise ValueError(
            f"path must be under {projects_root} (got {display_path}). "
            f"Create a symlink under projects_root pointing to the "
            f"real repo if the code lives elsewhere."
        )


class ProjectRootCache:
    """Small cache of resolved project roots, used by send-file
    validation. Rebuilt at most every 30s so newly-registered projects
    show up within one cycle without us hammering SQLite.

    Synchronous on purpose — called from the same thread as the IPC
    handler and we want a quick lookup, not an event-loop yield."""

    REBUILD_INTERVAL_S = 30

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._roots: list[Path] = []
        self._last_rebuild_at: float = 0.0

    def get(self) -> list[Path]:
        if time.monotonic() - self._last_rebuild_at > self.REBUILD_INTERVAL_S:
            self._last_rebuild_at = time.monotonic()
            try:
                conn = sqlite3.connect(str(self._db_path))
                cur = conn.execute("SELECT path FROM projects")
                roots: list[Path] = []
                for (raw,) in cur.fetchall():
                    try:
                        p = Path(raw).expanduser().resolve(strict=True)
                        roots.append(p)
                    except (OSError, FileNotFoundError):
                        continue
                conn.close()
                self._roots = roots
            except Exception:
                log.exception("Failed to refresh project roots cache")
        return self._roots


def _blocked_prefixes() -> list[Path]:
    """System and credential dirs that send_file refuses regardless of
    allowlist match. Resolved at call time so symlinks under these
    can't sneak through."""
    home_paths = [
        Path.home() / ".ssh",
        Path.home() / ".aws",
        Path.home() / ".gnupg",
    ]
    sentinels = [
        Path("/__no_such_path_ssh__"),
        Path("/__no_such_path_aws__"),
        Path("/__no_such_path_gpg__"),
    ]
    blocked = [Path("/etc").resolve(), Path("/proc").resolve(),
                Path("/sys").resolve(), Path("/root").resolve()]
    for hp, sentinel in zip(home_paths, sentinels):
        blocked.append(hp.resolve() if hp.exists() else sentinel)
    return blocked


def is_safe_send_path(cfg: DaemonConfig, root_cache: ProjectRootCache,
                       raw: str) -> tuple[bool, Path, str]:
    """Return ``(ok, resolved_path, error_msg)``.

    Rejects: nonexistent files, non-files, paths under system /
    credential dirs, sensitive-looking filenames, files >10 MiB.
    Allows: paths under projects_root, the orchestrator workspace,
    ``/tmp``, or any registered project's resolved root."""
    input_path = Path(raw).expanduser()
    if not input_path.is_absolute():
        input_path = (Path.cwd() / input_path).resolve(strict=False)
    try:
        resolved_target = input_path.resolve(strict=True)
    except FileNotFoundError:
        return False, input_path, f"file does not exist: {input_path}"

    if not resolved_target.is_file():
        return False, resolved_target, (
            f"path is not a regular file: {resolved_target}"
        )

    for blocked in _blocked_prefixes():
        try:
            resolved_target.relative_to(blocked)
            return False, resolved_target, (
                f"path is under a blocked location ({blocked}). "
                f"send_file_to_user refuses to read from system or "
                f"credential directories. Copy the file elsewhere "
                f"first if you really need to send it."
            )
        except ValueError:
            pass

    if resolved_target.name in RISKY_BASENAMES:
        return False, resolved_target, (
            f"refusing to send file with sensitive-looking name "
            f"'{resolved_target.name}'. If this is genuinely needed, "
            f"rename a copy first."
        )

    try:
        size = resolved_target.stat().st_size
        if size > DISCORD_MAX_UPLOAD_BYTES:
            return False, resolved_target, (
                f"file is {size / 1024 / 1024:.1f} MiB, exceeds "
                f"Discord's 10 MiB upload limit."
            )
    except OSError as e:
        return False, resolved_target, f"can't stat file: {e}"

    projects_root = cfg.projects_root.expanduser().resolve()
    workspace = cfg.workspace.resolve()
    tmp = Path("/tmp").resolve()
    allowed = [projects_root, workspace, tmp, *root_cache.get()]

    for prefix in allowed:
        try:
            resolved_target.relative_to(prefix)
            return True, resolved_target, ""
        except ValueError:
            continue

    return False, resolved_target, (
        f"file path {resolved_target} is outside allowed locations. "
        f"Allowed: under projects_root ({projects_root}), the "
        f"orchestrator workspace, /tmp, or any registered "
        f"project's root. Move the file to one of these first."
    )
