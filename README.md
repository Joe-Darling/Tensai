# Tensai

A personal multi-agent system that turns Claude Code into a project
manager. An orchestrator model (the "OM") coordinates a fleet of Claude
Code worker subprocesses across software projects you've registered,
mediates blocking decisions between them, and surfaces only what
actually needs your attention through Discord.

Runs against your existing Claude subscription (any plan — Pro, Max,
etc.) — no API spend. Optionally bridges to a ChatGPT subscription
for Codex worker fallback when Claude budget runs low.

> Status: working personal scaffold. End-to-end flows (project
> registration, worker spawning, mid-task escalations, scheduled
> follow-ups, two-engine routing) all run. Some convenience features
> are stubbed; see [What works / what's stubbed](#what-works--whats-stubbed).

## Why it exists

Claude Code is great at executing tasks but not at *managing* them
across days, projects, and parallel workstreams. Orchestrator adds the
missing layer:

- **A persistent orchestrator session** that remembers what's in flight
  across every project, with token-budget-aware compaction so it never
  forgets context it cared about.
- **Workers as subprocesses, not threads.** Each task gets its own
  `claude -p` (or `codex`) process with its own session, file edits,
  bash, and a small MCP tool surface for reporting progress and
  requesting decisions. Death and restart are first-class.
- **Discord as the PM interface.** One channel per project, threads per
  worker, reaction-based decisions on escalations. The orchestrator
  posts heartbeats and asks for direction; you reply when you want to.
- **Cross-engine routing.** When the Claude 5-hour quota nears its
  warning threshold, new workers are auto-routed to the OpenAI Codex
  CLI to reserve Claude budget for the orchestrator itself. Codex is
  optional — without a ChatGPT subscription, those workers just queue
  against Claude as usual.

## Requirements

- **Claude subscription.** Any plan — Pro, Max, Team — the daemon
  doesn't care which. The `claude` CLI must be logged in via OAuth.
  The daemon refuses to start if `ANTHROPIC_API_KEY` is set;
  everything bills your subscription, never the API.
- **A Discord server you control + a bot token** (free). The
  orchestrator uses Discord as its UI surface; one Discord bot
  identity is required, a second is optional for separating
  OM-voiced messages from system metadata.
- **Python 3.11+** on Linux, WSL, or macOS.
- **(Optional) ChatGPT subscription** + the `codex` CLI logged in,
  if you want the Codex worker fallback. The daemon auto-routes
  spawns to Codex when Claude is in rate-limit pressure, preserving
  the last bit of Claude budget for the orchestrator itself. Without
  Codex, the system still works — Claude does all the work.

## Architecture

```
                    ┌────────────────────────────┐
                    │       Discord channel      │
                    │  (per-project + main hub)  │
                    └──────────────┬─────────────┘
                                   │ messages, reactions
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Daemon (Python)                        │
│                                                                 │
│  ┌─────────────┐   ┌───────────────┐   ┌─────────────────────┐  │
│  │ Discord bot │←─→│ Priority bus  │   │ SQLite state.db     │  │
│  │ (1 or 2)    │   │ + IPC server  │   │ projects, workers,  │  │
│  └─────────────┘   │  (Unix sock)  │   │ sessions, schedules │  │
│                    └───────┬───────┘   └─────────────────────┘  │
│                            │ events                             │
│                            ▼                                    │
│             ┌──────────────────────────────┐                    │
│             │  OMRunner: claude -p per     │                    │
│             │  event, --resume <session>   │                    │
│             └──────────────┬───────────────┘                    │
│                            │ spawn / send / kill                │
│                            ▼                                    │
│             ┌──────────────────────────────┐                    │
│             │  WorkerRunner: claude / codex│                    │
│             │  subprocess pool, monitors,  │                    │
│             │  continuation, sweeping      │                    │
│             └──────────────────────────────┘                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
       ▲                                                  ▲
       │ MCP stdio                                        │ MCP stdio
       │                                                  │
┌──────┴──────────────┐                       ┌───────────┴────────┐
│ mcp_servers/        │                       │ mcp_servers/       │
│ om_tools.py         │                       │ worker_tools.py    │
│ (OM-side tool API)  │                       │ (worker tool API)  │
└─────────────────────┘                       └────────────────────┘
```

The MCP servers are thin stdio forwarders; every tool call becomes a
JSON line over the Unix socket to the daemon, which is the single
source of truth.

### How a turn works

1. A user message lands in a project's Discord channel.
2. The Discord bot publishes a `DISCORD_MESSAGE` event to the priority
   bus.
3. The daemon spawns a `claude -p` subprocess for the OM with
   `--resume <session_id>`, passing the formatted event as the prompt.
4. The OM runs one turn: thinks, decides, calls tools (which round-trip
   through MCP → IPC → daemon → result).
5. If the OM calls `spawn_worker`, the daemon launches a `claude -p` (or
   `codex`) child in the project directory and starts streaming its
   `stream-json` output for parsing.
6. Workers report progress / request decisions / mark complete via
   their own MCP tools. Each call hits the daemon, which posts to the
   project's Discord thread and routes decisions back to the worker
   when the OM (or the user) responds.

### Session and compaction

The OM has one logical session at a time. When token usage crosses a
soft threshold, the OM gets a one-shot `[COMPACTION_IMMINENT]` event so
it can persist anything ephemeral. When it crosses the hard threshold
(or the session is older than `max_session_hours`), the next turn is a
`[COMPACTION_REQUIRED]` summary turn. The summary plus a durable
`event_log` table seed the next fresh session.

### Worker engine routing

Workers default to Claude. When the daemon observes the Anthropic 5-hour
window in `allowed_warning` state (~90% of quota), new
`engine="claude"` spawns are silently rerouted to the Codex CLI,
preserving the remaining ~10% of Claude budget for the OM (which can't
fall back). A 🔀 notice is posted to the project channel so the
routing decision is visible.

## Setup

### 1. Install

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2. Verify subscription auth

The daemon refuses to start if `ANTHROPIC_API_KEY` is set in the
environment — that would silently bill the API account instead of
your Claude subscription. Check:

```bash
env | grep ANTHROPIC      # should print nothing
claude /status            # should report subscription auth
```

### 3. Smoke test

Validates the assumptions the daemon makes about the `claude` CLI
(stream-json schema, session resume, MCP stdio config). Run before the
daemon, especially after a CLI version bump.

```bash
python scripts/smoke_test.py
```

### 4. Configure

```bash
cp daemon.toml.example daemon.toml
cp .env.example .env
# Edit daemon.toml: set [user] name, discord channel/category IDs.
# Edit .env: set DISCORD_TOKEN (and optional DAEMON_DISCORD_TOKEN).
```

The configured `[user] name` is substituted into the OM's behavioral
spec — `om-workspace/CLAUDE.md` is rendered from
`om-workspace/CLAUDE.md.template` at every daemon startup — and into
the OM's runtime preambles, so the orchestrator addresses you
naturally rather than as "the user."

Create a Discord bot at https://discord.com/developers/applications →
New Application → Bot → enable Message Content Intent → copy token.
Invite to your server with Send Messages, Read Message History, Read
Messages, Add Reactions, Manage Channels (for auto-creating
per-project channels), Manage Threads, Use Slash Commands.

### 5. Register a project

Either drop a YAML file into `projects/`:

```yaml
# projects/my-project.yaml
project_id: my-project
path: /absolute/path/under/projects_root
description: One-liner the orchestrator will use to brief workers.
```

…or, with the daemon running, ask the orchestrator over Discord:

> Register a new project called my-project at
> /absolute/path/under/projects_root — it's a Flask service.

A `CLAUDE.md` in the project root is the project-specific context
workers and the orchestrator load.

### 6. Run

```bash
python -m daemon.main
```

The daemon logs to `logs/daemon.log`. Per-OM-turn JSON is in
`logs/om-turns.ndjson`. Per-worker output is in
`workers/<worker_id>/events.ndjson`.

## Usage

In a project's Discord channel:

> Spin up centralized inference for this project. Start with a design
> doc, no implementation yet.

The orchestrator parses the request, plans, spawns a worker, and
optionally replies with a status. The worker reports into the thread
on its spawn message; the orchestrator escalates to you only if it
hits a direction-changing decision it can't reasonably make on its
own.

You can ask anytime:

> What's running right now?
> Status on my-project.
> Kill the worker on my-project, I'm taking that direction elsewhere.

### Multi-modal Discord input

Messages aren't limited to text. The orchestrator handles whatever
you drop into Discord:

- **Text files** (`.md`, `.py`, `.json`, `.yaml`, `.log`, etc.) —
  downloaded to `inbox/<message_id>/` and surfaced to the OM as a
  path it can `Read`. Useful for "here's the spec, build this" or
  "review this log."
- **Images** — same flow, plus the OM reads them through Claude
  Code's native image support. Drag in a screenshot of a UI bug and
  the orchestrator can route it to a worker for fixing.
- **Other binary files** — surfaced with a "binary; Read may or may
  not work" hint so the OM tries appropriately.

### Reply context preserved

Reply to an earlier Discord message and the orchestrator sees the
quoted content and the original author. Useful for revisiting an
old escalation ("re: that decision yesterday, do X instead") or
referring back to a worker's progress update without re-typing it.

### Two-way file delivery

Workers produce real files (reports, screenshots, build artifacts);
the orchestrator can deliver them straight back over Discord. When a
worker completes with `artifacts=["/path/to/report.md"]` and you
asked for the report, the OM auto-uploads it to the project channel.
Subject to Discord's 10 MiB upload limit and a strict allowlist
(projects_root, workspace, `/tmp`, registered project roots) plus a
blocklist for system / credential dirs and sensitive-looking names.

The OM can also `zip_project` to bundle an entire project for
download.

### Reactions for fast decisions

When the OM needs you to choose between options, it posts an
escalation with up to 4 numbered reactions (1⃣..4⃣) seeded
automatically. Tap a reaction; the daemon synthesizes your choice
back into the OM's event stream as if you'd typed it. No reply
needed for quick yes/no or multi-choice. Free-form text replies
still work for anything nuanced.

### Worker progress lives in threads

Every spawned worker gets its own Discord thread off its spawn-ack
message. Worker status reports — "finished reading, starting
implementation," "tests green, writing docs," etc. — stream into
that thread, so the project channel stays clean and you can drill
into a specific worker's narrative without losing the bigger
picture.

### Slash commands

Scope-aware: run from a project channel they show that project; run
from the main channel they show everything.

- `/workers` — active and recent workers, with thread links.
- `/actions` — open escalations waiting on your input.
- `/projects` — all registered projects with active worker counts.

Responses are ephemeral, so they don't add channel noise.

### Long-running and silent worker recovery

- **Max-turns continuation.** Claude workers that hit their
  `--max-turns` budget mid-task pause with a structured progress
  report (`progress_summary`, `what_remains`,
  `recommended_next_turns`). The OM decides whether to resume them
  with more turns or move on.
- **Quiet-worker detection.** If a running worker hasn't called any
  orchestrator tool in ~12 minutes, the OM gets a `WORKER_QUIET`
  event with the worker's last 5 tool calls so it can investigate
  via `tail_worker_output` (which surfaces the actual command
  arguments and output snippets) before deciding to kill or wait.
- **Stuck-worker sweep.** Workers idle past `stuck_timeout` are
  auto-killed.

### Scheduled follow-ups

The OM can schedule its own future events: "in 20 minutes, check on
worker X if it's still silent." Conditional firing means the
reminder is silently dropped if circumstances change (e.g. the
worker is now reporting actively). Schedules survive session
compaction.

### Reply-dropped safety net

Sometimes the OM does its thinking in assistant text instead of
calling `reply_to_user`, and you see nothing. The daemon detects
this — if a turn triggered by your message ends without any
user-facing tool call, the OM gets a one-shot nudge to recover and
actually reply.

## What works

### Orchestration
- Per-event OM invocation with persistent session via `--resume`.
- Token-threshold session compaction with summary carry-forward,
  plus a one-shot pre-compaction warning so the OM can persist
  ephemeral state to durable notes.
- Time-based session compaction (sessions older than
  `max_session_hours` roll over).
- Durable `event_log` table — post-compaction sessions get seeded
  with the recent log so context isn't fully lost.
- Scheduled follow-ups (delay or absolute UTC time) with optional
  conditional firing rules.
- Reply-dropped detection and self-recovery.

### Discord interaction
- One- or two-bot mode (separate identities for OM-voiced messages
  vs system metadata).
- Per-project channels auto-created under a configured category
  when a project is registered.
- Per-worker threads on spawn-ack messages; worker progress streams
  there; completion and failure also land in the parent channel.
- Multi-modal input: text, images, and arbitrary attachments
  downloaded to `inbox/` for the OM to `Read`.
- Reply-to-message context: replies forward the quoted content +
  original author to the OM.
- Outbound text and file delivery, with chunked posts past Discord's
  ~2 KiB-per-message limit.
- `zip_project` for full-project archives (delivered as Discord
  uploads if under 10 MiB).
- Slash commands `/workers`, `/actions`, `/projects` (scope-aware,
  ephemeral responses).
- Reaction-based escalation resolution (1⃣..4⃣ on options).

### Workers
- Claude and Codex engine support; OM picks per spawn or accepts
  the Claude default.
- Auto-routing to Codex when Claude hits the `allowed_warning`
  rate-limit state, with a 🔀 notice posted to the project channel.
- Per-project concurrent-worker caps (configurable per project,
  with a sensible default).
- Mid-task `send_to_worker` (Claude only — Codex runs ephemeral).
- Worker continuation when Claude workers hit `--max-turns` (capped
  at 3 cycles per worker before forced failure).
- Worker decision requests are mediated by the OM; the OM only
  bubbles up to you for direction-changing calls.
- Artifact file delivery on completion, with placeholder-string
  rejection (workers can't fake a file by writing
  `"report (inline)"`).
- Stuck-worker sweep + quiet-worker detection.
- Engine-aware `tail_worker_output` parses both Claude and Codex
  stream-json formats.

### Project management
- YAML-based project registration (auto-loaded from `projects/` at
  startup).
- Per-project notes (`om-workspace/notes/<id>.md`) with category +
  timestamp structure, optional in-place updates for evolving state.
- Project archival (deletes DB rows + Discord channel; filesystem
  untouched, so re-registering restores the project fresh).
- Per-project `CLAUDE.md` loaded into worker and OM context.

### Safety
- Hard refusal of `ANTHROPIC_API_KEY` at startup.
- Path validation: `send_file_to_user` enforces an allowlist
  (`projects_root`, workspace, `/tmp`, registered project roots)
  and a blocklist (`/etc`, `/proc`, `~/.ssh`, `~/.aws`, `~/.gnupg`)
  plus filename heuristics (rejects `.env`, `id_rsa`, `credentials`,
  etc.).
- 10 MiB Discord upload cap.
- Workers run with a constrained tool surface (`mcp__orchestrator-
  worker`, `Bash`, `Read`, `Write`, `Edit`, `Glob`, `Grep`); the OM
  itself has no Bash/Write/Edit at all — it spawns workers for that.

## Stubbed / TODO

- `send_to_worker` to a Codex worker (Codex runs `--ephemeral`, so
  there's no session to resume — kill + respawn is the workaround).
- The separate time-based compaction watcher loop is empty; the
  per-tick check inside `_periodic_watcher` does the actual work,
  so behavior is correct, but the dedicated watcher is unwired.
- Graceful shutdown on SIGTERM flushes in-flight state but doesn't
  persist OM session checkpoints.

## Project layout

```
daemon/
  main.py            entry point, Daemon class, IPC tool dispatch
  config.py          daemon.toml + .env loading
  bus.py             priority event bus + IPC server/client
  db.py              SQLite schema + repository
  om_runner.py       per-event OM subprocess invocation
  worker_runner.py   worker spawn/monitor/lifecycle
  discord_bot.py     Discord layer (1- or 2-bot)
mcp_servers/
  om_tools.py        OM-side MCP server (stdio → IPC forwarder)
  worker_tools.py    worker-side MCP server (stdio → IPC forwarder)
prompts/
  worker_preamble.md       prepended to each Claude worker task
  worker_preamble_codex.md prepended to each Codex worker task
projects/            per-project YAML registrations (auto-loaded)
om-workspace/        OM's working dir
                       CLAUDE.md.template is the source-controlled
                       behavioral spec; daemon renders CLAUDE.md
                       from it on startup with [user] name baked in
skills/              reusable task templates the OM can compose into worker prompts
scripts/
  smoke_test.py      pre-flight check for the claude CLI
```

## Debugging

- Resume the OM's session interactively to inspect its conversation
  history: `claude --resume <session_id>`. The OM session is a real
  Claude session; `/status`, scrollback, and tool-call logs are all
  available.
- Per-OM-turn JSON: `logs/om-turns.ndjson`.
- Per-worker stream-json output: `workers/<worker_id>/events.ndjson`.
- Daemon log: `logs/daemon.log`.
- Per-OM-invocation stdout/stderr: `logs/om-stdout-<ts>.log`,
  `logs/om-stderr-<ts>.log`.

## License

MIT — see [LICENSE](LICENSE).
