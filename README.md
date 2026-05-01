# Orchestrator

A personal multi-agent system that turns Claude Code into a project
manager. An orchestrator model (the "OM") coordinates a fleet of Claude
Code worker subprocesses across software projects you've registered,
mediates blocking decisions between them, and surfaces only what
actually needs your attention through Discord.

Built to run against a Claude Max subscription — no API spend.

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
  CLI to reserve Claude budget for the orchestrator itself.

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
environment — that would silently bill the API account instead of the
Max subscription. Check:

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

Slash commands (any channel, scope inferred from the channel):

- `/workers` — workers in scope (project channel = that project, main
  channel = global).
- `/actions` — open escalations in scope.
- `/projects` — all registered projects with active worker counts.

## What works / what's stubbed

Working:

- Config + env loading (with API-key refusal).
- SQLite schema + DB layer.
- Priority event bus + Unix-socket IPC.
- Discord bot (one- or two-bot mode), per-project channel
  auto-creation, per-worker threads.
- OM lifecycle: per-event `claude -p` invocation with session resume,
  compaction (threshold-based and time-based) with summary carry-forward.
- Worker spawning and monitoring (Claude and Codex).
- Auto-routing to Codex on Claude rate-limit pressure.
- All OM-side and worker-side MCP tools routed through the daemon.
- Escalations to Discord with reaction-resolved options.
- Stuck-worker sweep, quiet-worker detection, scheduled follow-ups
  with conditional firing.
- Worker continuation when a Claude worker hits its `--max-turns`
  budget mid-task.

Stubbed / TODO:

- `send_to_worker` to a Codex worker (Codex runs `--ephemeral`, so
  there's no session to resume — kill + respawn is the workaround).
- `_compaction_watcher` is an empty loop. Token-threshold compaction
  works (checked after each turn); a separate time-based watcher
  doesn't.
- Graceful shutdown on SIGTERM (flushes in-flight state but doesn't
  persist OM session checkpoints).

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
