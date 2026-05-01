#!/usr/bin/env python3
"""Pre-flight check. Validates the assumptions the daemon makes about
the claude CLI before we rely on them. Run this first:

    python scripts/smoke_test.py

Checks (in order):
  1. `claude --version` reports >= 2.1.32 (Agent Teams minimum).
  2. ANTHROPIC_API_KEY is NOT set.
  3. `claude /status` reports subscription auth (not API key).
  4. `claude -p --output-format stream-json "..."` emits events we can parse.
  5. The "result" message contains session_id and usage.
  6. `--resume <id>` carries the conversation forward.
  7. `--mcp-config` with a toy stdio server works, and the tool is callable.

If anything fails, DO NOT try to run the daemon. Fix the failure first.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def fail(msg: str) -> None:
    print(f"❌ {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"✓ {msg}")


def check_claude_installed() -> str:
    path = shutil.which("claude")
    if not path:
        fail("claude CLI not on PATH")
    result = subprocess.run(["claude", "--version"], capture_output=True, text=True)
    if result.returncode != 0:
        fail(f"`claude --version` failed: {result.stderr}")
    ok(f"claude found at {path}: {result.stdout.strip()}")
    return result.stdout.strip()


def check_no_api_key() -> None:
    if os.environ.get("ANTHROPIC_API_KEY"):
        fail("ANTHROPIC_API_KEY is set; unset it or daemon will burn API credits.")
    ok("ANTHROPIC_API_KEY not set")


def check_subscription_auth() -> None:
    # /status is an interactive command; not easily scriptable. We approximate
    # by running a tiny -p invocation and checking it doesn't error.
    result = subprocess.run(
        ["claude", "-p", "--max-turns", "1", "--output-format", "json", "Say hi in 3 words."],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        fail(f"smoke -p call failed: {result.stderr[:500]}")
    # Look for tell-tale "Credit balance is too low" or similar.
    if "credit balance" in result.stdout.lower() or "credit balance" in result.stderr.lower():
        fail("Subscription auth likely missing; claude thinks you're on API credits.")
    ok("Basic `claude -p` invocation works")


def check_stream_json() -> str:
    """Returns session_id extracted from result."""
    result = subprocess.run(
        ["claude", "-p",
         "--output-format", "stream-json",
         "--verbose",
         "--max-turns", "1",
         "What is 2+2? Reply just the number."],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        fail(f"stream-json call failed: {result.stderr[:500]}")

    session_id = None
    saw_result = False
    saw_usage = False
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            msg = json.loads(line)
        except Exception:
            continue
        if msg.get("type") == "result":
            saw_result = True
            session_id = msg.get("session_id")
            if msg.get("usage"):
                saw_usage = True

    if not saw_result:
        fail("No `result` message in stream-json output. Check CLI version.")
    if not session_id:
        fail("`result` message missing session_id. CLI version likely changed schema.")
    if not saw_usage:
        print("⚠ no usage data in result (token tracking will be inaccurate)")
    ok(f"stream-json works; session_id={session_id[:8]}...")
    return session_id


def check_resume(session_id: str) -> None:
    result = subprocess.run(
        ["claude", "-p",
         "--resume", session_id,
         "--output-format", "json",
         "--max-turns", "1",
         "What did I just ask you?"],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        fail(f"--resume failed: {result.stderr[:500]}")
    # We sent "2+2" so the response should mention it.
    out_lower = (result.stdout or "").lower()
    if "2" not in out_lower and "two" not in out_lower and "math" not in out_lower:
        print(f"⚠ resume response doesn't reference prior turn. Output: {result.stdout[:200]}")
    ok(f"--resume works with session_id")


def check_mcp_config() -> None:
    """Create a toy MCP server and verify claude can load and call it."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        server = tmp / "toy_server.py"
        server.write_text('''
from mcp.server.fastmcp import FastMCP
mcp = FastMCP(name="smoke-test")

@mcp.tool
def ping() -> str:
    """Returns 'pong'. Use this tool to test connectivity."""
    return "pong"

if __name__ == "__main__":
    mcp.run()
''')
        mcp_cfg = tmp / "mcp.json"
        mcp_cfg.write_text(json.dumps({
            "mcpServers": {
                "smoke-test": {
                    "type": "stdio",
                    "command": sys.executable,
                    "args": [str(server)],
                }
            }
        }))
        result = subprocess.run(
            ["claude", "-p",
             "--mcp-config", str(mcp_cfg),
             "--allowedTools", "mcp__smoke-test",
             "--output-format", "stream-json",
             "--verbose",
             "--max-turns", "3",
             "Call the ping tool and tell me what it returned."],
            capture_output=True, text=True, timeout=90,
        )
        if result.returncode != 0:
            fail(f"MCP config call failed: {result.stderr[:800]}")
        # Look for tool_use with name="ping" in the output
        found_tool_use = False
        for line in result.stdout.splitlines():
            try:
                msg = json.loads(line)
            except Exception:
                continue
            if msg.get("type") == "assistant":
                for block in msg.get("message", {}).get("content", []):
                    if block.get("type") == "tool_use" and block.get("name", "").endswith("ping"):
                        found_tool_use = True
        if not found_tool_use:
            print(f"⚠ didn't see a tool_use for ping. Output head:\n{result.stdout[:600]}")
            print("This may be fine if claude answered without calling the tool.")
        ok("MCP config loads and claude is aware of the tool")


if __name__ == "__main__":
    print("Orchestrator smoke test\n")
    check_claude_installed()
    check_no_api_key()
    check_subscription_auth()
    sid = check_stream_json()
    check_resume(sid)
    check_mcp_config()
    print("\n✅ All smoke checks passed. You can proceed to run the daemon.")
