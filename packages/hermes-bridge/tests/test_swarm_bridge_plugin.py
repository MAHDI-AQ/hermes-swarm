"""Tests for hermes-swarm-bridge plugin activation gate, schemas, HTTP/disk board ops, wait, and hygiene."""

from __future__ import annotations

import ast
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
import unittest

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_TERMS = (
    "cto",
    "owner",
    "tier1",
    "company",
    "org",
    "senior",
    "junior",
    "queen",
    "yolo",
)
FORBIDDEN_RE = re.compile(r"\b(?:" + "|".join(FORBIDDEN_TERMS) + r")\b", re.IGNORECASE)


def _load_plugin():
    mod_name = "hermes_swarm_bridge"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(
        mod_name,
        PLUGIN_ROOT / "__init__.py",
        submodule_search_locations=[str(PLUGIN_ROOT)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


sys.modules.setdefault("__init__", _load_plugin())


class _FakeCtx:
    def __init__(self) -> None:
        self.tools: dict[str, dict] = {}
        self.sections: dict[str, tuple] = {}
        self.hooks: dict[str, Any] = {}

    def register_tool(self, *, name, toolset, schema, handler, description="", **kwargs):
        self.tools[name] = {
            "toolset": toolset,
            "schema": schema,
            "handler": handler,
            "description": description,
            "extra": kwargs,
        }

    def register_system_prompt_section(self, section_id, content, **kwargs):
        self.sections[section_id] = (content, kwargs)

    def register_hook(self, hook_name, hook_fn):
        self.hooks[hook_name] = hook_fn


class _EnvPatch:
    def __init__(self, updates: dict[str, str | None]) -> None:
        self._updates = updates
        self._saved: dict[str, str | None] = {}

    def __enter__(self):
        for k, v in self._updates.items():
            self._saved[k] = os.environ.get(k)
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return self

    def __exit__(self, *args):
        for k, prev in self._saved.items():
            if prev is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = prev


class SwarmBridgePluginTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plugin = _load_plugin()
        self.tmp = tempfile.TemporaryDirectory()
        self.board_dir = Path(self.tmp.name) / "swarm-1"
        self.board_dir.mkdir(parents=True, exist_ok=True)
        self.base_clear_env = {
            "HERMES_SWARM_ID": None,
            "HERMES_SWARM_AGENT": None,
            "HERMES_SWARM_ROLE": None,
            "HERMES_SWARM_HTTP_URL": None,
            "HERMES_SWARM_HTTP_TOKEN": None,
            "HERMES_SWARM_TOKEN_FILE": None,
            "HERMES_SWARM_BOARD_DIR": None,
            "HERMES_SWARM_CONVERSATION_ID": None,
            "HERMES_SWARM_TASK_ID": None,
            "HERMES_SWARM_CONTEXT_ID": None,
            "HERMES_A2A_SWARM_ID": None,
            "HERMES_A2A_WORKER_NAME": None,
            "HERMES_A2A_WORKER_ROLE": None,
            "HERMES_A2A_HTTP_URL": None,
            "HERMES_A2A_HTTP_TOKEN": None,
            "HERMES_A2A_TOKEN_FILE": None,
            "HERMES_A2A_SWARM_BOARD_DIR": None,
            "HERMES_A2A_CONVERSATION_ID": None,
            "HERMES_A2A_STATE_DIR": None,
        }

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_manifest_and_stdlib_only(self) -> None:
        manifest = (PLUGIN_ROOT / "plugin.yaml").read_text(encoding="utf-8")
        for expected in (
            "name: hermes-swarm-bridge",
            'version: "1.0.0"',
            "kind: standalone",
            "manifest_version: 2",
            "api_version: 1",
            'requires_hermes: ">=0.21.4"',
            "- swarm_post",
            "- swarm_read",
            "- swarm_claim",
            "- swarm_release",
            "- swarm_status",
            "- swarm_wait",
            "- swarm_channel",
        ):
            self.assertIn(expected, manifest)

        stdlib_names = getattr(sys, "stdlib_module_names", set())
        for py_path in (PLUGIN_ROOT / "__init__.py", PLUGIN_ROOT / "swarm_tools.py"):
            tree = ast.parse(py_path.read_text(encoding="utf-8"), filename=str(py_path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        top = alias.name.split(".")[0]
                        self.assertIn(top, stdlib_names, f"Non-stdlib import {top} in {py_path.name}")
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    top = node.module.split(".")[0]
                    self.assertIn(top, stdlib_names, f"Non-stdlib import {top} in {py_path.name}")

    def test_always_registers_tools_and_gates_prompt_section(self) -> None:
        with _EnvPatch({**self.base_clear_env, "HERMES_A2A_STATE_DIR": str(self.tmp.name)}):
            ctx = _FakeCtx()
            self.plugin.register(ctx)
            self.assertEqual(
                set(ctx.tools.keys()),
                {"swarm_post", "swarm_read", "swarm_claim", "swarm_release", "swarm_status", "swarm_wait", "swarm_channel"},
            )
            self.assertEqual(ctx.sections, {})
            for name in ("swarm_post", "swarm_read", "swarm_claim", "swarm_release", "swarm_status", "swarm_wait", "swarm_channel"):
                props = ctx.tools[name]["schema"]["parameters"]["properties"]
                for key in ("swarm_id", "agent", "http_url", "board_dir", "token_file", "conversation_id"):
                    self.assertIn(key, props, f"{name} missing explicit arg {key}")

            # Non-swarm call with no swarm_id/agent and empty registry returns clear error
            err_post = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "hello"}))
            self.assertFalse(err_post["ok"])
            self.assertIn("swarm_id", err_post["error"])

            err_read = json.loads(ctx.tools["swarm_read"]["handler"]({}))
            self.assertFalse(err_read["ok"])
            self.assertIn("swarm_id", err_read["error"])

        with _EnvPatch({**self.base_clear_env, "HERMES_SWARM_ID": "swarm-1", "HERMES_SWARM_AGENT": ""}):
            ctx = _FakeCtx()
            self.plugin.register(ctx)
            self.assertEqual(len(ctx.tools), 7)
            self.assertIn("hermes-swarm-bridge.worker", ctx.sections)

        with _EnvPatch({**self.base_clear_env, "HERMES_SWARM_ID": "", "HERMES_SWARM_AGENT": "coder"}):
            ctx = _FakeCtx()
            self.plugin.register(ctx)
            self.assertEqual(len(ctx.tools), 7)
            self.assertEqual(ctx.sections, {})

    def test_explicit_args_priority_and_registry_discovery(self) -> None:
        state_root = Path(self.tmp.name) / "state_root"
        swarm_a_dir = state_root / "swarms" / "swarm-reg"
        swarm_b_dir = state_root / "swarms" / "swarm-explicit"
        swarm_a_dir.mkdir(parents=True, exist_ok=True)
        swarm_b_dir.mkdir(parents=True, exist_ok=True)
        tok_file = state_root / "internal_token"
        tok_file.write_text("reg-secret-tok\n", encoding="utf-8")

        posts_seen: list[tuple[str, dict]] = []

        class _RegHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or "0")
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                posts_seen.append((self.headers.get("Authorization") or "", body))
                resp = json.dumps({"seq": 4, "id": "bmsg-reg", "summary": body.get("summary", ""), "kind": body.get("kind")}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)

            def do_GET(self):
                snap = {
                    "swarm": "swarm-reg",
                    "swarmId": "swarm-reg",
                    "head": 4,
                    "members": [{"name": "gw-worker", "role": "worker", "status": "working"}],
                    "messages": [{"seq": 4, "sender": "gw-worker", "summary": "@coordinator ok"}],
                    "claims": {},
                    "channels": [],
                }
                resp = json.dumps(snap).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)

        server = ThreadingHTTPServer(("127.0.0.1", 0), _RegHandler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            reg_doc = {
                "servers": {
                    "99999999": {
                        "pid": 99999999,
                        "swarmId": "dead-swarm",
                        "httpUrl": "http://127.0.0.1:1",
                        "conversationId": "conv-dead",
                    },
                    str(os.getpid()): {
                        "pid": os.getpid(),
                        "swarmId": "swarm-reg",
                        "httpUrl": f"http://127.0.0.1:{port}",
                        "conversationId": "conv-reg",
                    },
                }
            }
            (state_root / "registry.json").write_text(json.dumps(reg_doc), encoding="utf-8")

            with _EnvPatch({**self.base_clear_env, "HERMES_A2A_STATE_DIR": str(state_root)}):
                ctx = _FakeCtx()
                self.plugin.register(ctx)
                self.assertEqual(ctx.sections, {})

                # Registry discovery fills swarm_id, http_url, conversation_id, token_file; still needs agent for post
                missing_agent = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "missing agent"}))
                self.assertFalse(missing_agent["ok"])
                self.assertIn("agent", missing_agent["error"])

                # With explicit agent, registry.json discovery hits live HTTP server & internal_token
                ok_reg = json.loads(ctx.tools["swarm_post"]["handler"]({
                    "summary": "@coordinator via registry",
                    "agent": "gw-worker",
                    "kind": "handoff_ready",
                }))
                self.assertTrue(ok_reg["ok"])
                self.assertEqual(ok_reg["source"], "http")
                self.assertEqual(ok_reg["swarmId"], "swarm-reg")
                self.assertEqual(posts_seen[-1][0], "Bearer reg-secret-tok")
                self.assertEqual(posts_seen[-1][1]["conversationId"], "conv-reg")
                self.assertEqual(posts_seen[-1][1]["sender"], "gw-worker")

                # swarm_read works via registry discovery even without agent
                r_reg = json.loads(ctx.tools["swarm_read"]["handler"]({"since": 0}))
                self.assertTrue(r_reg["ok"])
                self.assertEqual(r_reg["source"], "http")
                self.assertEqual(r_reg["swarmId"], "swarm-reg")

            # Priority: explicit arg > HERMES_SWARM_* > HERMES_A2A_*
            env_prio = {
                **self.base_clear_env,
                "HERMES_A2A_STATE_DIR": str(state_root),
                "HERMES_A2A_SWARM_ID": "swarm-a2a",
                "HERMES_A2A_WORKER_NAME": "agent-a2a",
                "HERMES_SWARM_ID": "swarm-env",
                "HERMES_SWARM_AGENT": "agent-env",
                "HERMES_SWARM_BOARD_DIR": str(swarm_b_dir),
            }
            with _EnvPatch(env_prio):
                ctx = _FakeCtx()
                self.plugin.register(ctx)
                # Explicit arg overrides HERMES_SWARM_* and HERMES_A2A_*
                p_exp = json.loads(ctx.tools["swarm_post"]["handler"]({
                    "summary": "@coordinator explicit override",
                    "kind": "blocker",
                    "swarm_id": "swarm-explicit",
                    "agent": "agent-explicit",
                    "board_dir": str(swarm_b_dir),
                }))
                self.assertTrue(p_exp["ok"])
                self.assertEqual(p_exp["source"], "disk")
                self.assertEqual(p_exp["swarmId"], "swarm-explicit")
                self.assertEqual(p_exp["message"]["sender"], "agent-explicit")
                self.assertEqual(p_exp["message"]["kind"], "blocker")
                self.assertEqual(p_exp["message"]["type"], "escalation")
        finally:
            server.shutdown()
            server.server_close()

    def test_registers_all_six_tools_and_prompt_section(self) -> None:
        env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": "swarm-42",
            "HERMES_SWARM_AGENT": "coder-a",
            "HERMES_SWARM_ROLE": "worker",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        with _EnvPatch(env):
            ctx = _FakeCtx()
            self.plugin.register(ctx)

        expected_tools = {
            "swarm_post",
            "swarm_read",
            "swarm_claim",
            "swarm_release",
            "swarm_status",
            "swarm_wait",
            "swarm_channel",
        }
        self.assertEqual(set(ctx.tools.keys()), expected_tools)
        for name, entry in ctx.tools.items():
            self.assertEqual(entry["toolset"], "hermes-swarm-bridge")
            self.assertEqual(entry["schema"]["name"], name)
            self.assertEqual(entry["schema"]["parameters"]["type"], "object")

        self.assertIn("hermes-swarm-bridge.worker", ctx.sections)
        renderer, kwargs = ctx.sections["hermes-swarm-bridge.worker"]
        prompt_text = renderer({"platform": "cli"})
        self.assertIn("coder-a", prompt_text)
        self.assertIn("worker", prompt_text)
        self.assertIn("swarm-42", prompt_text)
        self.assertIn("swarm_post", prompt_text)
        self.assertIn("board_cli.py", prompt_text)
        self.assertLessEqual(len(prompt_text), kwargs["max_chars"])
        self.assertEqual(renderer({"platform": "subagent"}), "")

    def test_disk_post_read_claim_release_status(self) -> None:
        env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": "swarm-disk",
            "HERMES_SWARM_AGENT": "worker-1",
            "HERMES_SWARM_ROLE": "worker",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        with _EnvPatch(env):
            ctx = _FakeCtx()
            self.plugin.register(ctx)

            # Post with mention in summary only; details & backticked @ignored must not populate mentions
            post_res = json.loads(
                ctx.tools["swarm_post"]["handler"]({
                    "summary": "@coordinator [done] `@ghost` Finished module",
                    "details": "Also checked @prose_only in details",
                    "kind": "handoff",
                    "channel": "build",
                    "files": ["a.py", "b.py"],
                    "reply_to": 1,
                })
            )
            self.assertTrue(post_res["ok"])
            self.assertEqual(post_res["source"], "disk")
            msg = post_res["message"]
            # Posting to a new non-main channel auto-emits a `create` control message at seq 1, then the post at seq 2
            self.assertEqual(msg["seq"], 2)
            self.assertEqual(msg["mentions"], ["coordinator"])
            self.assertEqual(msg["channel"], "#build")
            self.assertEqual(msg["files"], ["a.py", "b.py"])
            self.assertEqual(msg["replyTo"], 1)

            read_all = json.loads(ctx.tools["swarm_read"]["handler"]({"since": 0, "channel": "build", "limit": 10}))
            self.assertTrue(read_all["ok"])
            self.assertEqual(read_all["head"], 2)
            self.assertEqual(read_all["count"], 2)
            self.assertEqual(read_all["messages"][0]["control"]["op"], "create")
            self.assertEqual(read_all["messages"][1]["seq"], 2)

            read_after = json.loads(ctx.tools["swarm_read"]["handler"]({"since": 2}))
            self.assertEqual(read_after["count"], 0)

            claim_res = json.loads(ctx.tools["swarm_claim"]["handler"]({"scope": "src/app.py", "notes": "editing"}))
            self.assertTrue(claim_res["ok"])
            self.assertEqual(claim_res["claimed"]["holder"], "worker-1")
            self.assertIsNone(claim_res["conflictWarning"])

            # Conflict check by another agent
            with _EnvPatch({"HERMES_SWARM_AGENT": "worker-2"}):
                conflict_res = json.loads(ctx.tools["swarm_claim"]["handler"]({"scope": "src/app.py"}))
                self.assertTrue(conflict_res["ok"])
                self.assertEqual(conflict_res["conflictWarning"]["holder"], "worker-1")

            status_res = json.loads(ctx.tools["swarm_status"]["handler"]({}))
            self.assertTrue(status_res["ok"])
            self.assertEqual(status_res["head"], 2)
            self.assertIn("src/app.py", status_res["claims"])

            with _EnvPatch({"HERMES_SWARM_AGENT": "worker-2"}):
                rel_res = json.loads(ctx.tools["swarm_release"]["handler"]({"scope": "src/app.py"}))
                self.assertTrue(rel_res["ok"])
                self.assertEqual(rel_res["released"], ["src/app.py"])

    def test_http_first_and_disk_fallback(self) -> None:
        recorded_posts: list[tuple[str, dict]] = []
        tok_file = Path(self.tmp.name) / "internal_token"
        tok_file.write_text("secret-file-token\n", encoding="utf-8")

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or "0")
                raw = self.rfile.read(length).decode("utf-8")
                body = json.loads(raw)
                auth = self.headers.get("Authorization") or ""
                if self.path == "/internal/board/post":
                    if auth != "Bearer secret-file-token":
                        self.send_response(401)
                        self.end_headers()
                        return
                    recorded_posts.append((auth, body))
                    resp = json.dumps({"seq": 9, "id": "bmsg-http", "summary": body["summary"]}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(resp)))
                    self.end_headers()
                    self.wfile.write(resp)
                    return
                self.send_response(404)
                self.end_headers()

            def do_GET(self):
                if self.path.startswith("/board"):
                    snap = {
                        "swarm": "swarm-http",
                        "swarmId": "swarm-http",
                        "title": "HTTP Swarm",
                        "head": 9,
                        "etag": "abc123",
                        "members": [
                            {"name": "coordinator", "role": "coordinator", "status": "waiting", "lastReadSeq": 9},
                            {"name": "coder-http", "role": "worker", "status": "working", "steps": 3, "tps": 42.5},
                        ],
                        "messages": [
                            {"seq": 9, "sender": "coordinator", "summary": "@coder-http go", "mentions": ["coder-http"], "channel": "main"}
                        ],
                        "claims": {},
                        "channels": [],
                        "telemetry": {"activeWorkers": 1, "totalSteps": 3},
                    }
                    resp = json.dumps(snap).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(resp)))
                    self.end_headers()
                    self.wfile.write(resp)
                    return
                self.send_response(404)
                self.end_headers()

        server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            env = {
                **self.base_clear_env,
                "HERMES_SWARM_ID": "swarm-http",
                "HERMES_SWARM_AGENT": "coder-http",
                "HERMES_SWARM_ROLE": "worker",
                "HERMES_SWARM_HTTP_URL": f"http://127.0.0.1:{port}",
                "HERMES_SWARM_TOKEN_FILE": str(tok_file),
                "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
                "HERMES_SWARM_CONVERSATION_ID": "conv-123",
            }
            with _EnvPatch(env):
                ctx = _FakeCtx()
                self.plugin.register(ctx)

                post_res = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "@coordinator hello"}))
                self.assertTrue(post_res["ok"])
                self.assertEqual(post_res["source"], "http")
                self.assertEqual(len(recorded_posts), 1)
                self.assertEqual(recorded_posts[0][1]["conversationId"], "conv-123")

                read_res = json.loads(ctx.tools["swarm_read"]["handler"]({"since": 0}))
                self.assertTrue(read_res["ok"])
                self.assertEqual(read_res["source"], "http")
                self.assertEqual(read_res["messages"][0]["seq"], 9)

                status_res = json.loads(ctx.tools["swarm_status"]["handler"]({}))
                self.assertTrue(status_res["ok"])
                self.assertEqual(status_res["source"], "http")
                self.assertEqual(status_res["telemetry"]["activeWorkers"], 1)

                # /internal/board/claim returns 404 on server -> cleanly falls back to locked disk
                claim_res = json.loads(ctx.tools["swarm_claim"]["handler"]({"scope": "fallback.py"}))
                self.assertTrue(claim_res["ok"])
                self.assertEqual(claim_res["source"], "disk")

                # HTTP-less / dead-port fallback for swarm_post
                with _EnvPatch({"HERMES_SWARM_HTTP_URL": "http://127.0.0.1:1"}):
                    fb_post = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "disk fallback"}))
                    self.assertTrue(fb_post["ok"])
                    self.assertEqual(fb_post["source"], "disk")
        finally:
            server.shutdown()
            server.server_close()

    def test_swarm_wait_in_process_wakeup_and_timeout(self) -> None:
        env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": "swarm-wait",
            "HERMES_SWARM_AGENT": "worker-w",
            "HERMES_SWARM_ROLE": "worker",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        with _EnvPatch(env):
            ctx = _FakeCtx()
            self.plugin.register(ctx)

            # Timeout when nothing arrives
            timeout_res = json.loads(ctx.tools["swarm_wait"]["handler"]({"since": 0, "timeout_seconds": 0.2}))
            self.assertTrue(timeout_res["ok"])
            self.assertFalse(timeout_res["woken"])
            self.assertTrue(timeout_res["timedOut"])

            # Background thread posts an unrelated mention, then a matching mention
            def _delayed_post():
                time.sleep(0.12)
                with _EnvPatch({"HERMES_SWARM_AGENT": "coordinator", "HERMES_SWARM_ROLE": "coordinator"}):
                    ctx.tools["swarm_post"]["handler"]({"summary": "@someone-else ignore this"})
                    time.sleep(0.12)
                    ctx.tools["swarm_post"]["handler"]({"summary": "@worker-w wake up now"})

            poster = threading.Thread(target=_delayed_post, daemon=True)
            poster.start()
            wake_res = json.loads(
                ctx.tools["swarm_wait"]["handler"]({"since": 0, "mentions_only": True, "timeout_seconds": 3.0})
            )
            poster.join(timeout=2.0)
            self.assertTrue(wake_res["ok"])
            self.assertTrue(wake_res["woken"])
            self.assertFalse(wake_res["timedOut"])
            self.assertEqual(len(wake_res["messages"]), 1)
            self.assertIn("@worker-w", wake_res["messages"][0]["summary"])

    def test_swarm_wait_http_only_and_env_token_override(self) -> None:
        calls = {"get": 0, "post_auth": ""}

        class _HttpOnlyHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002
                return

            def do_POST(self):
                calls["post_auth"] = self.headers.get("Authorization") or ""
                resp = json.dumps({"seq": 5, "summary": "ok"}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)

            def do_GET(self):
                calls["get"] += 1
                if calls["get"] < 2:
                    snap = {"swarm": "swarm-http-wait", "head": 0, "etag": "e1", "messages": [], "members": [], "channels": []}
                else:
                    snap = {
                        "swarm": "swarm-http-wait",
                        "head": 2,
                        "etag": "e2",
                        "members": [{"name": "worker-h", "role": "worker", "status": "working"}],
                        "channels": [],
                        "messages": [{"seq": 2, "sender": "coordinator", "summary": "@all sync", "mentions": [], "all": True}],
                    }
                resp = json.dumps(snap).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)

        server = ThreadingHTTPServer(("127.0.0.1", 0), _HttpOnlyHandler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            env = {
                **self.base_clear_env,
                "HERMES_SWARM_ID": "swarm-http-wait",
                "HERMES_SWARM_AGENT": "worker-h",
                "HERMES_SWARM_HTTP_URL": f"http://127.0.0.1:{port}",
                "HERMES_SWARM_HTTP_TOKEN": "direct-env-token",
                "HERMES_SWARM_CONVERSATION_ID": "conv-wait",
            }
            with _EnvPatch(env):
                ctx = _FakeCtx()
                self.plugin.register(ctx)

                post_res = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "@coordinator ping"}))
                self.assertTrue(post_res["ok"])
                self.assertEqual(calls["post_auth"], "Bearer direct-env-token")

                wait_res = json.loads(ctx.tools["swarm_wait"]["handler"]({"since": 0, "timeout_seconds": 3.0}))
                self.assertTrue(wait_res["ok"])
                self.assertTrue(wait_res["woken"])
                self.assertEqual(wait_res["source"], "http")
                self.assertEqual(wait_res["messages"][0]["seq"], 2)
        finally:
            server.shutdown()
            server.server_close()

    def test_forbidden_terminology_zero_hits(self) -> None:
        hits: list[str] = []
        for path in PLUGIN_ROOT.rglob("*"):
            if not path.is_file() or "__pycache__" in path.parts:
                continue
            if path.resolve() == Path(__file__).resolve():
                # Skip the FORBIDDEN_TERMS tuple definition line itself; check the rest
                lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
                for idx, line in enumerate(lines, start=1):
                    if idx <= 30 and ("FORBIDDEN_TERMS" in line or any(f'"{t}"' in line for t in FORBIDDEN_TERMS)):
                        continue
                    for m in FORBIDDEN_RE.finditer(line):
                        hits.append(f"{path.name}:{idx}:{m.group(0)}")
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            for m in FORBIDDEN_RE.finditer(text):
                hits.append(f"{path.name}:{m.group(0)}")
        self.assertEqual(hits, [], f"Forbidden terminology found: {hits}")

    def test_phase3_channel_normalization_archived_block_and_swarm_channel(self) -> None:
        env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": "swarm-p3-ch",
            "HERMES_SWARM_AGENT": "worker-a",
            "HERMES_SWARM_ROLE": "worker",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        with _EnvPatch(env):
            ctx = _FakeCtx()
            self.plugin.register(ctx)

            # Create channel, subscribe worker-a & worker-b, check bare slug & null -> list control
            c_res = json.loads(ctx.tools["swarm_channel"]["handler"]({
                "action": "create",
                "channel": "#Backend",
                "members": ["worker-a"],
            }))
            self.assertTrue(c_res["ok"], msg=c_res)
            self.assertEqual(c_res["channel"]["name"], "#backend")

            # Ensure worker-b is a known member on the board before subscribe so prev/next transitions are tracked
            with _EnvPatch({"HERMES_SWARM_AGENT": "worker-b", "HERMES_SWARM_ROLE": "worker"}):
                ctx.tools["swarm_post"]["handler"]({"summary": "worker-b online"})

            s_res = json.loads(ctx.tools["swarm_channel"]["handler"]({
                "action": "subscribe",
                "channel": "backend",
                "members": ["worker-b"],
            }))
            self.assertTrue(s_res["ok"], msg=s_res)
            ctl = s_res.get("controlMessage") or {}
            self.assertEqual(ctl.get("channel"), "#backend")
            self.assertIsNone(ctl.get("control", {}).get("prev", {}).get("worker-b"))
            self.assertEqual(ctl.get("control", {}).get("next", {}).get("worker-b"), ["backend"])

            # Post with to + thread_id on main/general normalization
            p_main = json.loads(ctx.tools["swarm_post"]["handler"]({
                "summary": "Ping in general",
                "channel": "#general",
                "to": ["worker-b"],
                "thread_id": "thr-1",
            }))
            self.assertTrue(p_main["ok"], msg=p_main)
            self.assertEqual(p_main["message"]["channel"], "#general")
            self.assertEqual(p_main["message"]["threadId"], "thr-1")
            self.assertIn("worker-b", p_main["message"]["mentions"])

            # Archive channel and verify posting to archived channel is blocked
            a_res = json.loads(ctx.tools["swarm_channel"]["handler"]({
                "action": "archive",
                "channel": "backend",
            }))
            self.assertTrue(a_res["ok"], msg=a_res)

            blocked_post = json.loads(ctx.tools["swarm_post"]["handler"]({
                "summary": "Should fail on archived",
                "channel": "backend",
            }))
            self.assertFalse(blocked_post["ok"])
            self.assertIn("archived", blocked_post["error"].lower())

            # Unsubscribe back to role default -> control.next is None
            c2 = json.loads(ctx.tools["swarm_channel"]["handler"]({"action": "create", "channel": "qa"}))
            self.assertTrue(c2["ok"])
            with _EnvPatch({"HERMES_SWARM_AGENT": "worker-c", "HERMES_SWARM_ROLE": "worker"}):
                ctx.tools["swarm_post"]["handler"]({"summary": "worker-c online"})
            sub_qa = json.loads(ctx.tools["swarm_channel"]["handler"]({
                "action": "subscribe",
                "channel": "qa",
                "members": ["worker-c"],
            }))
            self.assertTrue(sub_qa["ok"])
            unsub_qa = json.loads(ctx.tools["swarm_channel"]["handler"]({
                "action": "unsubscribe",
                "channel": "qa",
                "members": ["worker-c"],
            }))
            self.assertTrue(unsub_qa["ok"])
            self.assertIsNone((unsub_qa.get("controlMessage") or {}).get("control", {}).get("next", {}).get("worker-c"))

    def test_phase3_swarm_read_seqs_status_channel_star_sees_in_and_wait_cursors(self) -> None:
        env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": "swarm-p3-read",
            "HERMES_SWARM_AGENT": "worker-1",
            "HERMES_SWARM_ROLE": "worker",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        with _EnvPatch(env):
            ctx = _FakeCtx()
            self.plugin.register(ctx)

            # Seed worker-1 and worker-2 on board.json (as SwarmRuntime does on spawn)
            (self.board_dir / "board.json").write_text(
                json.dumps({
                    "swarmId": "swarm-p3-read",
                    "nextSeq": 1,
                    "members": {
                        "worker-1": {"name": "worker-1", "role": "worker", "state": "waiting", "channels": None, "lastReadSeq": 0},
                        "worker-2": {"name": "worker-2", "role": "worker", "state": "waiting", "channels": None, "lastReadSeq": 0},
                    },
                    "channels": {},
                    "messages": [],
                }),
                encoding="utf-8",
            )

            # Create two channels: frontend (worker-1 subscribed) and secret-ops (worker-2 only)
            ctx.tools["swarm_channel"]["handler"]({"action": "create", "channel": "frontend", "members": ["worker-1"]})
            ctx.tools["swarm_channel"]["handler"]({"action": "create", "channel": "secret-ops", "members": ["worker-2"]})

            with _EnvPatch({"HERMES_SWARM_AGENT": "coordinator", "HERMES_SWARM_ROLE": "coordinator"}):
                p1 = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "Main post", "channel": "main"}))
                p2 = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "Frontend post", "channel": "frontend"}))
                p3 = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "Secret ops post", "channel": "secret-ops"}))

            # Default read for worker-1 sees main + frontend, NOT secret-ops
            r_def = json.loads(ctx.tools["swarm_read"]["handler"]({"since": 0}))
            self.assertTrue(r_def["ok"])
            summaries_def = [m["summary"] for m in r_def["messages"]]
            self.assertIn("Main post", summaries_def)
            self.assertIn("Frontend post", summaries_def)
            self.assertNotIn("Secret ops post", summaries_def)

            # channel="*" sees all channels including secret-ops
            r_star = json.loads(ctx.tools["swarm_read"]["handler"]({"since": 0, "channel": "*"}))
            self.assertIn("Secret ops post", [m["summary"] for m in r_star["messages"]])

            # seqs targeted lookup
            r_seqs = json.loads(ctx.tools["swarm_read"]["handler"]({"seqs": [p1["message"]["seq"], p3["message"]["seq"]]}))
            self.assertEqual([m["seq"] for m in r_seqs["messages"]], [p1["message"]["seq"], p3["message"]["seq"]])

            # status=True returns roster
            r_status = json.loads(ctx.tools["swarm_read"]["handler"]({"status": True}))
            self.assertTrue(r_status["ok"])
            self.assertIn("members", r_status)

            # swarm_status visibleUnread excludes secret-ops for worker-1
            st_w1 = json.loads(ctx.tools["swarm_status"]["handler"]({}))
            self.assertTrue(st_w1["ok"])
            w1_status = next(m for m in st_w1["members"] if m["name"] == "worker-1")
            self.assertIn("visibleUnread", w1_status)
            self.assertLess(w1_status["visibleUnread"], st_w1["head"])

            # mentions_only=False wait ignores secret-ops and wakes on frontend/main, updating waitSince & lastReadSeq
            def _post_secret_then_frontend():
                time.sleep(0.1)
                with _EnvPatch({"HERMES_SWARM_AGENT": "worker-2", "HERMES_SWARM_ROLE": "worker"}):
                    ctx.tools["swarm_post"]["handler"]({"summary": "Ignore secret channel", "channel": "secret-ops"})
                    time.sleep(0.1)
                    ctx.tools["swarm_post"]["handler"]({"summary": "Wake frontend channel", "channel": "frontend"})

            t = threading.Thread(target=_post_secret_then_frontend, daemon=True)
            t.start()
            w_res = json.loads(ctx.tools["swarm_wait"]["handler"]({
                "since": p3["message"]["seq"],
                "mentions_only": False,
                "timeout_seconds": 3.0,
            }))
            t.join(timeout=2.0)
            self.assertTrue(w_res["ok"])
            self.assertTrue(w_res["woken"], msg=w_res)
            self.assertEqual([m["summary"] for m in w_res["messages"]], ["Wake frontend channel"])

            board_doc = json.loads((self.board_dir / "board.json").read_text(encoding="utf-8"))
            w1_mem = board_doc["members"]["worker-1"]
            self.assertIsNone(w1_mem.get("waitSince"))
            self.assertEqual(w1_mem.get("lastReadSeq"), w_res["messages"][0]["seq"])

    def test_phase3_atomic_file_lock_dead_pid_fast_eviction_and_released_takeover(self) -> None:
        from hermes_swarm_bridge import swarm_tools

        lock_path = Path(self.tmp.name) / "p3_bridge.lock"
        # Dead PID with fresh timestamp (NOT stale by age!) must be evicted immediately (<1s, no 30s stall)
        lock_path.write_text(
            json.dumps({"pid": 9999999, "ts": time.time(), "token": "dead-holder", "released": False}),
            encoding="utf-8",
        )
        t0 = time.monotonic()
        with swarm_tools._atomic_file_lock(lock_path, timeout=2.0, stale_seconds=30.0):
            elapsed = time.monotonic() - t0
            self.assertLess(elapsed, 1.0, f"Dead PID eviction stalled for {elapsed:.2f}s")
        self.assertFalse(lock_path.exists())

        # Concurrent released-lock takeover under 8 threads must not corrupt counter
        counter_file = Path(self.tmp.name) / "p3_counter.json"
        counter_file.write_text(json.dumps({"n": 0}), encoding="utf-8")

        def _inc(_idx: int) -> None:
            for _ in range(10):
                with swarm_tools._atomic_file_lock(lock_path, timeout=5.0):
                    cur = json.loads(counter_file.read_text(encoding="utf-8"))
                    cur["n"] += 1
                    counter_file.write_text(json.dumps(cur), encoding="utf-8")

        threads = [threading.Thread(target=_inc, args=(i,)) for i in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=10.0)
        self.assertEqual(json.loads(counter_file.read_text(encoding="utf-8"))["n"], 80)

    def test_http_4xx_rejection_no_silent_disk_fallback_and_contextvars_isolation(self) -> None:
        from hermes_swarm_bridge import swarm_tools

        class _RejectHandler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    _ = self.rfile.read(length)
                if self.path.endswith("/internal/board/claim"):
                    code, body = 409, {"ok": False, "error": "Scope conflict"}
                elif self.path.endswith("/internal/board/post"):
                    code, body = 403, {"ok": False, "error": "Forbidden boardDir"}
                else:
                    code, body = 400, {"ok": False, "error": "Bad request"}
                raw = json.dumps(body).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, format, *args):
                return

        srv = ThreadingHTTPServer(("127.0.0.1", 0), _RejectHandler)
        srv_thread = threading.Thread(target=srv.serve_forever, daemon=True)
        srv_thread.start()
        try:
            url = f"http://127.0.0.1:{srv.server_address[1]}"
            ctx = _FakeCtx()
            self.plugin.register(ctx)
            with _EnvPatch({
                "HERMES_SWARM_ID": "swarm-1",
                "HERMES_SWARM_AGENT": "worker-1",
                "HERMES_SWARM_ROLE": "worker",
                "HERMES_SWARM_HTTP_URL": url,
                "HERMES_SWARM_HTTP_TOKEN": "tok",
                "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
            }):
                res_409 = json.loads(ctx.tools["swarm_claim"]["handler"]({"scope": "src/a.py"}))
                self.assertFalse(res_409["ok"])
                self.assertEqual(res_409.get("status"), 409)

                res_403 = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "Should not hit disk"}))
                self.assertFalse(res_403["ok"])
                self.assertEqual(res_403.get("status"), 403)

                res_400 = json.loads(ctx.tools["swarm_release"]["handler"]({"scope": "src/a.py"}))
                self.assertFalse(res_400["ok"])
                self.assertEqual(res_400.get("status"), 400)

            # None of the HTTP 400/403/409 rejections may silently write board.json on disk
            self.assertFalse((self.board_dir / "board.json").exists())
        finally:
            srv.shutdown()
            srv.server_close()

        # contextvars turn isolation across concurrent threads
        observed: dict[str, list[str]] = {"t1": [], "t2": []}
        barrier = threading.Barrier(2)

        def _worker(tag: str, agent_name: str, swarm_name: str) -> None:
            with swarm_tools.turn_context({"agent": agent_name, "swarm_id": swarm_name}):
                barrier.wait(timeout=3.0)
                for _ in range(5):
                    cur = swarm_tools.get_turn_context()
                    observed[tag].append(f"{cur.get('agent')}:{cur.get('swarm_id')}")
                    time.sleep(0.01)

        th1 = threading.Thread(target=_worker, args=("t1", "alpha", "sw-alpha"))
        th2 = threading.Thread(target=_worker, args=("t2", "beta", "sw-beta"))
        th1.start()
        th2.start()
        th1.join(timeout=5.0)
        th2.join(timeout=5.0)
        self.assertEqual(observed["t1"], ["alpha:sw-alpha"] * 5)
        self.assertEqual(observed["t2"], ["beta:sw-beta"] * 5)
        self.assertEqual(swarm_tools.get_turn_context(), {})

    def test_v3_archive_board_reads_and_tail_pagination(self) -> None:
        import inspect
        from unittest.mock import patch
        from hermes_swarm_bridge import swarm_tools

        # Default limit=100 in signature and tool schema
        sig = inspect.signature(swarm_tools.swarm_read)
        self.assertEqual(sig.parameters["limit"].default, 100)
        read_schema = swarm_tools.schema("swarm_read")
        self.assertEqual(read_schema["parameters"]["properties"]["limit"]["default"], 100)

        env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": "swarm-arch-v3",
            "HERMES_SWARM_AGENT": "worker-1",
            "HERMES_SWARM_ROLE": "worker",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        with _EnvPatch(env), patch.object(swarm_tools, "MAX_BOARD_MESSAGES", 5):
            ctx = _FakeCtx()
            self.plugin.register(ctx)
            for idx in range(1, 9):
                res = json.loads(ctx.tools["swarm_post"]["handler"]({
                    "summary": f"msg-{idx} {'alpha-needle' if idx == 2 else 'body'}",
                }))
                self.assertTrue(res["ok"])

            arch_file = self.board_dir / "board_archive.jsonl"
            self.assertTrue(arch_file.is_file())
            archived = swarm_tools._read_archive_messages(arch_file)
            self.assertEqual([m["seq"] for m in archived], [1, 2, 3])

            # since=0 returns latest N tail from live board
            tail_res = json.loads(ctx.tools["swarm_read"]["handler"]({"since": 0, "limit": 2}))
            self.assertTrue(tail_res["ok"])
            self.assertEqual([m["seq"] for m in tail_res["messages"]], [7, 8])

            # include_archive=True merges archive + live by seq
            all_res = json.loads(ctx.tools["swarm_read"]["handler"]({
                "since": 0,
                "include_archive": True,
                "full": True,
            }))
            self.assertTrue(all_res["ok"])
            self.assertEqual([m["seq"] for m in all_res["messages"]], [1, 2, 3, 4, 5, 6, 7, 8])

            # before_seq + search across archive
            search_res = json.loads(ctx.tools["swarm_read"]["handler"]({
                "include_archive": True,
                "before_seq": 5,
                "search": "alpha-needle",
            }))
            self.assertTrue(search_res["ok"])
            self.assertEqual([m["seq"] for m in search_res["messages"]], [2])

    def test_v3_bridge_hooks_compaction_and_jsonl_logs(self) -> None:
        from hermes_swarm_bridge import swarm_tools

        env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": "swarm-hooks-v3",
            "HERMES_SWARM_AGENT": "worker-h",
            "HERMES_SWARM_ROLE": "worker",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        with _EnvPatch(env):
            ctx = _FakeCtx()
            self.plugin.register(ctx)
            self.assertEqual(
                set(ctx.hooks.keys()),
                {"post_compaction", "pre_llm_call", "pre_verify", "post_tool_call", "transform_tool_result"},
            )

            # Populate state via swarm_claim + swarm_post
            c_res = json.loads(ctx.tools["swarm_claim"]["handler"]({"scope": "src/v3.py"}))
            self.assertTrue(c_res["ok"])
            p_res = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "initial post", "channel": "dev"}))
            self.assertTrue(p_res["ok"])
            seq_num = p_res["message"]["seq"]

            # pre_verify warns when unreleased claims exist (attempt=0 only)
            pv0 = ctx.hooks["pre_verify"](attempt=0)
            self.assertIsNotNone(pv0)
            self.assertEqual(pv0["action"], "continue")
            self.assertIn("src/v3.py", pv0["message"])
            self.assertIsNone(ctx.hooks["pre_verify"](attempt=1))

            # post_compaction + pre_llm_call preserve swarm/claims/lastReadSeq and write bridge.jsonl
            ctx.hooks["post_compaction"](
                session_id="sess-hook-1",
                kind="auto",
                reason="threshold",
                before_tokens=200_000,
                after_tokens=40_000,
                messages_removed=12,
            )
            briefing_out = ctx.hooks["pre_llm_call"](session_id="sess-hook-1")
            self.assertIsNotNone(briefing_out)
            ctx_text = briefing_out["context"]
            self.assertIn("swarm_id=swarm-hooks-v3", ctx_text)
            self.assertIn("agent=worker-h", ctx_text)
            self.assertIn("channel=#dev", ctx_text)
            self.assertIn(f"lastReadSeq={seq_num}", ctx_text)
            self.assertIn("src/v3.py", ctx_text)
            # Consumed once
            self.assertIsNone(ctx.hooks["pre_llm_call"](session_id="sess-hook-1"))

            # post_tool_call updates lastHeartbeatAt in board.json for registered member
            bpath = self.board_dir / "board.json"
            bdata = json.loads(bpath.read_text(encoding="utf-8"))
            bdata.setdefault("members", {})["worker-h"] = {"name": "worker-h", "role": "worker", "state": "working"}
            bpath.write_text(json.dumps(bdata), encoding="utf-8")
            with swarm_tools._STATE_LOCK:
                swarm_tools._HEARTBEAT_TIMESTAMPS.clear()
            ctx.hooks["post_tool_call"](tool_name="swarm_read")
            board_doc = json.loads(bpath.read_text(encoding="utf-8"))
            self.assertTrue(board_doc["members"]["worker-h"].get("lastHeartbeatAt"))

            # Releasing claim clears pre_verify warning
            r_res = json.loads(ctx.tools["swarm_release"]["handler"]({"scope": "src/v3.py"}))
            self.assertTrue(r_res["ok"])
            self.assertIsNone(ctx.hooks["pre_verify"](attempt=0))

            # bridge.jsonl written with post_compaction event
            bridge_log = self.board_dir / "logs" / "bridge.jsonl"
            self.assertTrue(bridge_log.is_file())
            events = [json.loads(line) for line in bridge_log.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertTrue(any(ev.get("event") == "post_compaction" and ev.get("sessionId") == "sess-hook-1" for ev in events))

    def test_swarm_wait_avoids_redundant_disk_writes_and_prefers_lightweight_read(self) -> None:
        from unittest.mock import patch
        from hermes_swarm_bridge import swarm_tools

        bpath = self.board_dir / "board.json"
        bpath.write_text(
            json.dumps({
                "swarmId": "swarm-opt",
                "nextSeq": 2,
                "members": {
                    "worker-1": {"name": "worker-1", "role": "worker", "state": "working", "lastReadSeq": 1},
                },
                "channels": {},
                "messages": [
                    {"seq": 1, "sender": "coordinator", "summary": "init", "channel": "#general"},
                ],
            }),
            encoding="utf-8",
        )

        env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": "swarm-opt",
            "HERMES_SWARM_AGENT": "worker-1",
            "HERMES_SWARM_ROLE": "worker",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        orig_atomic_write = swarm_tools._atomic_write_json
        write_calls = 0

        def _counting_write(path, data):
            nonlocal write_calls
            if Path(path) == bpath:
                write_calls += 1
            return orig_atomic_write(path, data)

        with _EnvPatch(env), patch.object(swarm_tools, "_atomic_write_json", side_effect=_counting_write):
            ctx = _FakeCtx()
            self.plugin.register(ctx)
            # Worker timeout performs ZERO board.json writes (no enter/exit waitSince churn)
            t_res = json.loads(ctx.tools["swarm_wait"]["handler"]({"since": 1, "timeout_seconds": 0.15}))
            self.assertTrue(t_res["ok"])
            self.assertTrue(t_res["timedOut"])
            self.assertEqual(write_calls, 0, f"Expected 0 board.json writes on worker wait timeout, got {write_calls}")

            # Worker wake performs at most 1 board.json write (to advance lastReadSeq)
            def _delayed_wake():
                time.sleep(0.08)
                with _EnvPatch({"HERMES_SWARM_AGENT": "coordinator", "HERMES_SWARM_ROLE": "coordinator"}):
                    ctx.tools["swarm_post"]["handler"]({"summary": "@worker-1 ping"})

            th = threading.Thread(target=_delayed_wake, daemon=True)
            th.start()
            write_calls = 0
            w_res = json.loads(ctx.tools["swarm_wait"]["handler"]({"since": 1, "timeout_seconds": 2.0}))
            th.join(timeout=2.0)
            self.assertTrue(w_res["woken"])
            # 1 write from coordinator's swarm_post + 1 write from worker-1 advancing lastReadSeq = 2 total (never 4)
            self.assertEqual(write_calls, 2, f"Expected 1 post write + 1 wake cursor write, got {write_calls}")

        # HTTP polling uses lightweight /internal/board/read rather than /board
        seen_paths: list[str] = []

        class _LightweightReadHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002
                return

            def do_GET(self):
                seen_paths.append(self.path)
                if self.path.startswith("/internal/board/read"):
                    if len(seen_paths) < 2:
                        body = {"ok": True, "swarmId": "swarm-lw", "head": 0, "count": 0, "messages": []}
                    else:
                        body = {
                            "ok": True,
                            "swarmId": "swarm-lw",
                            "head": 3,
                            "count": 1,
                            "messages": [{"seq": 3, "sender": "coordinator", "summary": "@worker-lw ready", "mentions": ["worker-lw"]}],
                        }
                    raw = json.dumps(body).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return
                self.send_response(404)
                self.end_headers()

        srv = ThreadingHTTPServer(("127.0.0.1", 0), _LightweightReadHandler)
        port = srv.server_address[1]
        st = threading.Thread(target=srv.serve_forever, daemon=True)
        st.start()
        try:
            http_env = {
                **self.base_clear_env,
                "HERMES_SWARM_ID": "swarm-lw",
                "HERMES_SWARM_AGENT": "worker-lw",
                "HERMES_SWARM_HTTP_URL": f"http://127.0.0.1:{port}",
                "HERMES_SWARM_HTTP_TOKEN": "tok-lw",
                "HERMES_SWARM_CONVERSATION_ID": "conv-lw",
            }
            with _EnvPatch(http_env):
                ctx = _FakeCtx()
                self.plugin.register(ctx)
                res = json.loads(ctx.tools["swarm_wait"]["handler"]({"since": 0, "timeout_seconds": 2.0}))
                self.assertTrue(res["ok"])
                self.assertTrue(res["woken"])
                self.assertTrue(all(p.startswith("/internal/board/read") for p in seen_paths), seen_paths)
                self.assertFalse(any(p.startswith("/board?") for p in seen_paths), seen_paths)
        finally:
            srv.shutdown()
            srv.server_close()

    def test_swarm_wait_retries_transient_status_error_and_refreshes_channels(self) -> None:
        from unittest.mock import patch
        import urllib.parse
        from hermes_swarm_bridge import swarm_tools

        status_calls = 0

        class _ChannelRefreshHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002
                return

            def do_GET(self):
                nonlocal status_calls
                parsed = urllib.parse.urlparse(self.path)
                qs = urllib.parse.parse_qs(parsed.query)
                if parsed.path == "/internal/board/read" and "true" in qs.get("status", []):
                    status_calls += 1
                    if status_calls == 1:
                        raw = json.dumps({"ok": False, "error": "transient error"}).encode("utf-8")
                        self.send_response(500)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                        return
                    if status_calls == 2:
                        body = {
                            "ok": True,
                            "swarmId": "swarm-ch-ref",
                            "head": 1,
                            "members": [{"name": "worker-r", "role": "worker", "channels": []}],
                            "channels": [{"name": "#deploy", "archived": False, "subscribers": []}],
                        }
                    else:
                        body = {
                            "ok": True,
                            "swarmId": "swarm-ch-ref",
                            "head": 1,
                            "members": [{"name": "worker-r", "role": "worker", "channels": ["#deploy"]}],
                            "channels": [{"name": "#deploy", "archived": False, "subscribers": ["worker-r"]}],
                        }
                    raw = json.dumps(body).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return

                if parsed.path == "/internal/board/read":
                    body = {
                        "ok": True,
                        "swarmId": "swarm-ch-ref",
                        "head": 1,
                        "count": 1,
                        "messages": [
                            {"seq": 1, "sender": "coordinator", "channel": "#deploy", "summary": "deploy ping"}
                        ],
                    }
                    raw = json.dumps(body).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return

                self.send_response(404)
                self.end_headers()

        srv = ThreadingHTTPServer(("127.0.0.1", 0), _ChannelRefreshHandler)
        port = srv.server_address[1]
        st = threading.Thread(target=srv.serve_forever, daemon=True)
        st.start()
        try:
            http_env = {
                **self.base_clear_env,
                "HERMES_SWARM_ID": "swarm-ch-ref",
                "HERMES_SWARM_AGENT": "worker-r",
                "HERMES_SWARM_ROLE": "worker",
                "HERMES_SWARM_HTTP_URL": f"http://127.0.0.1:{port}",
                "HERMES_SWARM_HTTP_TOKEN": "tok-ch",
                "HERMES_SWARM_CONVERSATION_ID": "conv-ch",
            }
            with _EnvPatch(http_env), patch.object(swarm_tools, "CHANNEL_META_REFRESH_SECONDS", 0.05):
                ctx = _FakeCtx()
                self.plugin.register(ctx)
                res = json.loads(
                    ctx.tools["swarm_wait"]["handler"]({
                        "since": 0,
                        "mentions_only": False,
                        "timeout_seconds": 4.0,
                    })
                )
                self.assertTrue(res["ok"], msg=res)
                self.assertTrue(res["woken"], msg=res)
                self.assertEqual(res["source"], "http")
                self.assertEqual(res["messages"][0]["seq"], 1)
                self.assertGreaterEqual(status_calls, 3)
        finally:
            srv.shutdown()
            srv.server_close()

    def test_swarm_wait_coordinator_sets_waiting_state_and_flag_at_since_zero(self) -> None:
        bpath = self.board_dir / "board.json"
        bpath.write_text(
            json.dumps({
                "swarmId": "swarm-coord-wait",
                "nextSeq": 1,
                "members": {
                    "coordinator": {"name": "coordinator", "role": "coordinator", "state": "working", "lastReadSeq": 0},
                    "worker-1": {"name": "worker-1", "role": "worker", "state": "working", "lastReadSeq": 0},
                },
                "channels": {},
                "messages": [],
            }),
            encoding="utf-8",
        )

        coord_env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": "swarm-coord-wait",
            "HERMES_SWARM_AGENT": "coordinator",
            "HERMES_SWARM_ROLE": "coordinator",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        with _EnvPatch(coord_env):
            ctx = _FakeCtx()
            self.plugin.register(ctx)

            wait_result: dict[str, Any] = {}

            def _run_wait() -> None:
                wait_result["res"] = json.loads(
                    ctx.tools["swarm_wait"]["handler"]({"since": 0, "timeout_seconds": 3.0})
                )

            wt = threading.Thread(target=_run_wait, daemon=True)
            wt.start()

            deadline = time.monotonic() + 2.0
            mid_coord: dict[str, Any] = {}
            while time.monotonic() < deadline:
                if bpath.is_file():
                    doc = json.loads(bpath.read_text(encoding="utf-8"))
                    cand = (doc.get("members") or {}).get("coordinator") or {}
                    if cand.get("waiting") is True:
                        mid_coord = cand
                        break
                time.sleep(0.02)

            self.assertEqual(mid_coord.get("state"), "waiting", msg=mid_coord)
            self.assertIs(mid_coord.get("waiting"), True, msg=mid_coord)
            self.assertEqual(mid_coord.get("waitSince"), "0", msg=mid_coord)

            with _EnvPatch({"HERMES_SWARM_AGENT": "worker-1", "HERMES_SWARM_ROLE": "worker"}):
                ctx.tools["swarm_post"]["handler"]({"summary": "@coordinator [done] ready"})

            wt.join(timeout=3.0)
            res = wait_result.get("res") or {}
            self.assertTrue(res.get("ok"), msg=res)
            self.assertTrue(res.get("woken"), msg=res)

            after_wake = (json.loads(bpath.read_text(encoding="utf-8")).get("members") or {}).get("coordinator") or {}
            self.assertEqual(after_wake.get("state"), "working", msg=after_wake)
            self.assertIs(after_wake.get("waiting"), False, msg=after_wake)
            self.assertIsNone(after_wake.get("waitSince"), msg=after_wake)

            # Timeout path also restores working state and clears waiting/waitSince
            t_res = json.loads(
                ctx.tools["swarm_wait"]["handler"]({"since": res["head"], "timeout_seconds": 0.15})
            )
            self.assertTrue(t_res["ok"])
            self.assertTrue(t_res["timedOut"])
            after_to = (json.loads(bpath.read_text(encoding="utf-8")).get("members") or {}).get("coordinator") or {}
            self.assertEqual(after_to.get("state"), "working", msg=after_to)
            self.assertIs(after_to.get("waiting"), False, msg=after_to)
            self.assertIsNone(after_to.get("waitSince"), msg=after_to)

    def test_p0_lock_token_verified_eviction_release_win_handle_and_401_env_token_file_fallback(self) -> None:
        import ctypes
        import sys
        from hermes_swarm_bridge import swarm_tools

        # 1. _pid_alive uses 64-bit-safe c_void_p restype on Windows and reports live/dead accurately
        if sys.platform == "win32":
            self.assertIs(swarm_tools._K32.OpenProcess.restype, ctypes.c_void_p)
        self.assertTrue(swarm_tools._pid_alive(os.getpid()))
        self.assertFalse(swarm_tools._pid_alive(9999999))

        # 2. _reap_lock_file refuses to evict a lock whose on-disk token changed (new live holder)
        lock_path = Path(self.tmp.name) / "token_verify.lock"
        live_payload = {"pid": os.getpid(), "ts": time.time(), "token": "live-holder-token", "released": False}
        lock_path.write_text(json.dumps(live_payload), encoding="utf-8")
        self.assertFalse(
            swarm_tools._reap_lock_file(lock_path, expected_token="stale-dead-token"),
            "Must NOT evict lock when on-disk token no longer matches expected stale token",
        )
        self.assertTrue(lock_path.is_file())
        self.assertEqual(json.loads(swarm_tools._read_text_shared(lock_path))["token"], "live-holder-token")

        # Matching token DOES reap cleanly without leaving .evicted files
        self.assertTrue(swarm_tools._reap_lock_file(lock_path, expected_token="live-holder-token"))
        self.assertFalse(lock_path.exists())
        self.assertEqual(list(Path(self.tmp.name).glob("*.evicted.*")), [])

        # 3. _atomic_file_lock finally: must NOT unlink or overwrite another holder's lockfile
        release_lock_path = Path(self.tmp.name) / "release_verify.lock"
        other_holder = {"pid": os.getpid(), "ts": time.time(), "token": "foreign-holder-tok", "released": False}
        with swarm_tools._atomic_file_lock(release_lock_path, timeout=2.0):
            # Simulate external eviction + takeover by another process while we held the lock
            release_lock_path.write_text(json.dumps(other_holder), encoding="utf-8")
        self.assertTrue(release_lock_path.is_file(), "finally: must not unlink another holder's lock")
        after_doc = json.loads(swarm_tools._read_text_shared(release_lock_path))
        self.assertEqual(after_doc["token"], "foreign-holder-tok")
        self.assertIs(after_doc["released"], False, "finally: must not overwrite another holder's lock as released")
        release_lock_path.unlink(missing_ok=True)

        # 4. _http_request_json reloads token_file on 401 EVEN WHEN has_env_token=True
        tok_file = Path(self.tmp.name) / "fresh_internal_token"
        tok_file.write_text("fresh-disk-token-401\n", encoding="utf-8")
        seen_auths: list[str] = []

        class _AuthFallbackHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or "0")
                _ = self.rfile.read(length)
                auth = self.headers.get("Authorization") or ""
                seen_auths.append(auth)
                if auth != "Bearer fresh-disk-token-401":
                    raw = json.dumps({"ok": False, "error": "unauthorized"}).encode("utf-8")
                    self.send_response(401)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return
                raw = json.dumps({"seq": 77, "id": "bmsg-401-ok", "summary": "recovered"}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        srv = ThreadingHTTPServer(("127.0.0.1", 0), _AuthFallbackHandler)
        port = srv.server_address[1]
        st = threading.Thread(target=srv.serve_forever, daemon=True)
        st.start()
        try:
            # Direct _http_request_json call with has_env_token=True
            direct_cfg = {"token": "stale-env-tok", "token_file": str(tok_file), "has_env_token": "1"}
            res = swarm_tools._http_request_json(
                f"http://127.0.0.1:{port}/internal/board/post",
                method="POST",
                token="stale-env-tok",
                payload={"summary": "hello"},
                token_file=str(tok_file),
                has_env_token=True,
                cfg=direct_cfg,
            )
            self.assertTrue(res["ok"], msg=res)
            self.assertEqual(res["status"], 200)
            self.assertEqual(direct_cfg["token"], "fresh-disk-token-401")
            self.assertEqual(seen_auths, ["Bearer stale-env-tok", "Bearer fresh-disk-token-401"])

            # End-to-end swarm_post with stale HERMES_SWARM_HTTP_TOKEN + valid HERMES_SWARM_TOKEN_FILE
            seen_auths.clear()
            env = {
                **self.base_clear_env,
                "HERMES_SWARM_ID": "swarm-401-env",
                "HERMES_SWARM_AGENT": "coder-401",
                "HERMES_SWARM_HTTP_URL": f"http://127.0.0.1:{port}",
                "HERMES_SWARM_HTTP_TOKEN": "stale-env-tok",
                "HERMES_SWARM_TOKEN_FILE": str(tok_file),
                "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
            }
            with _EnvPatch(env):
                ctx = _FakeCtx()
                self.plugin.register(ctx)
                post_res = json.loads(ctx.tools["swarm_post"]["handler"]({"summary": "env token fallback"}))
                self.assertTrue(post_res["ok"], msg=post_res)
                self.assertEqual(post_res["source"], "http")
                self.assertEqual(post_res["message"]["seq"], 77)
                self.assertEqual(seen_auths, ["Bearer stale-env-tok", "Bearer fresh-disk-token-401"])
        finally:
            srv.shutdown()
            srv.server_close()

    def test_m6_reap_lock_file_rejects_live_token_when_expected_token_is_none(self) -> None:
        from hermes_swarm_bridge import swarm_tools

        # Live holder lock with a token MUST NOT be reaped when expected_token is None
        live_lock = Path(self.tmp.name) / "m6_live_token.lock"
        live_payload = {"pid": os.getpid(), "ts": time.time(), "token": "live-holder-uuid", "released": False}
        live_lock.write_text(json.dumps(live_payload), encoding="utf-8")
        self.assertFalse(
            swarm_tools._reap_lock_file(live_lock, expected_token=None),
            "Must NOT reap a live lock carrying a token when expected_token is None",
        )
        self.assertTrue(live_lock.is_file())
        self.assertEqual(json.loads(swarm_tools._read_text_shared(live_lock))["token"], "live-holder-uuid")

        # Legacy tokenless JSON lockfile (cur_token is None) still reaps cleanly when expected_token is None
        legacy_json_lock = Path(self.tmp.name) / "m6_legacy_json.lock"
        legacy_json_lock.write_text(json.dumps({"pid": 9999999, "ts": 1.0}), encoding="utf-8")
        self.assertTrue(swarm_tools._reap_lock_file(legacy_json_lock, expected_token=None))
        self.assertFalse(legacy_json_lock.exists())

        # Corrupt / non-JSON tokenless lockfile (cur_token is None) also reaps cleanly
        corrupt_lock = Path(self.tmp.name) / "m6_corrupt.lock"
        corrupt_lock.write_text("stale-non-json", encoding="utf-8")
        self.assertTrue(swarm_tools._reap_lock_file(corrupt_lock, expected_token=None))
        self.assertFalse(corrupt_lock.exists())
        self.assertEqual(list(Path(self.tmp.name).glob("*.reap.*")), [])

    def test_pre_llm_call_surfaces_mid_turn_pending_mentions_once(self) -> None:
        from hermes_swarm_bridge import swarm_tools

        swarm_id = "swarm-midturn-inbox"
        board_file = self.board_dir / "board.json"
        board_doc = {
            "schemaVersion": 2,
            "swarmId": swarm_id,
            "nextSeq": 7,
            "members": {
                "impl-1": {
                    "name": "impl-1",
                    "role": "worker",
                    "runtime": "hermes",
                    "state": "working",
                    "lastReadSeq": 5,
                }
            },
            "pendingMentions": {
                "impl-1": [
                    {
                        "seq": 6,
                        "sender": "coordinator",
                        "summary": "STOP run_ab.sh and apply kernel fix first",
                        "details": "Switch kernel signature back before running benchmarks.",
                    }
                ]
            },
            "messages": [],
        }
        board_file.write_text(json.dumps(board_doc), encoding="utf-8")

        env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": swarm_id,
            "HERMES_SWARM_AGENT": "impl-1",
            "HERMES_SWARM_ROLE": "worker",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        with _EnvPatch(env):
            ctx = _FakeCtx()
            self.plugin.register(ctx)
            hook = ctx.hooks["pre_llm_call"]

            # First mid-turn LLM call immediately surfaces unread #6
            res1 = hook(session_id="sess-midturn")
            self.assertIsNotNone(res1)
            self.assertIn("#6 from @coordinator", res1["context"])
            self.assertIn("STOP run_ab.sh", res1["context"])
            self.assertIn("Switch kernel signature back", res1["context"])

            # Second call with unchanged board.json returns None (stat-cached + deduped)
            self.assertIsNone(hook(session_id="sess-midturn"))

            # Adding a new @all message #7 surfaces #7 immediately without re-sending #6
            board_doc["nextSeq"] = 8
            board_doc["messages"].append({
                "seq": 7,
                "sender": "coordinator",
                "mentions": ["all"],
                "summary": "New @all update",
                "details": "Check updated constraints.",
            })
            board_file.write_text(json.dumps(board_doc), encoding="utf-8")
            res2 = hook(session_id="sess-midturn")
            self.assertIsNotNone(res2)
            self.assertNotIn("#6 from @coordinator", res2["context"])
            self.assertIn("#7 from @coordinator: New @all update", res2["context"])

    def test_transform_tool_result_persists_urgent_steer_and_dedupes_pre_llm_call(self) -> None:
        swarm_id = "swarm-transform-steer"
        board_file = self.board_dir / "board.json"
        board_doc = {
            "schemaVersion": 2,
            "swarmId": swarm_id,
            "nextSeq": 7,
            "members": {
                "impl-2": {
                    "name": "impl-2",
                    "role": "worker",
                    "runtime": "hermes",
                    "state": "working",
                    "lastReadSeq": 5,
                }
            },
            "pendingMentions": {
                "impl-2": [
                    {
                        "seq": 6,
                        "sender": "coordinator",
                        "summary": "STOP run_ab.sh and apply native_kernel_lb1 fix",
                        "details": "Restore 5-param signature before running A/B.",
                    }
                ]
            },
            "messages": [],
        }
        board_file.write_text(json.dumps(board_doc), encoding="utf-8")

        env = {
            **self.base_clear_env,
            "HERMES_SWARM_ID": swarm_id,
            "HERMES_SWARM_AGENT": "impl-2",
            "HERMES_SWARM_ROLE": "worker",
            "HERMES_SWARM_BOARD_DIR": str(self.board_dir),
        }
        with _EnvPatch(env):
            ctx = _FakeCtx()
            self.plugin.register(ctx)
            tx_hook = ctx.hooks["transform_tool_result"]
            pre_hook = ctx.hooks["pre_llm_call"]

            # Non-str multimodal results and swarm_read/swarm_wait results pass through untouched
            self.assertIsNone(tx_hook(tool_name="vision", result={"_multimodal": True}))
            self.assertIsNone(tx_hook(tool_name="swarm_read", result='{"ok": true}'))

            # String tool result from terminal appends persistent urgent steer
            steered = tx_hook(tool_name="terminal", result="exit code 0\nok")
            self.assertIsNotNone(steered)
            self.assertTrue(steered.startswith("exit code 0\nok\n\n"))
            self.assertIn("#6 from @coordinator: STOP run_ab.sh", steered)
            self.assertIn("Restore 5-param signature", steered)

            # Immediately following pre_llm_call does NOT double-inject the same #6
            self.assertIsNone(pre_hook(session_id="sess-transform"))
            # Subsequent unchanged terminal call returns None
            self.assertIsNone(tx_hook(tool_name="terminal", result="next output"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
