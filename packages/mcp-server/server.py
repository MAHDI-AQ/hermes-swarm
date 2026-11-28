"""Hermes A2A MCP Server — Clean 3-tier Antigravity <-> Hermes Swarm control plane."""

from __future__ import annotations

import argparse
import asyncio
from collections import OrderedDict
import copy
import json
import math
import os
from pathlib import Path
import threading
from typing import Any
from urllib.parse import quote
import weakref

from mcp.server.mcpserver import Context, MCPServer

import catalog as catalog_mod
import http_server
import models
import role_policy as role_policy_mod
import runtime as runtime_mod
import store
import worktree as worktree_mod

_SERVER_DIR = Path(__file__).resolve().parent
_OPEN_BOARD_LINK = "[Hermes Swarm](sidecar://hermes-swarm/swarm_board/)"
_ROLE_POLICY_CACHE_LOCK = threading.Lock()
_ROLE_POLICY_CACHE: OrderedDict[tuple[str, str, int, int], dict[str, Any]] = OrderedDict()
_ROLE_POLICY_CACHE_MAX = 32


def _load_role_policy_cached(workspace: str | None = None) -> dict[str, Any]:
    """Load local role_policy cached by `(env_override, path, mtime_ns, size)` while always honoring env overrides."""
    env_val = str(os.environ.get("HERMES_SWARM_ROLE_POLICY") or "").strip()
    if env_val.lower() in ("default", "none", "off"):
        return role_policy_mod.load_role_policy(workspace)

    chosen: Path | None = None
    chosen_sig: tuple[int, int] | None = None
    if env_val:
        explicit = Path(env_val).expanduser()
        try:
            st = explicit.stat()
            if explicit.is_file():
                chosen = explicit
                chosen_sig = (int(st.st_mtime_ns), int(st.st_size))
        except OSError:
            pass
        if chosen is None:
            return role_policy_mod.load_role_policy(workspace)
    else:
        for cand in role_policy_mod.candidate_role_policy_paths(workspace):
            try:
                if cand.is_file():
                    st = cand.stat()
                    chosen = cand
                    chosen_sig = (int(st.st_mtime_ns), int(st.st_size))
                    break
            except OSError:
                continue
        if chosen is None:
            return role_policy_mod.load_role_policy(workspace)

    assert chosen is not None and chosen_sig is not None
    cache_key = (env_val, str(chosen), chosen_sig[0], chosen_sig[1])
    with _ROLE_POLICY_CACHE_LOCK:
        cached = _ROLE_POLICY_CACHE.get(cache_key)
        if cached is not None:
            _ROLE_POLICY_CACHE.move_to_end(cache_key)
            return copy.deepcopy(cached)

    policy = role_policy_mod.load_role_policy(workspace)
    with _ROLE_POLICY_CACHE_LOCK:
        _ROLE_POLICY_CACHE[cache_key] = copy.deepcopy(policy)
        _ROLE_POLICY_CACHE.move_to_end(cache_key)
        while len(_ROLE_POLICY_CACHE) > _ROLE_POLICY_CACHE_MAX:
            _ROLE_POLICY_CACHE.popitem(last=False)
    return policy

ORCHESTRATOR_INSTRUCTIONS = """\
You are connected to the Hermes A2A Swarm Bridge (`hermes-a2a`), enabling a 3-tier
1:1 session-bound workflow with a read-only live observer pane ([Hermes Swarm](sidecar://hermes-swarm/swarm_board/)):

1. Tier 1 (`primary` — User + Primary Antigravity session):
   - Resolve active workspace via `swarm_workspace(action="get_workdir")` (and `set_workdir` if needed).
   - Initialize this conversation's 1:1 swarm via `swarm_manage(action="init", conversation_id=..., title=..., mission=...)` and put `open_board` on its own line (never a manual board link or command).
   - Spawn ONE dedicated Tier-2 Coordinator subagent via `invoke_subagent` with `Model="inherit"` (fast/medium dispatcher — NEVER `pro`/XHigh for Coordinator; reserve `pro`/XHigh strictly for Tier-2 IDE Specialists), bind its exact verbatim `conversationId`
     via `swarm_manage(action="bind_coordinator", conversation_id=..., coordinator_conversation_id=..., model=..., provider=...)` (NEVER hand-type or splice UUID segments), and wait for `@primary` escalations.
   - Read the project's Obsidian vault (`Handoffs/current.md` and `Lessons/lessons.md`) before starting and write verified receipts/lessons to `Evidence/` / `Handoffs/` before closing.
2. Tier 2a (`coordinator` + IDE Frontier Subagents via `invoke_subagent(TypeName="self", ...)`):
   - Coordinator is a **thin, fast dispatcher**: read ONLY the active project vault's `Handoffs/current.md` (if declared) and `hermes-swarm-pane/SKILL.md` once — NEVER read codebase source files, multiple skill manuals, or long logs inline. Within 1–2 turns, dispatch Tier-2 IDE Specialists (`invoke_subagent`, `Model="pro"`) and/or Hermes bricklayers (`swarm_worker`), register IDE members (`register_member`), publish `set_plan`, and block on `swarm_wait`.
   - Thinking & judgment roles (Research, Architecture/Design, Planning, Specialist Consultation, and Adversarial Verification) belong to **IDE Frontier Subagents** (`invoke_subagent`, `TypeName="self"`, `Model="pro"`), NEVER Coordinator inline reading or cheap Hermes execution workers.
   - Immediately after spawning any Antigravity IDE subagent with `invoke_subagent`, register it as a first-class board member via `swarm_manage(action="register_member", name=..., role=..., conversation_id="<exact conversationId>", assignment=...)` and include it in `set_plan` so it appears in `Members`, `Overview`, and `Graph` (and mark it `done` / `remove_member` when stopped or complete). Pass `swarm_id` + registered `name` in the subagent's prompt and instruct it to post its concise findings/verdict directly via `swarm_board_post(swarm_id=..., sender="<name>", kind="post", summary="...", details="...")` **without `@mention`ing peers or `@coordinator`** (so Board/Graph show real multi-agent posts without extra wakeup churn).
   - Inspect `role_policy` returned by `swarm_manage(init|status)` / `swarm_catalog()` so local user preferences (IDE tiers, restricted roles, execution worker model defaults) are always honored.
   - Because `wakeup_bridge` only wakes `primary` and `coordinator`, Coordinator bridges outputs between IDE subagents and Hermes workers (`@coordinator` only).
3. Tier 2b (Hermes A2A Execution Workers via `swarm_worker` — `worker|implementer|monitor|narrator`):
   - Discover available models dynamically via `swarm_catalog()` before spawning workers (or inherit local `role_policy` `hermes_worker_defaults`).
   - Reserve `swarm_worker` strictly for execution bricklaying: implementing pre-designed specs/patches, running builds, SSH/profiling/benchmark/test loops, and git/CI mechanics.
   - Publish `swarm_manage(action="set_plan", ...)`, spawn/resume/switch/diff/kill workers via
     `swarm_worker(...)` (`diff` accepts optional `name`, omitting `name` inspects all workers), manage channels via `swarm_manage(action="channel", ...)`, claim/release
     scopes via `swarm_board_read(scope_action=...)`, and block zero-cost via `swarm_wait(since=...)`. Note: posting `@<worker>` steers a running Hermes worker on its next tool result (`transform_tool_result` + `pre_llm_call`), whereas `swarm_worker(action="kill", name=...)` is required to hard-abort an in-flight long remote command immediately.
4. Tier 3 (`subagent` — Hermes `delegate_task` leaf workers):
   - Spawned exclusively by Tier-2 Hermes workers; surfaced read-only under Members.
"""

mcp = MCPServer(
    name="hermes-a2a",
    title="Hermes A2A Swarm Bridge",
    description="Clean 3-tier 1:1 session-bound A2A v1.0 orchestrator bridge and event-driven Swarm Board for Hermes Agent.",
    instructions=ORCHESTRATOR_INSTRUCTIONS,
    version="2.0.0",
)

_session_workdir: str | None = None
_session_conversation_id: str | None = None
_session_swarm_id: str | None = None
_SESSION_STATE: weakref.WeakKeyDictionary[Any, dict[str, str | None]] = weakref.WeakKeyDictionary()
_swarm_mgr = runtime_mod.SwarmManager(default_workdir_provider=lambda: _session_workdir)
_http_ctrl = http_server.EmbeddedA2AHttpController(_swarm_mgr)


def _get_session_bucket(ctx: Context | None) -> dict[str, str | None] | None:
    if ctx is None:
        return None
    sess = getattr(ctx, "session", None)
    if sess is None:
        return None
    conn = getattr(sess, "_connection", None) or getattr(sess, "connection", None)
    if conn is not None:
        key_obj = conn
    elif type(sess).__name__ != "ServerSession":
        key_obj = sess
    else:
        return None
    try:
        bucket = _SESSION_STATE.get(key_obj)
        if bucket is None:
            bucket = {"workdir": None, "conversation_id": None, "swarm_id": None}
            _SESSION_STATE[key_obj] = bucket
        return bucket
    except TypeError:
        return None


def _get_session_val(ctx: Context | None, key: str) -> str | None:
    bucket = _get_session_bucket(ctx)
    if bucket is not None:
        return bucket.get(key)
    if key == "workdir":
        return _session_workdir
    if key == "conversation_id":
        return _session_conversation_id
    if key == "swarm_id":
        return _session_swarm_id
    return None


def _set_session_val(ctx: Context | None, key: str, val: str | None) -> None:
    global _session_workdir, _session_conversation_id, _session_swarm_id
    bucket = _get_session_bucket(ctx)
    if bucket is not None:
        bucket[key] = val
        return
    if key == "workdir":
        _session_workdir = val
    elif key == "conversation_id":
        _session_conversation_id = val
    elif key == "swarm_id":
        _session_swarm_id = val


def _build_observer_ui_url(base_url: str, *, conv_id: str | None, swarm_id: str) -> str:
    clean_base = (base_url or "").rstrip("/")
    if not clean_base:
        return ""
    if conv_id:
        return f"{clean_base}/board/ui?conversationId={quote(str(conv_id), safe='')}"
    return f"{clean_base}/board/ui?swarm={quote(str(swarm_id), safe='')}"


def _json_str(data: Any) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False)


def _is_plausible_project_dir(path_str: str) -> bool:
    try:
        p = Path(path_str).resolve()
    except Exception:
        return False
    if not p.is_dir():
        return False
    low = str(p).lower()
    home = str(Path.home().resolve()).lower()
    if low == home or low == str(_SERVER_DIR.resolve()).lower():
        return False
    if "\\windows\\system32" in low or "\\appdata\\" in low:
        return False
    return True


async def _list_mcp_root_paths(ctx: Context | None) -> list[str]:
    if ctx is None:
        return []
    try:
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            res = await ctx.session.list_roots()
        paths: list[str] = []
        for r in getattr(res, "roots", None) or []:
            local = store.normalize_local_path(str(getattr(r, "uri", "") or ""))
            if local and Path(local).is_dir():
                paths.append(str(Path(local).resolve()))
        return paths
    except Exception:
        return []


async def _resolve_active_workdir(
    explicit_workdir: str | None,
    ctx: Context | None = None,
) -> tuple[str | None, str]:
    if explicit_workdir and explicit_workdir.strip() and explicit_workdir.strip().lower() != "auto":
        norm = store.normalize_local_path(explicit_workdir)
        return str(Path(norm or explicit_workdir).expanduser().resolve()), "explicit"

    roots = await _list_mcp_root_paths(ctx)
    if roots:
        return roots[0], "mcp_roots"

    sess_wd = _get_session_val(ctx, "workdir")
    if sess_wd and Path(sess_wd).is_dir():
        return sess_wd, "session_default"

    env_dir = (os.environ.get("HERMES_A2A_DEFAULT_WORKDIR") or "").strip()
    if env_dir and Path(env_dir).is_dir():
        return str(Path(env_dir).resolve()), "env_default"

    cwd = os.getcwd()
    if _is_plausible_project_dir(cwd):
        return str(Path(cwd).resolve()), "process_cwd"

    return None, "unset"


async def _resolve_mcp_swarm(
    *,
    swarm_id: str | None = None,
    conversation_id: str | None = None,
    workspace: str | None = None,
    ctx: Context | None = None,
) -> tuple[str, runtime_mod.SwarmRuntime]:
    try:
        cur_loop = asyncio.get_running_loop()
        if (
            _swarm_mgr._host_loop is None
            or _swarm_mgr._host_loop.is_closed()
            or not _swarm_mgr._host_loop.is_running()
        ):
            _swarm_mgr.set_host_loop(cur_loop)
    except RuntimeError:
        pass

    explicit_sid = store.sanitize_swarm_id(swarm_id)
    if explicit_sid:
        rt = await asyncio.to_thread(_swarm_mgr.get_runtime, explicit_sid)
        return explicit_sid, rt

    clean_conv = str(conversation_id or "").strip()
    if clean_conv:
        sid = await asyncio.to_thread(_swarm_mgr.resolve_swarm_id, conversation_id=clean_conv)
        rt = await asyncio.to_thread(_swarm_mgr.get_runtime, sid)
        return sid, rt

    sess_conv = _get_session_val(ctx, "conversation_id")
    if sess_conv:
        sid = await asyncio.to_thread(_swarm_mgr.resolve_swarm_id, conversation_id=sess_conv)
        rt = await asyncio.to_thread(_swarm_mgr.get_runtime, sid)
        return sid, rt

    sess_sid = _get_session_val(ctx, "swarm_id")
    if sess_sid:
        rt = await asyncio.to_thread(_swarm_mgr.get_runtime, sess_sid)
        return sess_sid, rt

    sid = await asyncio.to_thread(_swarm_mgr.resolve_swarm_id, workspace=workspace)
    rt = await asyncio.to_thread(_swarm_mgr.get_runtime, sid)
    return sid, rt


@mcp.tool()
async def swarm_catalog(
    profile: str = "default",
    workdir: str | None = None,
    ctx: Context | None = None,
) -> str:
    """Discover available Hermes models, providers, profiles, toolsets, skills, and A2A peers."""
    resolved_ws, _ = await _resolve_active_workdir(workdir, ctx)
    cat = await catalog_mod.discover_hermes_catalog(
        profile=profile or "default",
        workdir=resolved_ws,
        query_cli=True,
    )
    cat["http_url"] = _http_ctrl.url or _swarm_mgr.http_url
    cat["active_workdir"] = resolved_ws or _get_session_val(ctx, "workdir") or _swarm_mgr.get_default_workdir()
    cat["role_policy"] = await asyncio.to_thread(_load_role_policy_cached, cat["active_workdir"])
    return _json_str(cat)


@mcp.tool()
async def swarm_workspace(
    action: str,
    workdir: str | None = None,
    workspace: str | None = None,
    path: str | None = None,
    swarm_id: str | None = None,
    conversation_id: str | None = None,
    ctx: Context | None = None,
) -> str:
    """Manage active workspace directory and git worktrees (`get|set|prune_worktrees|git_status|git_diff`)."""
    act = str(action or "get").strip().lower()
    workdir = workdir or workspace or path

    if act in ("get", "get_workdir"):
        resolved, source = await _resolve_active_workdir(workdir, ctx)
        return _json_str({
            "workdir": resolved,
            "source": source,
            "sessionWorkdir": _get_session_val(ctx, "workdir"),
            "mcpRoots": await _list_mcp_root_paths(ctx),
        })

    if act in ("set", "set_workdir"):
        if not workdir or not workdir.strip():
            raise ValueError("`workdir` is required for action='set'.")
        norm = store.normalize_local_path(workdir)
        if not norm or not Path(norm).is_dir():
            raise ValueError(f"Directory does not exist: {workdir}")
        new_wd = runtime_mod._canonical_workspace_path(str(Path(norm).resolve())) or str(Path(norm).resolve())
        _set_session_val(ctx, "workdir", new_wd)
        if swarm_id or conversation_id or _get_session_val(ctx, "conversation_id") or _get_session_val(ctx, "swarm_id"):
            sid, _ = await _resolve_mcp_swarm(
                swarm_id=swarm_id,
                conversation_id=conversation_id,
                workspace=new_wd,
                ctx=ctx,
            )
            meta = await asyncio.to_thread(_swarm_mgr.store.upsert_meta, sid, workspace=new_wd)
            if _http_ctrl.is_running():
                await asyncio.to_thread(_http_ctrl._register_self, swarm_id=sid)
            return _json_str({"workdir": new_wd, "swarmId": sid, "meta": meta})
        return _json_str({"workdir": new_wd, "swarmId": None})

    resolved, source = await _resolve_active_workdir(workdir, ctx)
    if not workdir or not str(workdir).strip():
        hint_conv = conversation_id or _get_session_val(ctx, "conversation_id")
        hint_sid = store.sanitize_swarm_id(swarm_id)
        if not hint_sid and hint_conv:
            hint_sid = await asyncio.to_thread(_swarm_mgr.store.resolve_swarm_for_conversation, hint_conv)
        if not hint_sid:
            hint_sid = _get_session_val(ctx, "swarm_id")
        if hint_sid:
            meta_doc = await asyncio.to_thread(_swarm_mgr.store.read_meta, hint_sid)
            meta_ws = str((meta_doc or {}).get("workspace") or "").strip()
            if meta_ws and Path(meta_ws).is_dir():
                resolved, source = meta_ws, "swarm_meta"
    if not resolved:
        raise ValueError("No active workdir could be resolved; pass `workdir` or call `swarm_workspace(action='set_workdir', workdir=...)`.")

    if act == "git_status":
        res = await catalog_mod.run_git_inspect(resolved, action="status")
        return _json_str({"workdir": resolved, "source": source, **res})

    if act == "git_diff":
        res = await catalog_mod.run_git_inspect(resolved, action="diff")
        stat_res = await catalog_mod.run_git_inspect(resolved, action="diff_stat")
        return _json_str({
            "workdir": resolved,
            "source": source,
            "ok": res.get("ok"),
            "diffStat": stat_res.get("stdout", ""),
            "patch": res.get("stdout", ""),
            "stderr": res.get("stderr", ""),
        })

    if act == "prune_worktrees":
        prune_root = runtime_mod._canonical_workspace_path(resolved) or resolved
        sid, rt = await _resolve_mcp_swarm(
            swarm_id=swarm_id,
            conversation_id=conversation_id,
            workspace=prune_root,
            ctx=ctx,
        )
        tasks_snap = await asyncio.to_thread(rt.get_task_snapshots)
        active_ids: set[str] = set()
        for t in tasks_snap:
            st = str(((t.get("status") or {}).get("state")) or "")
            if st in models.ACTIVE_STATES:
                wcfg = (t.get("metadata") or {}).get("workerConfig") or {}
                if wcfg.get("worker_name"):
                    active_ids.add(str(wcfg["worker_name"]))
                if t.get("id"):
                    active_ids.add(str(t["id"]))
        res = await worktree_mod.prune_worktrees(prune_root, active_ids=active_ids)
        return _json_str({"swarmId": sid, **res})

    raise ValueError(f"Unsupported swarm_workspace action: {action!r}")


@mcp.tool()
async def swarm_manage(
    action: str,
    swarm_id: str | None = None,
    title: str | None = None,
    goal: str | None = None,
    mission: str | None = None,
    workspace: str | None = None,
    primary_conversation_id: str | None = None,
    coordinator_conversation_id: str | None = None,
    conversation_id: str | None = None,
    project_id: str | None = None,
    name: str | None = None,
    role: str | None = None,
    assignment: str | None = None,
    state: str | None = None,
    runtime: str | None = None,
    channels: list[str] | None = None,
    model: str | None = None,
    provider: str | None = None,
    plan: dict[str, Any] | None = None,
    op: str | None = None,
    channel: str | None = None,
    topic: str | None = None,
    members: list[str] | None = None,
    states: list[str] | None = None,
    ctx: Context | None = None,
) -> str:
    """Manage 1:1 session-bound swarm lifecycle (`init|title|bind_coordinator|register_member|remove_member|set_plan|status|clear_finished|channel`)."""
    act = str(action or "status").strip().lower()
    resolved_ws, _ = await _resolve_active_workdir(workspace, ctx)
    resolved_ws = runtime_mod._canonical_workspace_path(resolved_ws) if resolved_ws else None
    prim_cid = str(primary_conversation_id or conversation_id or "").strip() or None

    if act in ("init", "create"):
        explicit_sid = store.sanitize_swarm_id(swarm_id)
        if prim_cid:
            existing_sid = await asyncio.to_thread(_swarm_mgr.store.resolve_swarm_for_conversation, prim_cid)
            sid = explicit_sid or existing_sid or store.derive_swarm_id(conversation_id=prim_cid)
            _set_session_val(ctx, "conversation_id", prim_cid)
            _set_session_val(ctx, "swarm_id", sid)
        elif explicit_sid:
            sid = explicit_sid
            _set_session_val(ctx, "swarm_id", sid)
        else:
            raise ValueError(
                "`primary_conversation_id` (or `conversation_id`) is required for action='init' "
                "(or pass `swarm_id` for standalone testing)."
            )

        meta = await asyncio.to_thread(
            _swarm_mgr.store.upsert_meta,
            sid,
            title=title,
            goal=goal,
            mission=mission,
            workspace=resolved_ws,
            primary_conversation_id=prim_cid,
            coordinator_conversation_id=coordinator_conversation_id,
            project_id=project_id,
            archived=False,
        )
        await asyncio.to_thread(_swarm_mgr.get_runtime, sid)
        if _http_ctrl.is_running():
            await asyncio.to_thread(_http_ctrl._register_self, swarm_id=sid)
        base_url = (_http_ctrl.url or _swarm_mgr.http_url or "").rstrip("/")
        conv_for_url = prim_cid or meta.get("primaryConversationId") or meta.get("coordinatorConversationId") or ""
        observer_ui_url = _build_observer_ui_url(base_url, conv_id=conv_for_url, swarm_id=sid)
        return _json_str({
            "swarm_id": sid,
            "open_board": _OPEN_BOARD_LINK,
            "http_url": _http_ctrl.url or _swarm_mgr.http_url,
            "observer_ui_url": observer_ui_url,
            "meta": meta,
            "role_policy": await asyncio.to_thread(_load_role_policy_cached, meta.get("workspace") or resolved_ws),
        })

    lookup_conv = (
        primary_conversation_id or conversation_id or coordinator_conversation_id
        if act != "bind_coordinator"
        else (primary_conversation_id or _get_session_val(ctx, "conversation_id") or conversation_id or coordinator_conversation_id)
    )
    sid, rt = await _resolve_mcp_swarm(
        swarm_id=swarm_id,
        conversation_id=lookup_conv,
        workspace=resolved_ws,
        ctx=ctx,
    )

    if act in ("title", "rename"):
        if not title or not title.strip():
            raise ValueError("`title` is required for action='title'.")
        meta = await asyncio.to_thread(_swarm_mgr.store.rename_swarm, sid, title.strip())
        return _json_str({"swarm_id": sid, "open_board": _OPEN_BOARD_LINK, "meta": meta})

    if act == "bind_coordinator":
        ccid = str(coordinator_conversation_id or conversation_id or "").strip()
        if not ccid:
            raise ValueError("`coordinator_conversation_id` is required for action='bind_coordinator'.")
        existing_meta = await asyncio.to_thread(_swarm_mgr.store.read_meta, sid)
        prim_for_check = str(primary_conversation_id or (existing_meta or {}).get("primaryConversationId") or "").strip() or None
        valid_ccid, resolved_ccid, auto_corrected_from, valid_err = await asyncio.to_thread(
            runtime_mod.resolve_or_heal_antigravity_conversation_id,
            ccid,
            prim_for_check,
            allow_single_candidate=True,
        )
        if not valid_ccid:
            raise ValueError(valid_err or f"Invalid coordinator_conversation_id: {ccid!r}")
        ccid = resolved_ccid
        existing_ws = (existing_meta or {}).get("workspace")
        ws_for_bind = (
            resolved_ws
            if (resolved_ws and (not existing_ws or runtime_mod._is_worktree_subpath(existing_ws)))
            else None
        )
        eff_model = str(model or "").strip() or None
        eff_provider = str(provider or "").strip() or None
        if not eff_model:
            t_summary = await asyncio.to_thread(
                runtime_mod.extract_antigravity_transcript_summary,
                [ccid],
                primary_cid=prim_for_check,
            )
            eff_model = str(t_summary.get("model") or "").strip() or None
            if not eff_provider and t_summary.get("provider"):
                eff_provider = str(t_summary["provider"]).strip() or None
        meta = await asyncio.to_thread(
            _swarm_mgr.store.upsert_meta,
            sid,
            primary_conversation_id=primary_conversation_id,
            coordinator_conversation_id=ccid,
            project_id=project_id,
            workspace=ws_for_bind,
        )
        await asyncio.to_thread(
            _swarm_mgr.board_service.upsert_member,
            models.Role.COORDINATOR.value,
            role=models.Role.COORDINATOR.value,
            tier=2,
            runtime="antigravity",
            state=models.MemberState.WORKING.value,
            conversation_id=ccid,
            model=eff_model,
            provider=eff_provider,
            channels=["*"],
            swarm_id=sid,
        )
        if _http_ctrl.is_running():
            await asyncio.to_thread(_http_ctrl._register_self, swarm_id=sid)
        resp_bind: dict[str, Any] = {
            "swarm_id": sid,
            "open_board": _OPEN_BOARD_LINK,
            "coordinator_conversation_id": ccid,
            "model": eff_model,
            "provider": eff_provider,
            "meta": meta,
        }
        if auto_corrected_from:
            resp_bind["auto_corrected_from"] = auto_corrected_from
        return _json_str(resp_bind)

    if act in ("register_member", "add_member"):
        clean_name = str(name or "").strip().lstrip("@")
        if not clean_name:
            raise ValueError("`name` is required for action='register_member'.")
        eff_runtime = str(runtime or "antigravity").strip().lower() or "antigravity"
        cid = str(conversation_id or coordinator_conversation_id or "").strip() or None
        existing_meta = await asyncio.to_thread(_swarm_mgr.store.read_meta, sid)
        prim_for_check = str(primary_conversation_id or (existing_meta or {}).get("primaryConversationId") or "").strip() or None
        coord_for_check = str((existing_meta or {}).get("coordinatorConversationId") or "").strip() or None
        auto_corrected_cid_from: str | None = None
        if eff_runtime == "antigravity":
            if not cid:
                raise ValueError("`conversation_id` is required when registering an Antigravity member.")
            valid_cid, resolved_cid, auto_corrected_cid_from, valid_err = await asyncio.to_thread(
                runtime_mod.resolve_or_heal_antigravity_conversation_id,
                cid,
                coord_for_check or prim_for_check,
                allow_single_candidate=False,
            )
            if not valid_cid:
                raise ValueError(valid_err or f"Invalid conversation_id: {cid!r}")
            cid = resolved_cid

        eff_model = str(model or "").strip() or None
        eff_provider = str(provider or "").strip() or ("antigravity" if eff_runtime == "antigravity" else None)
        if cid and eff_runtime == "antigravity" and not eff_model:
            t_summary = await asyncio.to_thread(
                runtime_mod.extract_antigravity_transcript_summary,
                [cid],
                primary_cid=None,
            )
            eff_model = str(t_summary.get("model") or "").strip() or None
            if t_summary.get("provider"):
                eff_provider = str(t_summary["provider"]).strip() or eff_provider

        eff_role = models.normalize_member_role(role or "engineer", runtime=eff_runtime)
        eff_state_raw = str(state or models.MemberState.WORKING.value).strip().lower() or models.MemberState.WORKING.value
        is_killed = eff_state_raw == models.MemberState.KILLED.value
        eff_assignment = assignment if assignment is not None else (goal or mission)
        member_rec = await asyncio.to_thread(
            _swarm_mgr.board_service.upsert_member,
            clean_name,
            role=eff_role,
            tier=2,
            runtime=eff_runtime,
            state=eff_state_raw,
            killed=False if not is_killed else True,
            assignment=eff_assignment,
            conversation_id=cid,
            model=eff_model,
            provider=eff_provider,
            workdir=resolved_ws or (existing_meta or {}).get("workspace"),
            channels=channels or ["*"],
            swarm_id=sid,
        )
        meta = existing_meta or models.default_swarm_meta(sid)
        if cid:
            meta = await asyncio.to_thread(
                _swarm_mgr.store.upsert_meta,
                sid,
                extra_conversation_ids=[cid],
            )
        if _http_ctrl.is_running():
            await asyncio.to_thread(_http_ctrl._register_self, swarm_id=sid)
        resp_reg: dict[str, Any] = {
            "swarm_id": sid,
            "open_board": _OPEN_BOARD_LINK,
            "member": member_rec,
            "meta": meta,
        }
        if auto_corrected_cid_from:
            resp_reg["auto_corrected_from"] = auto_corrected_cid_from
        return _json_str(resp_reg)

    if act in ("remove_member", "unregister_member"):
        clean_name = str(name or "").strip().lstrip("@")
        if not clean_name:
            raise ValueError("`name` is required for action='remove_member'.")
        eff_state_raw = str(state or models.MemberState.KILLED.value).strip().lower() or models.MemberState.KILLED.value
        is_killed = eff_state_raw == models.MemberState.KILLED.value
        board_doc = await asyncio.to_thread(_swarm_mgr.board_service.read_board, sid)
        prev_mem = (board_doc.get("members") or {}).get(clean_name) if isinstance(board_doc.get("members"), dict) else None
        prev_runtime = str((prev_mem or {}).get("runtime") or runtime or "antigravity").strip().lower() or "antigravity"
        prev_role = str((prev_mem or {}).get("role") or role or "engineer")
        member_rec = await asyncio.to_thread(
            _swarm_mgr.board_service.upsert_member,
            clean_name,
            role=prev_role,
            runtime=prev_runtime,
            state=eff_state_raw,
            killed=is_killed,
            swarm_id=sid,
        )
        return _json_str({
            "swarm_id": sid,
            "open_board": _OPEN_BOARD_LINK,
            "member": member_rec,
        })

    if act == "set_plan":
        if not isinstance(plan, dict):
            raise ValueError("`plan` object is required for action='set_plan'.")
        res = await asyncio.to_thread(_swarm_mgr.board_service.set_plan, plan, swarm_id=sid)
        return _json_str({"swarm_id": sid, "open_board": _OPEN_BOARD_LINK, **res})

    if act == "clear_finished":
        target_states = tuple(states) if states else (
            models.TaskState.COMPLETED.value,
            models.TaskState.FAILED.value,
            models.TaskState.CANCELED.value,
        )
        res = await rt.run_on_loop(rt.clear_finished_tasks, states=target_states)
        return _json_str({"swarm_id": sid, "open_board": _OPEN_BOARD_LINK, **res})

    if act == "channel":
        res = await asyncio.to_thread(
            _swarm_mgr.board_service.channel_action,
            op or "list",
            channel=channel,
            topic=topic or "",
            members=members,
            swarm_id=sid,
        )
        return _json_str({"swarm_id": sid, "open_board": _OPEN_BOARD_LINK, **res})

    if act == "status":
        board_status = await asyncio.to_thread(_swarm_mgr.board_service.get_status, swarm_id=sid)
        tasks_res = await rt.run_on_loop(rt.list_tasks, page_size=50, include_live=True)
        meta = await asyncio.to_thread(_swarm_mgr.store.read_meta, sid) or models.default_swarm_meta(sid)
        snap = await asyncio.to_thread(http_server.build_observer_snapshot, _swarm_mgr, sid)
        base_url = (_http_ctrl.url or _swarm_mgr.http_url or "").rstrip("/")
        conv_for_url = (
            meta.get("primaryConversationId")
            or meta.get("coordinatorConversationId")
            or ""
        )
        observer_ui_url = _build_observer_ui_url(base_url, conv_id=conv_for_url, swarm_id=sid)
        return _json_str({
            "swarm_id": sid,
            "open_board": _OPEN_BOARD_LINK,
            "http_url": _http_ctrl.url or _swarm_mgr.http_url,
            "observer_ui_url": observer_ui_url,
            "meta": meta,
            "board": board_status,
            "telemetry": snap.get("telemetry") or {},
            "members": snap.get("members") or [],
            "tasks": tasks_res.get("tasks") or [],
            "role_policy": await asyncio.to_thread(_load_role_policy_cached, meta.get("workspace") or resolved_ws),
        })

    raise ValueError(f"Unsupported swarm_manage action: {action!r}")


@mcp.tool()
async def swarm_worker(
    action: str,
    name: str | None = None,
    prompt: str | None = None,
    role: str = "worker",
    tasks: list[str] | None = None,
    n: int | None = None,
    fork: str | None = None,
    workdir: str | None = None,
    worktree: bool = False,
    channels: list[str] | None = None,
    model: str | None = None,
    provider: str | None = None,
    base_url: str | None = None,
    profile: str = "default",
    reasoning: str | None = None,
    toolsets: list[str] | None = None,
    skills: list[str] | None = None,
    max_turns: int | None = None,
    run_budget_seconds: int | None = None,
    role_prompt: str | None = None,
    return_immediately: bool = True,
    wait_seconds: float = 300.0,
    include_patch: bool = True,
    reason: str = "Killed by coordinator.",
    swarm_id: str | None = None,
    conversation_id: str | None = None,
    workspace: str | None = None,
    allow_ide_only_role: bool = False,
    ctx: Context | None = None,
) -> str:
    """Manage Tier-2 Hermes A2A workers (`spawn|resume|switch_model|status|diff|kill`)."""
    act = str(action or "status").strip().lower()
    resolved_ws, _ = await _resolve_active_workdir(workdir or workspace, ctx)
    sid, rt = await _resolve_mcp_swarm(
        swarm_id=swarm_id,
        conversation_id=conversation_id,
        workspace=resolved_ws,
        ctx=ctx,
    )

    if act == "spawn":
        if not name or not name.strip():
            raise ValueError("`name` is required for action='spawn'.")
        if not prompt or not prompt.strip():
            raise ValueError("`prompt` is required for action='spawn'.")
        meta_doc = await asyncio.to_thread(_swarm_mgr.store.read_meta, sid) or {}
        policy = await asyncio.to_thread(_load_role_policy_cached, resolved_ws or meta_doc.get("workspace"))
        allowed, policy_msg = role_policy_mod.evaluate_worker_spawn_policy(
            role,
            worker_name=name.strip(),
            policy=policy,
            allow_ide_only_role=bool(allow_ide_only_role),
        )
        if not allowed:
            raise ValueError(policy_msg or f"Role {role!r} is restricted to IDE subagents under role_policy.")
        eff_defaults = role_policy_mod.apply_worker_defaults(
            policy,
            model=model,
            provider=provider,
            base_url=base_url,
            reasoning=reasoning,
            profile=profile,
        )
        res = await rt.run_on_loop(
            rt.spawn_workers,
            name=name.strip(),
            prompt=prompt.strip(),
            role=role,
            tasks=tasks,
            n=n,
            fork_from=fork,
            workdir=resolved_ws,
            worktree=bool(worktree),
            channels=channels,
            model=eff_defaults["model"],
            provider=eff_defaults["provider"],
            base_url=eff_defaults["base_url"],
            profile=eff_defaults["profile"] or "default",
            reasoning=eff_defaults["reasoning"],
            toolsets=toolsets,
            skills=skills,
            max_turns=max_turns,
            run_budget_seconds=run_budget_seconds,
            role_prompt=role_prompt,
            return_immediately=bool(return_immediately),
            wait_seconds=wait_seconds,
        )
        if policy_msg and isinstance(res, dict):
            res["policy_warning"] = policy_msg
        if isinstance(res, dict):
            res["role_policy_source"] = policy.get("source", "default")
        return _json_str(res)

    if act == "resume":
        if not name or not name.strip():
            raise ValueError("`name` is required for action='resume'.")
        if not prompt or not prompt.strip():
            raise ValueError("`prompt` is required for action='resume'.")
        task = await rt.run_on_loop(
            rt.resume_worker,
            name.strip(),
            prompt.strip(),
            return_immediately=bool(return_immediately),
            wait_seconds=wait_seconds,
        )
        return _json_str({"swarmId": sid, "worker": name.strip(), "task": task})

    if act == "switch_model":
        if not name or not name.strip():
            raise ValueError("`name` is required for action='switch_model'.")
        res = await rt.run_on_loop(
            rt.switch_model,
            name.strip(),
            model=model,
            provider=provider,
            reasoning=reasoning,
            prompt=prompt,
            return_immediately=bool(return_immediately),
            wait_seconds=wait_seconds,
        )
        return _json_str({"swarmId": sid, **res})

    if act == "status":
        snap = await asyncio.to_thread(http_server.build_observer_snapshot, _swarm_mgr, sid)
        if name and name.strip():
            wname = name.strip()
            task = rt.find_latest_worker_task(wname)
            if not task:
                with rt._state_rlock:
                    task = rt._tasks.get(wname)
            if not task:
                raise KeyError(f"Worker or task '{wname}' not found in swarm '{sid}'.")
            view = await rt.run_on_loop(rt.get_task, str(task["id"]), include_artifacts=True)
            member_entry = next(
                (m for m in (snap.get("members") or []) if str(m.get("name") or "").lower() == wname.lower()),
                None,
            )
            return _json_str({
                "swarmId": sid,
                "worker": wname,
                "member": member_entry,
                "telemetry": snap.get("telemetry") or {},
                "task": view,
            })
        res = await rt.run_on_loop(rt.list_tasks, page_size=50, include_live=True)
        return _json_str({
            "swarmId": sid,
            "telemetry": snap.get("telemetry") or {},
            "members": snap.get("members") or [],
            **res,
        })

    if act == "diff":
        target_name = name.strip() if (name and name.strip()) else None
        res = await rt.run_on_loop(rt.diff_worker, target_name, include_patch=bool(include_patch))
        return _json_str({"swarmId": sid, **res})

    if act == "kill":
        if not name or not name.strip():
            raise ValueError("`name` is required for action='kill'.")
        res = await rt.run_on_loop(rt.kill_worker, name.strip(), reason=reason)
        return _json_str(res)

    raise ValueError(f"Unsupported swarm_worker action: {action!r}")


@mcp.tool()
async def swarm_board_post(
    sender: str,
    summary: str,
    kind: str = "post",
    channel: str = "main",
    to: list[str] | None = None,
    details: str = "",
    files: list[str] | None = None,
    reply_to: int | str | None = None,
    thread_id: str | None = None,
    swarm_id: str | None = None,
    conversation_id: str | None = None,
    workspace: str | None = None,
    ctx: Context | None = None,
) -> str:
    """Post a message to the Swarm Board with automatic `@peer`, `@#channel`, and `@all` wakeups."""
    sid, _ = await _resolve_mcp_swarm(swarm_id=swarm_id, conversation_id=conversation_id, workspace=workspace, ctx=ctx)
    msg = await asyncio.to_thread(
        _swarm_mgr.board_service.post_message,
        sender=sender,
        summary=summary,
        msg_type=kind,
        channel=channel,
        to=to,
        details=details,
        files=files,
        reply_to=reply_to,
        thread_id=thread_id,
        swarm_id=sid,
    )
    return _json_str(msg)


@mcp.tool()
async def swarm_board_read(
    swarm_id: str | None = None,
    channel: str | None = None,
    since: int | None = None,
    seqs: list[int] | None = None,
    status: bool = False,
    agent: str | None = None,
    limit: int = 100,
    mention: str | None = None,
    sender: str | None = None,
    kind: str | None = None,
    mark_read_by: str | None = None,
    include_archive: bool = False,
    before_seq: int | None = None,
    search: str | None = None,
    full: bool = False,
    scope_action: str | None = None,
    scope: str | None = None,
    holder: str | None = None,
    notes: str = "",
    force: bool = False,
    conversation_id: str | None = None,
    workspace: str | None = None,
    ctx: Context | None = None,
) -> str:
    """Read Swarm Board messages (`since`, `seqs`, `status`, `channel`, `mention`) or manage scope locks (`claim|release|list`)."""
    sid, _ = await _resolve_mcp_swarm(swarm_id=swarm_id, conversation_id=conversation_id, workspace=workspace, ctx=ctx)
    if scope_action:
        sa = str(scope_action).strip().lower()
        if sa == "claim":
            if not scope:
                raise ValueError("`scope` is required for scope_action='claim'.")
            res = await asyncio.to_thread(
                _swarm_mgr.board_service.claim_scope,
                scope=scope,
                holder=holder or models.Role.COORDINATOR.value,
                notes=notes,
                force=bool(force),
                swarm_id=sid,
            )
            return _json_str(res)
        if sa == "release":
            res = await asyncio.to_thread(
                _swarm_mgr.board_service.release_scope,
                scope=scope,
                holder=holder,
                swarm_id=sid,
            )
            return _json_str(res)
        if sa == "list":
            claims = await asyncio.to_thread(_swarm_mgr.board_service.list_claims, swarm_id=sid)
            return _json_str(claims)
        raise ValueError(f"Unsupported scope_action: {scope_action!r}")

    if mark_read_by is None:
        effective_mark_read_by: str | None = (
            None
            if status
            else (str(agent).strip() if agent and str(agent).strip() else models.Role.COORDINATOR.value)
        )
    else:
        effective_mark_read_by = mark_read_by.strip() or None

    res = await asyncio.to_thread(
        _swarm_mgr.board_service.read_messages,
        swarm_id=sid,
        since=since,
        seqs=seqs,
        status=status,
        agent=agent,
        channel=channel,
        mention=mention,
        sender=sender,
        msg_type=kind,
        limit=limit,
        mark_read_by=effective_mark_read_by,
        include_archive=bool(include_archive),
        before_seq=before_seq,
        search=search,
        full=bool(full),
    )
    return _json_str(res)


@mcp.tool()
async def swarm_wait(
    since: int = 0,
    mentions_only: bool = True,
    timeout_seconds: float = 120.0,
    target: str = "coordinator",
    swarm_id: str | None = None,
    conversation_id: str | None = None,
    workspace: str | None = None,
    ctx: Context | None = None,
) -> str:
    """Block zero-cost until new matching Swarm Board activity arrives after `since` (capped at 150s)."""
    sid, _ = await _resolve_mcp_swarm(swarm_id=swarm_id, conversation_id=conversation_id, workspace=workspace, ctx=ctx)
    try:
        raw_t = float(timeout_seconds)
    except (TypeError, ValueError):
        raw_t = 120.0
    if not math.isfinite(raw_t):
        raw_t = 120.0
    clamped_timeout = max(0.2, min(150.0, raw_t))
    res = await _swarm_mgr.board_service.wait_for_update(
        swarm_id=sid,
        since=since,
        target=target or models.Role.COORDINATOR.value,
        mentions_only=bool(mentions_only),
        timeout_seconds=clamped_timeout,
    )
    return _json_str(res)


def main() -> None:
    global _session_swarm_id
    parser = argparse.ArgumentParser(description="Hermes A2A Swarm MCP Server")
    parser.add_argument("--http", action="store_true", help="Run standalone A2A + Observer HTTP server.")
    parser.add_argument("--host", default=os.environ.get("HERMES_A2A_HTTP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("HERMES_A2A_HTTP_PORT", "9910")))
    parser.add_argument("--swarm", default=os.environ.get("HERMES_A2A_SWARM_ID") or None)
    parser.add_argument("--with-http", action="store_true", help="Start embedded HTTP server alongside stdio MCP.")
    parser.add_argument("--no-http", action="store_true", help="Disable embedded HTTP server.")
    args = parser.parse_args()

    if args.swarm:
        clean_swarm = store.sanitize_swarm_id(args.swarm)
        if clean_swarm:
            _session_swarm_id = clean_swarm
            os.environ["HERMES_A2A_SWARM_ID"] = clean_swarm

    if args.http:
        import uvicorn

        token = os.environ.get("HERMES_A2A_HTTP_TOKEN") or None
        if args.host not in ("127.0.0.1", "localhost", "::1") and not token:
            raise ValueError("Refusing non-loopback bind without HERMES_A2A_HTTP_TOKEN set.")

        client_host = "127.0.0.1" if args.host in ("0.0.0.0", "") else ("::1" if args.host in ("::", "[::]") else args.host)
        host_fmt = f"[{client_host}]" if ":" in client_host and not client_host.startswith("[") else client_host
        url = f"http://{host_fmt}:{args.port}"
        os.environ["HERMES_A2A_HTTP_URL"] = url
        _swarm_mgr.set_http_url(url)
        _swarm_mgr.get_internal_token()
        store.register_server_instance(http_url=url, swarm_id=_session_swarm_id or "default")
        try:
            app = http_server.create_a2a_app(_swarm_mgr, http_url=url, bearer_token=token)
            uvicorn.run(app, host=args.host, port=args.port, log_level="info")
        finally:
            store.unregister_server_instance(http_url=url)
        return

    auto_http = not args.no_http and os.environ.get("HERMES_A2A_NO_AUTO_HTTP", "").strip() != "1"
    if args.with_http or auto_http:
        try:
            url = _http_ctrl.start(host=args.host, port=args.port)
            if url:
                os.environ["HERMES_A2A_HTTP_URL"] = url
                _swarm_mgr.set_http_url(url)
        except Exception as exc:
            import sys

            print(f"[hermes-a2a] embedded HTTP controller start failed: {exc}", file=sys.stderr)

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
