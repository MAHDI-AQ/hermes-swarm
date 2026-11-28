"""Tier-2 Hermes A2A worker runtime, process lifecycle, Tier-3 subagent tracking, and SwarmManager."""

from __future__ import annotations

import asyncio
import atexit
import bisect
from collections import OrderedDict
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import secrets
import sqlite3
import sys
import threading
import time
from typing import Any, Awaitable, Callable, TypeVar

import yaml

import board as board_mod
import models
import store
import wakeup as wakeup_mod
import win_launcher
import worktree as worktree_mod

_T = TypeVar("_T")

MAX_PERSISTED_TASKS = 200
_TOOL_TRACE_LIMIT = 80
_RECENT_TOOLS_LIMIT = 12
_SUBAGENT_RECENT_TOOLS_LIMIT = 5
_LIVE_TRANSCRIPT_MAX_CHARS = 32_768
_SUBPROCESS_STREAM_LIMIT = 8 * 1024 * 1024


async def _iter_stream_lines(
    stream: asyncio.StreamReader,
    *,
    max_line_bytes: int = _SUBPROCESS_STREAM_LIMIT,
    chunk_size: int = 65536,
):
    """Yield `(line_bytes, was_truncated)` without raising `asyncio.LimitOverrunError`."""
    buf = bytearray()
    draining = False
    truncated_prefix = b""
    while True:
        chunk = await stream.read(chunk_size)
        if not chunk:
            if draining:
                yield truncated_prefix, True
            elif buf:
                yield bytes(buf), False
            break
        if draining:
            nl = chunk.find(b"\n")
            if nl != -1:
                draining = False
                yield truncated_prefix, True
                truncated_prefix = b""
                buf.extend(chunk[nl + 1 :])
            else:
                continue
        else:
            buf.extend(chunk)
        while True:
            nl = buf.find(b"\n")
            if nl != -1:
                line = bytes(buf[:nl])
                del buf[: nl + 1]
                yield line, False
                continue
            if len(buf) > max_line_bytes:
                truncated_prefix = bytes(buf[: min(len(buf), 16384)])
                buf.clear()
                draining = True
            break
_DELEG_ID_RE = re.compile(r"\b(deleg_[A-Za-z0-9_-]+)\b")
QUOTA_RE = re.compile(r"RESOURCE_EXHAUSTED|quota|\b429\b|exhausted your capacity|rate.?limit", re.I)
_SERVER_BOOT_TS = time.time()
_SECRET_INPUT_KEYS = frozenset({"token", "secret", "password", "passwd", "api_key", "apikey", "authorization", "auth"})
_PREVIEW_KEY_ORDER = (
    "path",
    "file_path",
    "target_path",
    "command",
    "cmd",
    "query",
    "pattern",
    "url",
    "goal",
    "task",
    "scope",
    "summary",
    "action",
    "name",
)
_CONTEXT_YAML_CACHE: dict[str, tuple[tuple[int, int, int], dict[str, Any]]] = {}
_CONTEXT_YAML_LOCK = threading.Lock()
_SQLITE_SCHEMA_CACHE: dict[tuple[str, str], tuple[int, set[str]]] = {}
_SQLITE_SCHEMA_CACHE_LOCK = threading.Lock()
_COMMITTED_TC_CACHE: dict[tuple[str, str, int], tuple[int, int, set[str], set[str]]] = {}
_COMMITTED_TC_CACHE_LOCK = threading.Lock()
_TRANSCRIPT_CACHE_LOCK = threading.Lock()
_TRANSCRIPT_PARSE_CACHE: OrderedDict[tuple[str, int, int, int], dict[str, Any]] = OrderedDict()
_TRANSCRIPT_SUMMARY_CACHE: OrderedDict[tuple[str, int, int, int], dict[str, Any]] = OrderedDict()
_TRANSCRIPT_PATH_KEYS: dict[str, tuple[str, int, int, int]] = {}
_TRANSCRIPT_CACHE_MAX = 64
COORDINATOR_STALE_MS = 60_000
COORDINATOR_RUNNING_STALE_MS = 120_000
_TERMINAL_TRANSCRIPT_STATUSES = frozenset({
    "DONE",
    "COMPLETED",
    "ERROR",
    "INVALID",
    "CANCELED",
    "CANCELLED",
    "STOPPED",
    "FAILED",
})
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_CONV_ID_RE = re.compile(r"^[A-Za-z0-9_-]{4,128}$")
_TRANSCRIPT_COMPLETED_AT_RE = re.compile(r"^Completed At:\s*(\S+)", re.MULTILINE)
_SUBAGENT_RESULT_CID_RE = re.compile(r'"conversationId"\s*:\s*"([A-Za-z0-9_-]{4,128})"')
_NON_SLUG_RE = re.compile(r"[^a-z0-9]+")
_DEFAULT_CONTEXT_LIMITS: tuple[tuple[str, int], ...] = (
    ("900k", 900_000),
    ("1m", 1_000_000),
    ("gpt-6", 272_000),
    ("gpt-5", 272_000),
    ("claude-", 200_000),
    ("gemini-", 1_000_000),
    ("deepseek-v4", 1_000_000),
    ("deepseek", 131_072),
    ("qwen", 131_072),
    ("llama", 131_072),
    ("glm", 131_072),
    ("minimax", 204_800),
    ("step", 256_000),
)


def _sanitize_tool_input(raw_input: Any) -> Any:
    if isinstance(raw_input, str):
        return raw_input[:1500] + ("...[truncated]" if len(raw_input) > 1500 else "")
    if isinstance(raw_input, dict):
        out: dict[str, Any] = {}
        for k, v in list(raw_input.items())[:25]:
            if isinstance(v, str) and len(v) > 1000:
                out[k] = v[:1000] + "...[truncated]"
            else:
                out[k] = v
        return out
    return raw_input


def _enqueue_nowait(q: asyncio.Queue[Any], item: Any) -> None:
    try:
        q.put_nowait(item)
    except asyncio.QueueFull:
        pass


def _enqueue_drop_oldest(q: asyncio.Queue[Any], item: Any) -> None:
    try:
        q.put_nowait(item)
    except asyncio.QueueFull:
        try:
            q.get_nowait()
            q.put_nowait(item)
        except Exception:
            pass


def _is_worktree_subpath(raw_path: str | None) -> bool:
    if not raw_path:
        return False
    norm = store.normalize_local_path(str(raw_path))
    if not norm:
        return False
    return any(part.lower() == ".worktrees" for part in Path(norm).parts)


def _canonical_workspace_path(raw_path: str | None) -> str | None:
    """Normalize a workspace path and strip any `.worktrees/<name>` suffix back to the repo root."""
    if not raw_path or not str(raw_path).strip():
        return None
    norm = store.normalize_local_path(str(raw_path))
    if not norm:
        return None
    p = Path(norm)
    parts_low = [part.lower() for part in p.parts]
    if ".worktrees" in parts_low:
        idx = parts_low.index(".worktrees")
        if idx > 0:
            return str(Path(*p.parts[:idx]))
    return norm


def _elapsed_ms(start_iso: str | None, *, ended_at: str | None = None) -> int:
    if not start_iso:
        return 0
    try:
        dt_start = datetime.fromisoformat(str(start_iso).replace("Z", "+00:00"))
        dt_end = (
            datetime.fromisoformat(str(ended_at).replace("Z", "+00:00"))
            if ended_at
            else datetime.now(timezone.utc)
        )
        return max(0, int((dt_end - dt_start).total_seconds() * 1000))
    except Exception:
        return 0


def _duration_seconds(start_iso: str | None, *, ended_at: str | None = None) -> int | None:
    if not start_iso:
        return None
    return _elapsed_ms(start_iso, ended_at=ended_at) // 1000


def _read_yaml_cached(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        st = path.stat()
        sig = (int(st.st_mtime_ns), int(st.st_size), int(getattr(st, "st_ino", 0)))
    except OSError:
        return {}
    key = str(path)
    with _CONTEXT_YAML_LOCK:
        cached = _CONTEXT_YAML_CACHE.get(key)
        if cached and cached[0] == sig:
            return cached[1]
    try:
        parsed = yaml.safe_load(path.read_text(encoding="utf-8", errors="replace"))
        data = parsed if isinstance(parsed, dict) else {}
    except Exception:
        data = {}
    with _CONTEXT_YAML_LOCK:
        _CONTEXT_YAML_CACHE[key] = (sig, data)
    return data


def _resolve_context_limit(
    model: str | None,
    provider: str | None = None,
    base_url: str | None = None,
) -> int:
    """Resolve model context limit cleanly from local Hermes caches/config without network calls."""
    model_str = str(model or "").strip()
    model_low = model_str.lower()
    base_clean = str(base_url or "").strip().rstrip("/").lower()
    hhome = store.default_hermes_home()

    cache_doc = _read_yaml_cached(hhome / "context_length_cache.yaml")
    ctx_map = cache_doc.get("context_lengths") if isinstance(cache_doc.get("context_lengths"), dict) else {}
    if model_low and ctx_map:
        if base_clean:
            for k, v in ctx_map.items():
                if str(k).rstrip("/").lower() == f"{model_low}@{base_clean}":
                    try:
                        return max(0, int(v))
                    except (TypeError, ValueError):
                        pass
        for k, v in ctx_map.items():
            kl = str(k).lower()
            head = kl.split("@", 1)[0]
            if head == model_low or head.endswith(f"/{model_low}"):
                try:
                    return max(0, int(v))
                except (TypeError, ValueError):
                    pass

    cfg_doc = _read_yaml_cached(hhome / "config.yaml")
    if isinstance(cfg_doc, dict):
        for cp in cfg_doc.get("custom_providers") or []:
            if not isinstance(cp, dict):
                continue
            models_dict = cp.get("models")
            if isinstance(models_dict, dict):
                for mk, mv in models_dict.items():
                    if str(mk).lower() == model_low and isinstance(mv, dict) and mv.get("context_length"):
                        try:
                            return max(0, int(mv["context_length"]))
                        except (TypeError, ValueError):
                            pass
        m_sec = cfg_doc.get("model")
        if isinstance(m_sec, dict):
            m_providers = m_sec.get("providers")
            prov_entries: list[dict[str, Any]] = []
            if isinstance(m_providers, dict):
                for pk, pv in m_providers.items():
                    if isinstance(pv, dict):
                        prov_entries.append(pv)
                        if str(pk).lower() == model_low and pv.get("context_length"):
                            try:
                                return max(0, int(pv["context_length"]))
                            except (TypeError, ValueError):
                                pass
            elif isinstance(m_providers, list):
                prov_entries.extend(p for p in m_providers if isinstance(p, dict))
            for pv in prov_entries:
                models_dict = pv.get("models")
                if isinstance(models_dict, dict):
                    for mk, mv in models_dict.items():
                        if str(mk).lower() == model_low and isinstance(mv, dict) and mv.get("context_length"):
                            try:
                                return max(0, int(mv["context_length"]))
                            except (TypeError, ValueError):
                                pass
                if pv.get("context_length") and str(pv.get("default") or "").strip().lower() == model_low:
                    try:
                        return max(0, int(pv["context_length"]))
                    except (TypeError, ValueError):
                        pass
            if m_sec.get("context_length"):
                def_m = str(m_sec.get("default") or "").strip().lower()
                if not model_low or not def_m or def_m == model_low:
                    try:
                        return max(0, int(m_sec["context_length"]))
                    except (TypeError, ValueError):
                        pass

    if model_low:
        for pat, limit in _DEFAULT_CONTEXT_LIMITS:
            if pat in model_low:
                return limit
    return 0


def _normalize_tokens_dict(
    raw: dict[str, Any] | None,
    *,
    context_used: int = 0,
    context_limit: int = 0,
) -> dict[str, Any]:
    """Normalize stream-json / SQLite / legacy token dicts into canonical telemetry shape."""
    src = raw if isinstance(raw, dict) else {}

    def _int_of(*keys: str) -> int:
        for k in keys:
            val = src.get(k)
            if val is not None and not isinstance(val, bool):
                try:
                    return max(0, int(val))
                except (TypeError, ValueError):
                    pass
        return 0

    raw_inp = _int_of("input", "input_tokens", "prompt_tokens")
    out = _int_of("output", "output_tokens", "completion_tokens")
    cache_read = _int_of("cachedInput", "cacheRead", "cache_read", "cache_read_tokens")
    if cache_read == 0:
        for det_key in ("prompt_tokens_details", "input_tokens_details"):
            det = src.get(det_key)
            if isinstance(det, dict):
                val = det.get("cached_tokens") or det.get("cache_read_tokens")
                if val is not None and not isinstance(val, bool):
                    try:
                        cache_read = max(0, int(val))
                        break
                    except (TypeError, ValueError):
                        pass
    cache_write = _int_of("cacheWrite", "cache_write", "cache_write_tokens")
    reasoning = _int_of("reasoning", "reasoning_tokens")
    if reasoning == 0:
        for det_key in ("completion_tokens_details", "output_tokens_details"):
            det = src.get(det_key)
            if isinstance(det, dict):
                val = det.get("reasoning_tokens")
                if val is not None and not isinstance(val, bool):
                    try:
                        reasoning = max(0, int(val))
                        break
                    except (TypeError, ValueError):
                        pass
    explicit_uncached_keys = ("pureInput", "pure_input", "uncachedInput", "uncached_input", "uncached_input_tokens")
    has_explicit_uncached = any(
        src.get(k) is not None and not isinstance(src.get(k), bool)
        for k in explicit_uncached_keys
    )
    if has_explicit_uncached:
        inp = _int_of(*explicit_uncached_keys)
    elif cache_read > 0 and raw_inp >= cache_read:
        inp = raw_inp - cache_read
    else:
        inp = raw_inp
    new_tokens = inp + out
    total_processed = inp + cache_read + cache_write + out

    raw_ctx_used = _int_of("contextUsed", "context_used")
    ctx_used = raw_ctx_used if raw_ctx_used > 0 else max(0, int(context_used or 0))
    raw_ctx_limit = _int_of("contextLimit", "context_limit")
    ctx_limit = raw_ctx_limit if raw_ctx_limit > 0 else max(0, int(context_limit or 0))

    raw_pct = src.get("contextPct") if src.get("contextPct") is not None else src.get("context_pct")
    if ctx_used > 0 and ctx_limit > 0:
        ctx_pct = round(min(100.0, (ctx_used / ctx_limit) * 100.0), 1)
    elif raw_pct is not None:
        try:
            ctx_pct = round(float(raw_pct), 1)
        except (TypeError, ValueError):
            ctx_pct = 0.0
    else:
        ctx_pct = 0.0

    return {
        "input": inp,
        "pureInput": inp,
        "uncachedInput": inp,
        "output": out,
        "cacheRead": cache_read,
        "cachedInput": cache_read,
        "cacheWrite": cache_write,
        "reasoning": reasoning,
        "total": total_processed,
        "totalProcessed": total_processed,
        "new": new_tokens,
        "contextUsed": ctx_used,
        "contextLimit": ctx_limit,
        "contextPct": ctx_pct,
    }


def _compute_tps(output_tokens: int, elapsed_ms: int) -> float:
    if output_tokens <= 0 or elapsed_ms <= 0:
        return 0.0
    return round(float(output_tokens) / (float(elapsed_ms) / 1000.0), 1)


def _format_tool_preview(tool_name: str, raw_input: Any) -> str:
    """Build a short, secret-free argument preview for `currentTool` and `recentTools`."""
    if raw_input is None:
        return ""
    val: Any = None
    if isinstance(raw_input, dict):
        if isinstance(raw_input.get("targets"), dict) and raw_input["targets"]:
            raw_input = raw_input["targets"]
        if tool_name == "delegate_task" and isinstance(raw_input.get("tasks"), list):
            tasks = raw_input["tasks"]
            first_goal = ""
            if tasks and isinstance(tasks[0], dict):
                first_goal = str(tasks[0].get("goal") or tasks[0].get("task") or "").strip()
            txt = f"{len(tasks)} tasks" + (f": {first_goal}" if first_goal else "")
            return re.sub(r"\s+", " ", txt)[:77] + ("..." if len(txt) > 80 else "")
        chosen_key = ""
        for k in _PREVIEW_KEY_ORDER:
            if k in raw_input and raw_input[k] not in (None, "", [], {}):
                chosen_key = k
                val = raw_input[k]
                break
        if val is None:
            for k, v in raw_input.items():
                if str(k).lower() in _SECRET_INPUT_KEYS or v in (None, "", [], {}):
                    continue
                chosen_key = str(k)
                val = v
                break
        if isinstance(val, (dict, list)):
            s = json.dumps(val, ensure_ascii=False)
        else:
            s = str(val or "")
        s = re.sub(r"\s+", " ", s).strip()
        if chosen_key in ("path", "file_path", "target_path") and ("/" in s or "\\" in s):
            parts = [p for p in s.replace("\\", "/").split("/") if p]
            if len(parts) >= 2:
                s = "/".join(parts[-2:])
            elif parts:
                s = parts[-1]
    else:
        s = re.sub(r"\s+", " ", str(raw_input)).strip()
    if len(s) > 80:
        return s[:77] + "..."
    return s


def _parse_delegate_task_results(raw_output: str) -> list[dict[str, Any]]:
    """Parse structured JSON entries returned by Hermes `delegate_task`."""
    txt = str(raw_output or "").strip()
    if not txt or not txt.startswith(("{", "[")):
        return []
    try:
        parsed = json.loads(txt)
    except Exception:
        return []
    if isinstance(parsed, dict):
        items = parsed.get("results")
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
        if "status" in parsed or "task_index" in parsed:
            return [parsed]
        return []
    if isinstance(parsed, list):
        return [x for x in parsed if isinstance(x, dict)]
    return []


def _query_hermes_session_stats(
    session_id: str,
    *,
    model: str | None = None,
    provider: str | None = None,
    base_url: str | None = None,
) -> tuple[dict[str, Any], float | None, dict[str, Any]]:
    """Read token counts, context usage, tool/message counts, and cost for one Hermes session from state.db."""
    if not session_id:
        return {}, None, {}
    db_path = store.default_hermes_home() / "state.db"
    if not db_path.is_file():
        return {}, None, {}
    con: sqlite3.Connection | None = None
    try:
        uri = f"{db_path.resolve().as_uri()}?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=2.0)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA busy_timeout = 2000")
            con.execute("PRAGMA query_only = ON")
        except sqlite3.Error:
            pass
        sess_cols = _sqlite_table_columns(con, "sessions", db_path=db_path)
        if not sess_cols or "id" not in sess_cols:
            return {}, None, {}
        wanted = [
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "reasoning_tokens",
            "estimated_cost_usd",
            "actual_cost_usd",
            "model",
            "model_config",
            "message_count",
            "tool_call_count",
            "started_at",
            "ended_at",
            "billing_provider",
            "billing_base_url",
        ]
        sel_cols = [c if c in sess_cols else f"NULL AS {c}" for c in wanted]
        row = con.execute(
            f"SELECT {', '.join(sel_cols)} FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        if row is None:
            return {}, None, {}
        d = dict(row)
        ctx_used = 0
        gw_prov = ""
        raw_mc = d.get("model_config")
        if isinstance(raw_mc, str) and raw_mc.strip():
            try:
                mc = json.loads(raw_mc)
                if isinstance(mc, dict):
                    anchor = mc.get("_usage_anchor")
                    if isinstance(anchor, dict):
                        ctx_used = int(anchor.get("prompt_tokens") or 0) + int(anchor.get("completion_tokens") or 0)
                    gw_rt = mc.get("gateway_runtime")
                    if isinstance(gw_rt, dict):
                        gw_prov = str(gw_rt.get("provider") or "").strip()
            except Exception:
                pass
        eff_model = str(d.get("model") or model or "").strip() or None
        eff_prov = str(d.get("billing_provider") or gw_prov or provider or "").strip() or None
        eff_base = str(d.get("billing_base_url") or base_url or "").strip() or None
        ctx_limit = _resolve_context_limit(eff_model, eff_prov, eff_base)
        tokens = _normalize_tokens_dict(
            {
                "input_tokens": int(d.get("input_tokens") or 0),
                "uncachedInput": int(d.get("input_tokens") or 0),
                "output_tokens": int(d.get("output_tokens") or 0),
                "cache_read_tokens": int(d.get("cache_read_tokens") or 0),
                "cache_write_tokens": int(d.get("cache_write_tokens") or 0),
                "reasoning_tokens": int(d.get("reasoning_tokens") or 0),
            },
            context_limit=ctx_limit,
            context_used=ctx_used,
        )
        actual = d.get("actual_cost_usd")
        est = d.get("estimated_cost_usd")
        actual_f: float | None = None
        est_f: float | None = None
        if actual is not None:
            try:
                actual_f = float(actual)
            except (TypeError, ValueError):
                actual_f = None
        if est is not None:
            try:
                est_f = float(est)
            except (TypeError, ValueError):
                est_f = None
        if actual_f is not None and actual_f > 0.0:
            cost = round(actual_f, 6)
        elif est_f is not None and est_f > 0.0:
            cost = round(est_f, 6)
        elif (eff_prov or "").lower() == "custom":
            cost = None
        elif actual_f is not None:
            cost = round(actual_f, 6)
        else:
            cost = None
        extra = {
            "model": eff_model,
            "billingProvider": eff_prov,
            "gatewayProvider": gw_prov or None,
            "messageCount": int(d.get("message_count") or 0),
            "toolCallCount": int(d.get("tool_call_count") or 0),
            "startedAtEpoch": d.get("started_at"),
            "endedAtEpoch": d.get("ended_at"),
        }
        return tokens, cost, extra
    except Exception:
        return {}, None, {}
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _query_hermes_subagent_sessions(parent_session_id: str) -> list[dict[str, Any]]:
    """Read live Tier-3 subagent session rows from state.db for `parent_session_id` (read-only)."""
    if not parent_session_id:
        return []
    db_path = store.default_hermes_home() / "state.db"
    if not db_path.is_file():
        return []
    con: sqlite3.Connection | None = None
    try:
        uri = f"{db_path.resolve().as_uri()}?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=2.0)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA busy_timeout = 2000")
            con.execute("PRAGMA query_only = ON")
        except sqlite3.Error:
            pass
        rows = con.execute(
            "SELECT id, model, started_at, ended_at, message_count, tool_call_count, "
            "input_tokens, output_tokens, model_config "
            "FROM sessions WHERE parent_session_id = ? AND source = 'subagent' ORDER BY started_at ASC",
            (parent_session_id,),
        ).fetchall()
        out: list[dict[str, Any]] = []
        now_s = time.time()
        for r in rows:
            d = dict(r)
            sub_id = ""
            raw_mc = d.get("model_config")
            if isinstance(raw_mc, str) and raw_mc.strip():
                try:
                    mc = json.loads(raw_mc)
                    if isinstance(mc, dict):
                        sub_id = str(mc.get("_subagent_id") or "")
                except Exception:
                    pass
            started = float(d.get("started_at") or now_s)
            ended = float(d.get("ended_at") or now_s)
            elapsed_ms = max(0, int((ended - started) * 1000))
            in_tok = int(d.get("input_tokens") or 0)
            out_tok = int(d.get("output_tokens") or 0)
            msg_cnt = int(d.get("message_count") or 0)
            tc_cnt = int(d.get("tool_call_count") or 0)
            out.append({
                "sessionId": d.get("id"),
                "subagentId": sub_id,
                "model": d.get("model"),
                "toolCalls": tc_cnt,
                "steps": max(1 if (msg_cnt > 0 or tc_cnt > 0) else 0, (msg_cnt + 1) // 3),
                "tokens": {"input": in_tok, "output": out_tok, "total": in_tok + out_tok},
                "elapsedMs": elapsed_ms,
                "tps": _compute_tps(out_tok, elapsed_ms),
                "done": d.get("ended_at") is not None,
                "startedAt": started,
            })
        return out
    except Exception:
        return []
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def build_worker_preamble(
    cfg: models.WorkerConfig,
    *,
    swarm_id: str,
    task_id: str,
    context_id: str,
    board_dir: Path,
    http_url: str | None = None,
    token_file: Path | None = None,
    conversation_id: str | None = None,
) -> str:
    """Build the native-tool-first and 3-tier reporting preamble for a Tier-2 Hermes worker."""
    role = models.validate_worker_role(cfg.role)
    name = cfg.worker_name or f"{role}-{task_id[-6:]}"
    py_posix = Path(sys.executable).as_posix()
    cli_posix = (Path(__file__).resolve().parent / "board_cli.py").as_posix()
    bdir_posix = board_dir.as_posix()

    tool_ctx_parts = [
        f'swarm_id="{swarm_id}"',
        f'agent="{name}"',
        f'board_dir="{bdir_posix}"',
    ]
    if http_url:
        tool_ctx_parts.append(f'http_url="{http_url}"')
    if conversation_id:
        tool_ctx_parts.append(f'conversation_id="{conversation_id}"')
    if token_file is not None:
        tool_ctx_parts.append(f'token_file="{token_file.as_posix()}"')
    tool_ctx_args = ", ".join(tool_ctx_parts)

    cmd_parts = [
        f'"{py_posix}"',
        f'"{cli_posix}"',
        f'--board-dir "{bdir_posix}"',
        f'--swarm "{swarm_id}"',
        f'--agent "{name}"',
        f'--role "{role}"',
        f'--task-id "{task_id}"',
        f'--context-id "{context_id}"',
    ]
    if http_url:
        cmd_parts.append(f'--http-url "{http_url}"')
    if token_file is not None:
        cmd_parts.append(f'--token-file "{token_file.as_posix()}"')
    board_cmd = " ".join(cmd_parts)

    channels_str = ", ".join(board_mod.channel_display_name(c) for c in (cfg.channels or [models.DEFAULT_CHANNEL]))
    parent_note = ""
    if cfg.fork_from:
        parent_note = f"\nForked From: @{cfg.fork_from}"
    elif cfg.parent_worker:
        parent_note = f"\nParent Worker: @{cfg.parent_worker}"

    if role == models.Role.NARRATOR.value:
        return (
            f"[TIER-2 SWARM NARRATOR ({name}) | SWARM_DIR={bdir_posix}]\n"
            "Rules:\n"
            "- Never edit repository files, never run builds/tests, and NEVER call `swarm_post` or `board_cli.py post`.\n"
            f"- Your ONLY output is `{bdir_posix}/view.json` (under 64 KB, written atomically via temp file + rename)"
            " and optional `{bdir_posix}/view.html` if explicitly requested.\n".format(bdir_posix=bdir_posix)
            + f"- Use native `swarm_read` and `swarm_wait` tools FIRST on every call passing: {tool_ctx_args}\n"
            "- Read posts via `swarm_read(since=0, limit=200, ...)` and wait via `swarm_wait(since=<LAST_SEQ>, mentions_only=false, timeout_seconds=300, ...)`.\n"
            f"- Emergency fallback ONLY if `swarm_read`/`swarm_wait` is unavailable: {board_cmd} read --since 0 --limit 200 | {board_cmd} wait --since <LAST_SEQ> --mentions-only false --timeout 300\n"
            "- Group `threads` by outcome (1 to 6 threads), cite `#seq` refs on every workstream/milestone/risk, "
            "and only mark workstreams done once confirmed by worker/verifier posts."
        )

    if role == models.Role.MONITOR.value:
        return (
            f"[TIER-2 SWARM MONITOR ({name}) | SWARM_DIR={bdir_posix}]\n"
            "Rules:\n"
            "- Never do technical work (no repo edits, no builds, no code reviews, no user replies).\n"
            "- Post ONLY to break a genuine stall (1 concise post per stall, citing `#seq` and member/task states). "
            "Address `@coordinator` or the single stalled peer. Never use `@all`, never `@primary`, never message `narrator`.\n"
            f"- Use native `swarm_read`, `swarm_status`, `swarm_wait`, `swarm_post` tools FIRST passing: {tool_ctx_args}\n"
            "- Park & wait via `swarm_wait(since=<LAST_SEQ>, mentions_only=true, timeout_seconds=300, ...)`.\n"
            f"- Emergency fallback ONLY if `swarm_post`/`swarm_wait` is unavailable: {board_cmd} status | {board_cmd} wait --since <LAST_SEQ> --mentions-only true --timeout 300 | {board_cmd} post --channel main --kind post --summary \"@coordinator [stall] ...\""
        )

    role_guidance = {
        models.Role.WORKER.value: (
            "Own your assigned lane cleanly inside the active workspace/worktree. "
            "Delegate focused leaf discovery, isolated edits, or test runs to Tier-3 Hermes subagents via `delegate_task` "
            "(synchronous/batched `delegate_task`; avoid leaving detached background subagents past turn end)."
        ),
        models.Role.VERIFIER.value: (
            "Independently verify claims against actual files, diffs, and live command/test outputs. "
            "Report PASS/FAIL with exact observed counts and file citations."
        ),
        models.Role.RESEARCHER.value: (
            "Investigate the assigned codebase or technical question thoroughly and return cited file/line/doc findings."
        ),
    }.get(role, "Execute your assigned task cleanly and verify your work.")

    return (
        f"[TIER-2 HERMES {role.upper()} ({name}) — REPORTS TO @coordinator | Channels: {channels_str}]{parent_note}\n"
        f"1. {role_guidance}\n"
        "2. 3-Tier Reporting Law: Report results, handoffs, blockers, and questions to `@coordinator` or the next acting peer `@<name>` "
        "on the Swarm Board. Never claim to be primary or coordinator.\n"
        "3. Anti-Ping-Pong Rule: NEVER `@mention` a peer or `@coordinator` merely to say accepted, confirmed, thanks, acknowledged, or "
        "already-answered — `@name` is strictly a wakeup doorbell when the target must perform NEW work. If no further work is needed, "
        "end your turn without posting an `@mention`.\n"
        "4. Native Swarm Board Tools FIRST (use `swarm_post`, `swarm_read`, `swarm_claim`, `swarm_release`, `swarm_status`, `swarm_wait` "
        "with these explicit arguments on every call so they work cleanly even inside persistent `hermes-gateway`):\n"
        f"   - Required context args on every native `swarm_*` call: {tool_ctx_args}\n"
        f'   - Post update / handoff / [done]: swarm_post({tool_ctx_args}, channel="main", kind="post|question|handoff|escalation", summary="@coordinator [done] ...", details="...", files=["a.py"])\n'
        f'   - Post milestone progress (no @mention so zero wakeup spam): swarm_post({tool_ctx_args}, channel="main", kind="post", summary="[progress] <phase done>; starting <next phase>", details="...") — post briefly at major phase transitions and call `swarm_read` before starting any long build/GPU/benchmark run (urgent mid-turn `@mention`s also inject automatically).\n'
        f'   - Wake a peer: swarm_post({tool_ctx_args}, channel="main", kind="handoff", summary="@<peer> ...", details="...")\n'
        f'   - Read / status / wait: swarm_read({tool_ctx_args}, since=0) | swarm_status({tool_ctx_args}) | swarm_wait({tool_ctx_args}, since=<LAST_SEQ>, mentions_only=true, timeout_seconds=120)\n'
        f'   - Claim / release scope: swarm_claim({tool_ctx_args}, scope="path/or/lane", notes="...") | swarm_release({tool_ctx_args}, scope="path/or/lane")\n'
        f"5. Emergency fallback ONLY if native `swarm_post`/`swarm_wait` is unavailable: {board_cmd} post --channel main --kind <post|question|handoff|escalation> --summary \"@coordinator [done] ...\" --details \"...\"\n"
        "6. NEVER wrap `swarm_*` calls (especially `swarm_post` or `swarm_release`) inside a batched `tool_call(calls=...)` JSON wrapper — always call `swarm_post` directly as its own native tool call so Markdown/JSON escaping never fails. NEVER send diagnostic/test probe posts (such as \"Probe post.\") to the board, and NEVER include `[done]` in `summary` until your final report is genuinely complete.\n"
        "7. If blocked waiting on `@coordinator`, post a `question`/`escalation` on the board AND end your turn with a line starting with `[INPUT_REQUIRED]`."
    )


_PREAMBLE_LINE_RE = re.compile(
    r"^(?:You\s+are\s+[`'\"*]?[\w-]+[`'\"*]?\s+in\s+(?:this\s+)?(?:Hermes\s+)?swarm\b.*|\[SYSTEM\s*/\s*ROLE\]|\[TASK\]|Ownership(?:\s*&\s*Locks)?\s*:|Rules\s*:|Constraints\s*:|Completion\s*:)\s*$",
    re.IGNORECASE,
)
_PREAMBLE_INLINE_SWARM_RE = re.compile(
    r"^You\s+are\s+[`'\"*]?[\w-]+[`'\"*]?\s+in\s+(?:this\s+)?(?:Hermes\s+)?swarm\b(?:\s*\([^)]*(?:\)|$))?[.:—–\-\s]*",
    re.IGNORECASE,
)
_TASK_HEADER_RE = re.compile(r"^\s*(?:#+\s*)?(?:task|assignment|goal)\s*:\s*(.*)$", re.IGNORECASE)
_SECTION_STOP_RE = re.compile(r"^\s*(?:#+\s*)?(?:ownership(?:\s*&\s*locks)?|completion|rules|constraints)\s*:\s*$", re.IGNORECASE)


def _extract_task_assignment(prompt_body: str) -> str:
    """Extract a concise human-readable task summary from `prompt_body` without swarm/ownership boilerplate."""
    text = str(prompt_body or "").strip()
    if not text:
        return ""
    lines = [ln.strip() for ln in text.replace("\r\n", "\n").splitlines()]

    def _clean_line(s: str) -> str:
        s = re.sub(r"^\s*(?:[-*]|\d+[.)]|#+)\s+", "", s)
        s = re.sub(r"\*\*([^*]+)\*\*", r"\1", s)
        s = s.replace("`", "").replace("__", "")
        return re.sub(r"\s+", " ", s).strip()

    for idx, raw_ln in enumerate(lines):
        m_hdr = _TASK_HEADER_RE.match(raw_ln)
        if m_hdr:
            inline = _clean_line(m_hdr.group(1) or "")
            if inline:
                return inline[:240]
            for sub in lines[idx + 1 :]:
                if not sub:
                    continue
                if _SECTION_STOP_RE.match(sub):
                    break
                cleaned = _clean_line(sub)
                if cleaned:
                    return cleaned[:240]

    in_ignored_section = False
    for raw_ln in lines:
        if not raw_ln:
            in_ignored_section = False
            continue
        if _SECTION_STOP_RE.match(raw_ln) or _PREAMBLE_LINE_RE.match(raw_ln):
            in_ignored_section = True
            continue
        stripped_swarm = _PREAMBLE_INLINE_SWARM_RE.sub("", raw_ln).strip()
        if not stripped_swarm:
            continue
        if in_ignored_section and raw_ln.startswith(("-", "*")):
            continue
        in_ignored_section = False
        cleaned = _clean_line(stripped_swarm)
        if cleaned:
            return cleaned[:240]

    return text[:240]


class SwarmRuntime:
    """Manages A2A v1.0 Tasks and Tier-2 Hermes worker subprocesses for a single swarm."""

    def __init__(
        self,
        *,
        swarm_id: str = "default",
        board_service: board_mod.BoardService,
        mention_router: wakeup_mod.MentionRouter,
        state_dir: Path | None = None,
        http_url: str | None = None,
        token_file_provider: Callable[[], Path | None] | None = None,
        on_board_event: Callable[[str, str, dict[str, Any]], None] | None = None,
        on_bind_loop: Callable[[asyncio.AbstractEventLoop], None] | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self.swarm_id = store.sanitize_swarm_id(swarm_id) or "default"
        self._board = board_service
        self._router = mention_router
        self._state_dir = state_dir
        self._token_file_provider = token_file_provider
        self._on_board_event = on_board_event
        self._on_bind_loop = on_bind_loop
        self._loop: asyncio.AbstractEventLoop | None = loop
        self.http_url: str | None = http_url

        # Dedicated in-memory state lock decoupled from BoardService / log file locks.
        self._state_rlock = threading.RLock()
        self._async_lock = asyncio.Lock()
        self._async_lock_loop: asyncio.AbstractEventLoop | None = loop

        self._tasks: dict[str, dict[str, Any]] = {}
        self._context_sessions: dict[str, str] = {}
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._bg_jobs: dict[str, asyncio.Task[None]] = {}
        self._events: dict[str, asyncio.Event] = {}
        self._subscribers: dict[str, list[tuple[asyncio.Queue[dict[str, Any] | None], asyncio.AbstractEventLoop | None]]] = {}
        self._live_buffers: dict[str, dict[str, Any]] = {}
        self._last_progress_save = 0.0
        self._last_active_worker_count = 0
        self._killed_workers: set[str] = set()
        self._suppress_all_finished_wake = False

        self._load_state()
        try:
            b_init = self._board.read_board(self.swarm_id)
            for mname, mrec in (b_init.get("members") or {}).items():
                if isinstance(mrec, dict) and (
                    mrec.get("killed") or str(mrec.get("state") or "").strip().lower() == models.MemberState.KILLED.value
                ):
                    self._killed_workers.add(str(mname).strip().lower())
        except Exception:
            pass
        with self._state_rlock:
            self._last_active_worker_count = self._count_active_workers_unlocked()
        if self._loop is not None and not self._loop.is_closed() and self._loop.is_running():
            self._router.start_watcher(self._loop)
        atexit.register(self._cleanup_on_exit)

    def _paths(self) -> store.SwarmPaths:
        return store.resolve_swarm_paths(self.swarm_id, state_dir=self._state_dir)

    def _count_active_workers_unlocked(self) -> int:
        count = 0
        for t in self._tasks.values():
            st = str(((t.get("status") or {}).get("state")) or "")
            if st not in (models.TaskState.WORKING.value, models.TaskState.SUBMITTED.value):
                continue
            wcfg = (t.get("metadata") or {}).get("workerConfig") or {}
            if str(wcfg.get("role") or "").strip().lower() == models.Role.SUBAGENT.value:
                continue
            wname = str(wcfg.get("worker_name") or "").strip().lower()
            if wname and wname in self._killed_workers:
                continue
            count += 1
        return count

    def get_active_worker_count(self) -> int:
        with self._state_rlock:
            return self._count_active_workers_unlocked()

    def bind_loop(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        target = loop
        if target is None:
            try:
                target = asyncio.get_running_loop()
            except RuntimeError:
                return
        self._loop = target
        if not target.is_closed() and target.is_running():
            if self._on_bind_loop is not None:
                try:
                    self._on_bind_loop(target)
                except Exception:
                    pass
            self._router.start_watcher(target)

    def _ensure_async_lock(self, cur_loop: asyncio.AbstractEventLoop) -> None:
        if self._async_lock_loop is not cur_loop:
            self._async_lock = asyncio.Lock()
            self._async_lock_loop = cur_loop

    async def run_on_loop(self, coro_fn: Callable[..., Awaitable[_T]], *args: Any, **kwargs: Any) -> _T:
        cur_loop = asyncio.get_running_loop()
        target_loop = self._loop
        if target_loop is not None and not target_loop.is_closed() and target_loop.is_running() and target_loop is not cur_loop:
            cfut = asyncio.run_coroutine_threadsafe(coro_fn(*args, **kwargs), target_loop)
            return await asyncio.wrap_future(cfut)
        self.bind_loop(cur_loop)
        self._ensure_async_lock(cur_loop)
        return await coro_fn(*args, **kwargs)

    def _cleanup_on_exit(self) -> None:
        for proc in list(self._procs.values()):
            if proc.returncode is None and proc.pid:
                win_launcher.kill_process_tree_sync(proc.pid, timeout=2.0)

    def _load_state(self) -> None:
        tfile = self._paths().tasks_file
        if not tfile.is_file():
            return
        raw = store.read_json_safe(tfile, None)
        if not isinstance(raw, dict):
            return
        tasks = raw.get("tasks") or {}
        ctx_map = raw.get("context_sessions") or {}
        reset_workers: list[str] = []
        with self._state_rlock:
            if isinstance(tasks, dict):
                for tid, task in tasks.items():
                    if not isinstance(task, dict):
                        continue
                    st = ((task.get("status") or {}).get("state")) or ""
                    if st not in models.TERMINAL_STATES and st not in models.INTERRUPTED_STATES:
                        task["status"] = {
                            "state": models.TaskState.FAILED.value,
                            "message": models.new_message(
                                "ROLE_AGENT",
                                [models.text_part("Worker process ended because the server restarted.")],
                                context_id=task.get("contextId"),
                                task_id=tid,
                            ),
                            "timestamp": models.now_iso(),
                        }
                        wcfg = (task.get("metadata") or {}).get("workerConfig") or {}
                        wname = str(wcfg.get("worker_name") or "").strip()
                        if wname and wname not in reset_workers:
                            reset_workers.append(wname)
                    self._tasks[tid] = task
            if isinstance(ctx_map, dict):
                self._context_sessions.update({str(k): str(v) for k, v in ctx_map.items()})
        if reset_workers:
            try:
                board = self._board.read_board(self.swarm_id)
                members = board.get("members") or {}
                for wname in reset_workers:
                    mem = members.get(wname)
                    if isinstance(mem, dict) and not mem.get("killed") and str(mem.get("state") or "").lower() == models.MemberState.WORKING.value:
                        self._board.upsert_member(
                            wname,
                            role=str(mem.get("role") or models.Role.WORKER.value),
                            state=models.MemberState.STOPPED.value,
                            swarm_id=self.swarm_id,
                        )
            except Exception:
                pass

    def _snapshot_state_payload(self) -> tuple[Any, dict[str, Any]]:
        paths = self._paths()
        with self._state_rlock:
            ordered = sorted(
                self._tasks.items(),
                key=lambda kv: ((kv[1].get("status") or {}).get("timestamp") or ""),
                reverse=True,
            )[:MAX_PERSISTED_TASKS]
            payload = {
                "tasks": dict(ordered),
                "context_sessions": dict(self._context_sessions),
            }
        return paths, payload

    @staticmethod
    def _write_state_payload_sync(paths: Any, payload: dict[str, Any]) -> None:
        with store.atomic_file_lock(paths.tasks_lock):
            store.atomic_write_json(paths.tasks_file, payload)

    def _save_state(self) -> None:
        paths, payload = self._snapshot_state_payload()
        self._write_state_payload_sync(paths, payload)

    async def _save_state_async(self) -> None:
        paths, payload = self._snapshot_state_payload()
        await asyncio.to_thread(self._write_state_payload_sync, paths, payload)

    async def _flush_progress_save(self) -> None:
        try:
            async with self._async_lock:
                await self._save_state_async()
        except Exception:
            pass

    def _maybe_save_progress(self, min_interval_s: float = 3.0) -> None:
        now_m = time.monotonic()
        if (now_m - self._last_progress_save) >= min_interval_s:
            self._last_progress_save = now_m
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is not None and not loop.is_closed():
                prev = getattr(self, "_progress_save_task", None)
                if prev is None or prev.done():
                    self._progress_save_task = loop.create_task(self._flush_progress_save())
                return
            try:
                self._save_state()
            except Exception:
                pass

    def _emit_observer_event(self, event_type: str, payload: dict[str, Any]) -> None:
        if self._on_board_event is not None:
            try:
                self._on_board_event(self.swarm_id, event_type, payload)
            except Exception:
                pass

    def _publish_stream_event(self, task_id: str, event: dict[str, Any] | None) -> None:
        try:
            cur_loop = asyncio.get_running_loop()
        except RuntimeError:
            cur_loop = None
        for q, sub_loop in list(self._subscribers.get(task_id) or []):
            if sub_loop is not None and not sub_loop.is_closed() and sub_loop.is_running() and sub_loop is not cur_loop:
                try:
                    sub_loop.call_soon_threadsafe(_enqueue_nowait, q, event)
                except RuntimeError:
                    pass
            else:
                _enqueue_nowait(q, event)

    def subscribe(self, task_id: str) -> asyncio.Queue[dict[str, Any] | None]:
        q: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=256)
        try:
            sub_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            sub_loop = None
        self._subscribers.setdefault(task_id, []).append((q, sub_loop))
        return q

    def unsubscribe(self, task_id: str, q: asyncio.Queue[dict[str, Any] | None]) -> None:
        subs = self._subscribers.get(task_id)
        if not subs:
            return
        self._subscribers[task_id] = [item for item in subs if item[0] is not q]
        if not self._subscribers[task_id]:
            self._subscribers.pop(task_id, None)

    def _append_live_transcript(self, task_id: str, delta: str) -> None:
        if not delta:
            return
        with self._state_rlock:
            buf = self._live_buffers.setdefault(
                task_id, {"liveTranscript": "", "liveReasoning": "", "liveToolTrace": []}
            )
            txt = str(buf.get("liveTranscript") or "") + delta
            if len(txt) > _LIVE_TRANSCRIPT_MAX_CHARS:
                txt = txt[-_LIVE_TRANSCRIPT_MAX_CHARS:]
            buf["liveTranscript"] = txt

    def _append_live_reasoning(self, task_id: str, delta: str) -> None:
        if not delta:
            return
        with self._state_rlock:
            buf = self._live_buffers.setdefault(
                task_id, {"liveTranscript": "", "liveReasoning": "", "liveToolTrace": []}
            )
            txt = str(buf.get("liveReasoning") or "") + delta
            if len(txt) > _LIVE_TRANSCRIPT_MAX_CHARS:
                txt = txt[-_LIVE_TRANSCRIPT_MAX_CHARS:]
            buf["liveReasoning"] = txt

    def _reset_live_step_buffers(self, task_id: str) -> None:
        """Clear active-step transcript/reasoning when advancing past a committed tool/step boundary."""
        with self._state_rlock:
            buf = self._live_buffers.get(task_id)
            if buf is not None:
                buf["liveTranscript"] = ""
                buf["liveReasoning"] = ""

    def _append_live_tool_trace(self, task_id: str, entry: dict[str, Any]) -> None:
        with self._state_rlock:
            buf = self._live_buffers.setdefault(
                task_id, {"liveTranscript": "", "liveReasoning": "", "liveToolTrace": []}
            )
            trace: list[dict[str, Any]] = buf.setdefault("liveToolTrace", [])
            trace.append(entry)
            if len(trace) > _TOOL_TRACE_LIMIT:
                del trace[: len(trace) - _TOOL_TRACE_LIMIT]

    def _enrich_view_telemetry_unlocked(self, view: dict[str, Any]) -> None:
        meta = view.setdefault("metadata", {})
        act = meta.get("liveActivity")
        if not isinstance(act, dict):
            return
        st = str((view.get("status") or {}).get("state") or "")
        is_terminal = st in models.TERMINAL_STATES
        ended = act.get("endedAt") or ((view.get("status") or {}).get("timestamp") if is_terminal else None)
        started_at = act.get("startedAt") or meta.get("createdAt")
        elapsed_ms = int(act.get("elapsedMs") or 0)
        if not is_terminal or elapsed_ms <= 0:
            computed_ms = _elapsed_ms(started_at, ended_at=ended)
            if computed_ms > 0:
                elapsed_ms = computed_ms
        act["elapsedMs"] = elapsed_ms
        act["elapsedSeconds"] = elapsed_ms // 1000
        act["idleSinceEventSeconds"] = _duration_seconds(
            act.get("lastEventAt") or started_at, ended_at=ended
        )
        if is_terminal:
            act["currentTool"] = None
        wcfg = meta.get("workerConfig") or {}
        raw_tok = meta.get("tokens")
        if isinstance(raw_tok, dict) and raw_tok:
            norm_tok = _normalize_tokens_dict(
                raw_tok,
                context_limit=_resolve_context_limit(wcfg.get("model"), wcfg.get("provider"), wcfg.get("base_url")),
            )
            meta["tokens"] = norm_tok
            llm_ms = int(act.get("llmActiveMs") or 0)
            tps_ms = llm_ms if llm_ms >= 250 else elapsed_ms
            turn_out = act.get("turnOutputTokens")
            tps_tokens = int(turn_out) if turn_out is not None else norm_tok["output"]
            if tps_tokens > 0:
                act["tps"] = _compute_tps(tps_tokens, tps_ms)
                act["tpsEstimated"] = False
            else:
                streamed_chars = int(act.get("streamedChars") or 0)
                if streamed_chars > 0 and tps_ms > 200:
                    act["tps"] = round(max(1, streamed_chars // 4) / (tps_ms / 1000.0), 1)
                    act["tpsEstimated"] = True
                elif act.get("tpsEstimated") and float(act.get("tps") or 0.0) > 0.0:
                    act["tps"] = float(act.get("tps") or 0.0)
                else:
                    act["tps"] = 0.0
                    act["tpsEstimated"] = False
        else:
            act["tps"] = float(act.get("tps") or 0.0)

    def get_task_snapshots(self, *, include_artifacts: bool = True) -> list[dict[str, Any]]:
        """Thread-safe snapshot of persisted/in-memory tasks for cross-thread HTTP observers."""
        with self._state_rlock:
            out: list[dict[str, Any]] = []
            for tid, task in self._tasks.items():
                view = models.format_task_view(task, include_artifacts=include_artifacts)
                buf = self._live_buffers.get(tid)
                meta = view.setdefault("metadata", {})
                if buf:
                    meta["liveTranscript"] = str(buf.get("liveTranscript") or "")
                    meta["liveReasoning"] = str(buf.get("liveReasoning") or "")
                    meta["liveToolTrace"] = [dict(x) for x in (buf.get("liveToolTrace") or [])]
                self._enrich_view_telemetry_unlocked(view)
                out.append(view)
            out.sort(key=lambda t: str(((t.get("status") or {}).get("timestamp")) or ""), reverse=True)
            return out

    def get_observer_state_fingerprint(self) -> tuple[Any, ...]:
        """Return a cheap immutable digest of in-memory task state (no deep copies, excludes wall-clock TPS)."""
        with self._state_rlock:
            task_sigs: list[tuple[Any, ...]] = []
            for tid, task in sorted(self._tasks.items()):
                status = task.get("status") if isinstance(task.get("status"), dict) else {}
                meta = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
                act = meta.get("liveActivity") if isinstance(meta.get("liveActivity"), dict) else {}
                wcfg = meta.get("workerConfig") if isinstance(meta.get("workerConfig"), dict) else {}
                tok = meta.get("tokens") if isinstance(meta.get("tokens"), dict) else {}
                cur_tool = act.get("currentTool") if isinstance(act.get("currentTool"), dict) else None
                fail = meta.get("failure") if isinstance(meta.get("failure"), dict) else None
                raw_recent = act.get("recentTools") if isinstance(act.get("recentTools"), list) else ()
                recent_sig = tuple(
                    (
                        str(item.get("name") or ""),
                        str(item.get("preview") or ""),
                        str(item.get("status") or ""),
                        str(item.get("at") or ""),
                    )
                    for item in raw_recent[-_RECENT_TOOLS_LIMIT:]
                    if isinstance(item, dict)
                )
                pure_in = int(
                    tok["pureInput"]
                    if tok.get("pureInput") is not None
                    else (
                        tok["uncachedInput"]
                        if tok.get("uncachedInput") is not None
                        else (tok.get("input") or tok.get("input_tokens") or 0)
                    )
                )
                cached_in = int(
                    tok["cachedInput"]
                    if tok.get("cachedInput") is not None
                    else (tok.get("cacheRead") or tok.get("cache_read_tokens") or 0)
                )
                cache_write = int(tok.get("cacheWrite") or tok.get("cache_write_tokens") or 0)
                task_sigs.append((
                    tid,
                    status.get("state"),
                    status.get("timestamp"),
                    wcfg.get("worker_name"),
                    wcfg.get("model") or meta.get("resolvedModel") or act.get("model"),
                    wcfg.get("provider") or meta.get("resolvedProvider"),
                    int(act.get("steps") or 0),
                    int(act.get("toolCalls") or 0),
                    act.get("lastTool"),
                    cur_tool.get("name") if cur_tool else None,
                    cur_tool.get("preview") if cur_tool else None,
                    cur_tool.get("startedAt") if cur_tool else None,
                    act.get("lastTextPreview") or "",
                    recent_sig,
                    act.get("lastEventAt"),
                    act.get("hermesSessionId"),
                    meta.get("worktreePath"),
                    meta.get("worktreeBranch"),
                    pure_in,
                    cached_in,
                    cache_write,
                    int(tok.get("total") or tok.get("totalProcessed") or 0),
                    int(tok.get("output") or tok.get("output_tokens") or 0),
                    int(tok.get("contextUsed") or tok.get("context_used") or 0),
                    act.get("endedAt"),
                    act.get("lastError"),
                    fail.get("kind") if fail else None,
                    meta.get("costUsd"),
                    len(task.get("artifacts") or ()),
                ))
            return (tuple(sorted(self._killed_workers)), tuple(task_sigs))

    def is_worker_busy(self, worker_name: str) -> bool:
        wlow = str(worker_name or "").strip().lower()
        if not wlow:
            return False
        with self._state_rlock:
            for t in self._tasks.values():
                wcfg = (t.get("metadata") or {}).get("workerConfig") or {}
                if str(wcfg.get("worker_name") or "").strip().lower() != wlow:
                    continue
                st = str(((t.get("status") or {}).get("state")) or "")
                if st in (models.TaskState.WORKING.value, models.TaskState.SUBMITTED.value):
                    return True
        return False

    def find_latest_worker_task(self, worker_name: str) -> dict[str, Any] | None:
        wlow = str(worker_name or "").strip().lower()
        if not wlow:
            return None
        with self._state_rlock:
            matches = [
                t for t in self._tasks.values()
                if str((((t.get("metadata") or {}).get("workerConfig") or {}).get("worker_name") or "")).strip().lower() == wlow
            ]
            if not matches:
                return None
            matches.sort(
                key=lambda t: str(((t.get("status") or {}).get("timestamp")) or ((t.get("metadata") or {}).get("createdAt")) or ""),
                reverse=True,
            )
            return dict(matches[0])

    def get_live_worker_overlay(self, worker_name: str) -> dict[str, Any] | None:
        """Return live in-memory telemetry and streaming buffers for `worker_name` cleanly under lock."""
        wlow = str(worker_name or "").strip().lower()
        if not wlow:
            return None
        with self._state_rlock:
            matches = [
                t for t in self._tasks.values()
                if str((((t.get("metadata") or {}).get("workerConfig") or {}).get("worker_name") or "")).strip().lower() == wlow
            ]
            if not matches:
                return None

            def _sort_key(t: dict[str, Any]) -> tuple[int, str]:
                st = str(((t.get("status") or {}).get("state")) or "")
                active_rank = 1 if st in (models.TaskState.WORKING.value, models.TaskState.SUBMITTED.value) else 0
                ts = str(((t.get("status") or {}).get("timestamp")) or ((t.get("metadata") or {}).get("createdAt")) or "")
                return (active_rank, ts)

            matches.sort(key=_sort_key, reverse=True)
            task = matches[0]
            tid = str(task.get("id") or "")
            view = models.format_task_view(task, include_artifacts=False)
            self._enrich_view_telemetry_unlocked(view)
            meta = view.get("metadata") if isinstance(view.get("metadata"), dict) else {}
            act = meta.get("liveActivity") if isinstance(meta.get("liveActivity"), dict) else {}
            wcfg = meta.get("workerConfig") if isinstance(meta.get("workerConfig"), dict) else {}
            raw_state = str((view.get("status") or {}).get("state") or "")
            is_killed = wlow in self._killed_workers
            is_active = (not is_killed) and (raw_state in (models.TaskState.WORKING.value, models.TaskState.SUBMITTED.value))
            norm_state = _normalize_member_history_state(None, killed=is_killed, task_state=raw_state)

            buf = self._live_buffers.get(tid) or {}
            ctx_id = str(view.get("contextId") or "").strip()
            hermes_sid = str(
                act.get("hermesSessionId")
                or (self._context_sessions.get(ctx_id) if ctx_id else "")
                or ""
            ).strip() or None

            cur_tool = dict(act["currentTool"]) if is_active and isinstance(act.get("currentTool"), dict) else None
            recent_tools = [dict(x) for x in (act.get("recentTools") or []) if isinstance(x, dict)]
            started_at = act.get("startedAt") or meta.get("createdAt")
            elapsed_ms = max(0, int(act.get("elapsedMs") or 0))
            return {
                "taskId": tid,
                "contextId": ctx_id or None,
                "hermesSessionId": hermes_sid,
                "state": norm_state,
                "rawState": raw_state,
                "active": is_active,
                "model": wcfg.get("model"),
                "tokens": dict(meta.get("tokens") or {}),
                "tps": float(act.get("tps") or 0.0),
                "tpsEstimated": bool(act.get("tpsEstimated", False)),
                "currentTool": cur_tool,
                "recentTools": recent_tools,
                "transcript": str(buf.get("liveTranscript") or ""),
                "reasoning": str(buf.get("liveReasoning") or ""),
                "toolTrace": [dict(x) for x in (buf.get("liveToolTrace") or []) if isinstance(x, dict)],
                "startedAt": started_at,
                "endedAt": act.get("endedAt"),
                "elapsedMs": elapsed_ms,
            }

    def _resolve_resume_target(self, context_id: str) -> str | None:
        with self._state_rlock:
            if context_id in self._context_sessions:
                return self._context_sessions[context_id]
        if context_id.startswith("local-"):
            return context_id
        return None

    def _build_hermes_command(
        self,
        cfg: models.WorkerConfig,
        *,
        resume_session_id: str | None = None,
        query_file: str = "-",
        effective_workdir: str | None = None,
    ) -> tuple[list[str], str | None]:
        hermes_args: list[str] = []
        if cfg.profile and cfg.profile != "default":
            hermes_args.extend(["-p", cfg.profile])

        hermes_args.extend(["chat", "--query-file", query_file, "-Q", "--format", "stream-json"])

        if resume_session_id:
            hermes_args.extend(["--resume", resume_session_id])
        else:
            if cfg.model:
                hermes_args.extend(["-m", cfg.model])
            if cfg.provider:
                hermes_args.extend(["--provider", cfg.provider])
            if cfg.base_url:
                hermes_args.extend(["--base-url", cfg.base_url])
            if cfg.reasoning:
                hermes_args.extend(["--reasoning", cfg.reasoning])
            if cfg.toolsets:
                ts = ",".join(cfg.toolsets) if isinstance(cfg.toolsets, list) else str(cfg.toolsets)
                if ts.strip():
                    hermes_args.extend(["-t", ts.strip()])
            if cfg.skills:
                sk = ",".join(cfg.skills) if isinstance(cfg.skills, list) else str(cfg.skills)
                if sk.strip():
                    hermes_args.extend(["-s", sk.strip()])
            if cfg.max_turns is not None:
                hermes_args.extend(["--max-turns", str(int(cfg.max_turns))])
            if cfg.accept_hooks:
                hermes_args.append("--accept-hooks")
            if cfg.checkpoints:
                hermes_args.append("--checkpoints")
            if cfg.pass_session_id:
                hermes_args.append("--pass-session-id")
            if cfg.ignore_rules:
                hermes_args.append("--ignore-rules")
            if cfg.ignore_user_config:
                hermes_args.append("--ignore-user-config")
            if cfg.safe_mode:
                hermes_args.append("--safe-mode")

        raw_dir = effective_workdir or cfg.workdir
        norm_workdir = store.normalize_local_path(raw_dir) if raw_dir else None
        if norm_workdir:
            hermes_args.extend(["--in", norm_workdir])
        if cfg.source:
            hermes_args.extend(["--source", cfg.source])

        cmd = win_launcher.build_hermes_worker_argv(
            hermes_args,
            hermes_home=store.default_hermes_home(),
            hermes_exe=store.default_hermes_exe(),
        )
        return cmd, norm_workdir

    def _build_fork_seed_context(self, fork_from: str) -> str:
        """Build concise context seed when branching a new worker from an existing worker or coordinator."""
        if str(fork_from or "").strip().lower() == models.Role.COORDINATOR.value:
            chunks: list[str] = ["[FORKED CONTEXT FROM @coordinator]"]
            try:
                meta = store.read_swarm_meta(self.swarm_id, state_dir=self._state_dir) or {}
                if meta.get("goal"):
                    chunks.append(f"Swarm goal: {str(meta['goal'])[:500]}")
                board = self._board.read_board(self.swarm_id)
                plan = board.get("plan")
                if isinstance(plan, dict) and plan.get("tasks"):
                    raw_ptasks = plan["tasks"]
                    plan_lines: list[str] = []
                    if isinstance(raw_ptasks, dict):
                        for pk, pv in list(raw_ptasks.items())[:12]:
                            if isinstance(pv, dict):
                                plan_lines.append(f"- {pv.get('title', pk)} [{pv.get('state') or pv.get('status') or 'pending'}]")
                            else:
                                plan_lines.append(f"- {pk}: {pv}")
                    elif isinstance(raw_ptasks, list):
                        plan_lines = [
                            f"- {t.get('title', t.get('id', 'task'))} [{t.get('state', 'pending')}]"
                            for t in raw_ptasks[:12]
                            if isinstance(t, dict)
                        ]
                    if plan_lines:
                        chunks.append("Current plan:\n" + "\n".join(plan_lines))
                coord_posts = [
                    m for m in (board.get("messages") or [])
                    if isinstance(m, dict)
                    and not isinstance(m.get("control"), dict)
                    and str(m.get("sender") or "").lower() == models.Role.COORDINATOR.value
                ][-5:]
                if coord_posts:
                    chunks.append("Recent @coordinator posts:\n" + "\n".join(f"- #{m.get('seq')}: {str(m.get('summary') or '')[:240]}" for m in coord_posts))
            except Exception:
                pass
            return "\n".join(chunks) if len(chunks) > 1 else ""

        src_task = self.find_latest_worker_task(fork_from)
        if not src_task:
            return ""
        chunks: list[str] = [f"[FORKED CONTEXT FROM @{fork_from} (task={src_task.get('id')}, context={src_task.get('contextId')})]"]
        meta = src_task.get("metadata") or {}
        if meta.get("worktreePath"):
            chunks.append(f"Source worktree: {meta['worktreePath']} (branch {meta.get('worktreeBranch')})")
        for art in reversed(src_task.get("artifacts") or []):
            if str(art.get("name") or "").startswith("response"):
                for part in art.get("parts") or []:
                    if isinstance(part, dict) and part.get("text"):
                        chunks.append("Latest output from source worker:\n" + str(part["text"])[-3000:])
                        return "\n".join(chunks)
        live = str(meta.get("liveTranscript") or "")
        if live:
            chunks.append("Recent transcript tail:\n" + live[-2000:])
        return "\n".join(chunks)

    async def send_message(
        self,
        *,
        message_parts: list[dict[str, Any]],
        context_id: str | None = None,
        task_id: str | None = None,
        config: models.WorkerConfig | None = None,
        config_overrides: dict[str, Any] | None = None,
        return_immediately: bool = False,
        wait_seconds: float = 300.0,
        history_length: int | None = None,
        preserve_assignment: bool = False,
        defer_channel_subscribe: bool = False,
        allow_unkill: bool = False,
    ) -> dict[str, Any]:
        """Create or resume an A2A Task backed by a Tier-2 Hermes worker subprocess."""
        cur_loop = asyncio.get_running_loop()
        if self._loop is not None and not self._loop.is_closed() and self._loop.is_running() and self._loop is not cur_loop:
            return await self.run_on_loop(
                self.send_message,
                message_parts=message_parts,
                context_id=context_id,
                task_id=task_id,
                config=config,
                config_overrides=config_overrides,
                return_immediately=return_immediately,
                wait_seconds=wait_seconds,
                history_length=history_length,
                preserve_assignment=preserve_assignment,
                defer_channel_subscribe=defer_channel_subscribe,
                allow_unkill=allow_unkill,
            )
        self.bind_loop(cur_loop)
        self._ensure_async_lock(cur_loop)

        prompt_body = models.extract_text_from_parts(message_parts).strip()
        if not prompt_body:
            raise ValueError("Message parts produced an empty prompt.")

        async with self._async_lock:
            task_id_out, done_event = await self._send_message_locked(
                message_parts=message_parts,
                prompt_body=prompt_body,
                context_id=context_id,
                task_id=task_id,
                config=config,
                config_overrides=config_overrides,
                preserve_assignment=preserve_assignment,
                defer_channel_subscribe=defer_channel_subscribe,
                allow_unkill=allow_unkill,
            )

        if not return_immediately:
            try:
                await asyncio.wait_for(done_event.wait(), timeout=max(1.0, float(wait_seconds)))
            except asyncio.TimeoutError:
                pass

        return await self.get_task(task_id_out, history_length=history_length, include_artifacts=True)

    async def _send_message_locked(
        self,
        *,
        message_parts: list[dict[str, Any]],
        prompt_body: str,
        context_id: str | None = None,
        task_id: str | None = None,
        config: models.WorkerConfig | None = None,
        config_overrides: dict[str, Any] | None = None,
        preserve_assignment: bool = False,
        defer_channel_subscribe: bool = False,
        allow_unkill: bool = False,
    ) -> tuple[str, asyncio.Event]:
        """Must be called with `self._async_lock` held."""
        cfg = config or models.WorkerConfig()
        is_new_task = not bool(task_id)
        swarm_meta_doc = await asyncio.to_thread(store.read_swarm_meta, self.swarm_id, state_dir=self._state_dir) or {}

        with self._state_rlock:
            if task_id:
                existing = self._tasks.get(task_id)
                if not existing:
                    raise KeyError(f"TaskNotFoundError: task '{task_id}' does not exist.")
                cur_state = (existing.get("status") or {}).get("state")
                if cur_state in models.TERMINAL_STATES:
                    raise RuntimeError(
                        f"UnsupportedOperationError: task '{task_id}' is already {cur_state}. "
                        "Start a new task with the same contextId to continue the session."
                    )
                if cur_state not in models.INTERRUPTED_STATES:
                    raise RuntimeError(
                        f"Task '{task_id}' is currently {cur_state}; wait or cancel before sending another turn."
                    )
                resolved_context_id = context_id or existing["contextId"]
                task = existing
                prev_cfg = dict((task.get("metadata") or {}).get("workerConfig") or {})
                if config_overrides is not None:
                    ovr = {k: v for k, v in config_overrides.items() if v is not None}
                elif config is not None:
                    ovr = config.to_resume_overrides()
                else:
                    ovr = {}
                cfg = models.WorkerConfig.from_dict({**prev_cfg, **ovr})
                worker_name_cand = cfg.worker_name or f"{cfg.role}-{task_id[-6:]}"
                if self._is_worker_killed(worker_name_cand):
                    raise RuntimeError(f"Worker '{worker_name_cand}' has been killed and cannot be resumed.")
                task.setdefault("metadata", {})["workerConfig"] = cfg.to_metadata()
            else:
                resolved_context_id = context_id or models.new_id("ctx-")
                task_id = models.new_id("task-")
                worker_name_cand = cfg.worker_name or f"{cfg.role}-{task_id[-6:]}"
                if self._is_worker_killed(worker_name_cand):
                    if allow_unkill:
                        self._killed_workers.discard(worker_name_cand.strip().lower())
                    else:
                        raise RuntimeError(f"Worker '{worker_name_cand}' has been killed and cannot be messaged.")
                if cfg.workdir:
                    cfg.workdir = store.normalize_local_path(cfg.workdir)
                meta_block: dict[str, Any] = {
                    "createdAt": models.now_iso(),
                    "swarmId": self.swarm_id,
                    "workerConfig": cfg.to_metadata(),
                    "tokens": _normalize_tokens_dict(
                        {},
                        context_limit=_resolve_context_limit(cfg.model, cfg.provider, cfg.base_url),
                    ),
                    "liveActivity": {
                        "startedAt": models.now_iso(),
                        "steps": 0,
                        "toolCalls": 0,
                        "lastTool": None,
                        "currentTool": None,
                        "recentTools": [],
                        "lastTextPreview": "",
                        "lastEventAt": models.now_iso(),
                        "streamedChars": 0,
                        "elapsedMs": 0,
                        "tps": 0.0,
                        "hermesSessionId": self._resolve_resume_target(resolved_context_id),
                    },
                }
                if cfg.parent_task_id:
                    meta_block["parentTaskId"] = cfg.parent_task_id
                if cfg.parent_worker:
                    meta_block["parentWorker"] = cfg.parent_worker
                task = {
                    "id": task_id,
                    "contextId": resolved_context_id,
                    "status": {
                        "state": models.TaskState.SUBMITTED.value,
                        "timestamp": models.now_iso(),
                    },
                    "artifacts": [],
                    "history": [],
                    "metadata": meta_block,
                }
                self._tasks[task_id] = task

            with self._state_rlock:
                self._live_buffers[task_id] = {"liveTranscript": "", "liveReasoning": "", "liveToolTrace": []}

            worker_name = cfg.worker_name or f"{cfg.role}-{task_id[-6:]}"
            cfg.worker_name = worker_name
            task["metadata"]["workerConfig"] = cfg.to_metadata()

            token_file = self._token_file_provider() if self._token_file_provider else None
            swarm_conv_id = str(
                swarm_meta_doc.get("primaryConversationId")
                or swarm_meta_doc.get("coordinatorConversationId")
                or ""
            ).strip() or None
            preamble = build_worker_preamble(
                cfg,
                swarm_id=self.swarm_id,
                task_id=task_id,
                context_id=resolved_context_id,
                board_dir=self._paths().swarm_dir,
                http_url=self.http_url,
                token_file=token_file,
                conversation_id=swarm_conv_id,
            )
            fork_seed = self._build_fork_seed_context(cfg.fork_from) if (cfg.fork_from and is_new_task) else ""
            header_blocks = [b for b in (preamble, (cfg.role_prompt or "").strip(), fork_seed) if b]
            if header_blocks:
                full_prompt = f"[SYSTEM / ROLE]\n" + "\n\n".join(header_blocks) + f"\n\n[TASK]\n{prompt_body}"
            else:
                full_prompt = prompt_body

            user_msg = models.new_message(
                "ROLE_USER",
                message_parts,
                context_id=resolved_context_id,
                task_id=task_id,
            )
            task.setdefault("history", []).append(user_msg)
            task["status"] = {
                "state": models.TaskState.WORKING.value,
                "message": models.new_message(
                    "ROLE_AGENT",
                    [models.text_part("Hermes worker started.")],
                    context_id=resolved_context_id,
                    task_id=task_id,
                ),
                "timestamp": models.now_iso(),
            }
            done_event = asyncio.Event()
            self._events[task_id] = done_event
            act_cur = task.setdefault("metadata", {}).setdefault("liveActivity", {})
            if isinstance(act_cur, dict):
                act_cur.pop("endedAt", None)
                act_cur.setdefault("startedAt", models.now_iso())
                act_cur["currentTool"] = None
            if allow_unkill and not preserve_assignment and worker_name:
                self._killed_workers.discard(worker_name.strip().lower())
            self._suppress_all_finished_wake = False
            self._last_active_worker_count = self._count_active_workers_unlocked()

        existing_meta = await asyncio.to_thread(store.read_swarm_meta, self.swarm_id, state_dir=self._state_dir)
        existing_ws = (existing_meta or {}).get("workspace")
        cand_ws = _canonical_workspace_path(cfg.workdir)
        meta_ws_update: str | None = None
        if cand_ws and (not existing_ws or _is_worktree_subpath(existing_ws)):
            meta_ws_update = cand_ws
        await asyncio.to_thread(store.upsert_swarm_meta, self.swarm_id, workspace=meta_ws_update, state_dir=self._state_dir)
        await self._save_state_async()

        assignment_val: str | None = None
        if is_new_task and not preserve_assignment:
            extracted_assign = _extract_task_assignment(prompt_body) or prompt_body[:240]
            try:
                cur_board = await asyncio.to_thread(self._board.read_board, self.swarm_id)
                cur_mem = (cur_board.get("members") or {}).get(worker_name)
                cur_assign = str((cur_mem or {}).get("assignment") or "").strip() if isinstance(cur_mem, dict) else ""
                if not cur_assign:
                    assignment_val = extracted_assign
            except Exception:
                assignment_val = extracted_assign

        try:
            await asyncio.to_thread(
                self._board.upsert_member,
                worker_name,
                role=cfg.role,
                tier=2,
                runtime="hermes",
                state=models.MemberState.WORKING.value,
                assignment=assignment_val,
                channels=None if defer_channel_subscribe else cfg.channels,
                parent=cfg.parent_worker or "coordinator",
                parent_task_id=cfg.parent_task_id,
                task_id=task_id,
                context_id=resolved_context_id,
                model=cfg.model,
                provider=cfg.provider,
                workdir=cfg.workdir,
                killed=False if allow_unkill and not preserve_assignment else None,
                swarm_id=self.swarm_id,
            )
        except Exception:
            pass

        self._emit_observer_event("task_created" if is_new_task else "task_resumed", {"taskId": task_id, "contextId": resolved_context_id})

        resume_sid = self._resolve_resume_target(resolved_context_id)
        job = asyncio.create_task(self._execute_worker(task_id, full_prompt, cfg, resume_sid))
        self._bg_jobs[task_id] = job
        return task_id, done_event

    async def _execute_worker(
        self,
        task_id: str,
        full_prompt: str,
        cfg: models.WorkerConfig,
        resume_session_id: str | None,
    ) -> None:
        swarm_dir = self._paths().swarm_dir
        swarm_dir.mkdir(parents=True, exist_ok=True)
        prompt_path = swarm_dir / f"prompt-{task_id}.txt"
        worktree_path: str | None = None
        worktree_branch: str | None = None
        worker_name = cfg.worker_name or f"{cfg.role}-{task_id[-6:]}"

        try:
            if cfg.worktree:
                try:
                    wt_info = await worktree_mod.create_worktree(cfg.workdir or "", worker_name or task_id)
                    worktree_path = wt_info.worktree_path
                    worktree_branch = wt_info.branch
                    with self._state_rlock:
                        t = self._tasks.get(task_id)
                        if t:
                            t["metadata"]["worktreePath"] = worktree_path
                            t["metadata"]["worktreeBranch"] = worktree_branch
                    await asyncio.to_thread(
                        self._board.upsert_member,
                        worker_name,
                        role=cfg.role,
                        state=models.MemberState.WORKING.value,
                        task_id=task_id,
                        workdir=cfg.workdir,
                        worktree_path=worktree_path,
                        worktree_branch=worktree_branch,
                        swarm_id=self.swarm_id,
                    )
                    self._emit_observer_event("task_progress", {"taskId": task_id, "worktreePath": worktree_path})
                except Exception as exc:
                    await self._finalize_failure(task_id, f"Worktree setup failed: {exc}", [])
                    return

            await asyncio.to_thread(prompt_path.write_text, full_prompt, encoding="utf-8")
            cmd, cwd = self._build_hermes_command(
                cfg,
                resume_session_id=resume_session_id,
                query_file=str(prompt_path),
                effective_workdir=worktree_path,
            )

            env = dict(os.environ)
            env["PYTHONUTF8"] = "1"
            env["PYTHONIOENCODING"] = "utf-8"
            env["HERMES_A2A_NO_AUTO_HTTP"] = "1"
            env["HERMES_A2A_STATE_DIR"] = str(self._board.state_dir)
            env["HERMES_A2A_SWARM_ID"] = self.swarm_id
            env["HERMES_A2A_SWARM_BOARD_DIR"] = str(swarm_dir)
            env["HERMES_A2A_TASK_ID"] = task_id
            with self._state_rlock:
                context_id_val = str((self._tasks.get(task_id) or {}).get("contextId") or "")
            env["HERMES_A2A_CONTEXT_ID"] = context_id_val
            role_val = str(cfg.role or models.Role.WORKER.value)
            env["HERMES_A2A_WORKER_ROLE"] = role_val
            env["HERMES_A2A_WORKER_NAME"] = worker_name
            if self.http_url:
                env["HERMES_A2A_HTTP_URL"] = str(self.http_url)
            token_file = self._token_file_provider() if self._token_file_provider else None
            if token_file is not None:
                env["HERMES_A2A_TOKEN_FILE"] = str(token_file)
            env["HERMES_SWARM_ID"] = self.swarm_id
            env["HERMES_SWARM_AGENT"] = worker_name
            env["HERMES_SWARM_ROLE"] = role_val
            env["HERMES_SWARM_BOARD_DIR"] = str(swarm_dir)
            if self.http_url:
                env["HERMES_SWARM_HTTP_URL"] = str(self.http_url)
            if token_file is not None:
                env["HERMES_SWARM_TOKEN_FILE"] = str(token_file)
            swarm_meta_doc = await asyncio.to_thread(store.read_swarm_meta, self.swarm_id, state_dir=self._state_dir) or {}
            swarm_conv_id = str(
                swarm_meta_doc.get("primaryConversationId")
                or swarm_meta_doc.get("coordinatorConversationId")
                or ""
            ).strip()
            if swarm_conv_id:
                env["HERMES_SWARM_CONVERSATION_ID"] = swarm_conv_id
            else:
                env.pop("HERMES_SWARM_CONVERSATION_ID", None)
            if cfg.auto_approve and not resume_session_id:
                env["HERMES_YOLO_MODE"] = "1"

            text_chunks: list[str] = []
            tool_trace: list[dict[str, Any]] = []
            stderr_lines: list[str] = []
            result_event: dict[str, Any] | None = None
            hermes_session_id = resume_session_id or ""
            init_model = cfg.model or ""
            last_board_emit = 0.0
            last_db_poll = 0.0
            turn_started_mono = time.monotonic()
            turn_started_wall = time.time()
            with self._state_rlock:
                t_init = self._tasks.get(task_id)
                act_init = (t_init.get("metadata") or {}).get("liveActivity") if isinstance(t_init, dict) else {}
                prior_steps = int(act_init.get("steps") or 0) if isinstance(act_init, dict) else 0
                prior_recent = list(act_init.get("recentTools") or []) if isinstance(act_init, dict) else []
                prior_output_tokens = int(((t_init.get("metadata") or {}).get("tokens") or {}).get("output") or 0) if isinstance(t_init, dict) else 0
                if isinstance(act_init, dict):
                    act_init["streamedChars"] = 0
                    act_init["turnOutputTokens"] = 0
                    act_init["tps"] = 0.0
                    act_init["tpsEstimated"] = False
            if resume_session_id:
                prior_db_toks, _, _ = await asyncio.to_thread(
                    _query_hermes_session_stats,
                    resume_session_id,
                    model=cfg.model,
                    provider=cfg.provider,
                    base_url=cfg.base_url,
                )
                if prior_db_toks:
                    prior_output_tokens = max(prior_output_tokens, int(prior_db_toks.get("output") or 0))
            streamed_chars = 0
            steps_count = max(1, prior_steps + 1)
            saw_tool_result = False
            active_tools: dict[str, dict[str, Any]] = {}
            recent_entries: list[tuple[str, dict[str, Any]]] = [("", dict(r)) for r in prior_recent if isinstance(r, dict)][-_RECENT_TOOLS_LIMIT:]
            active_subagents: dict[str, dict[str, Any]] = {}
            subagent_start_mono: dict[str, float] = {}
            spawned_delegate_tc_ids: set[str] = set()
            llm_active_ms_acc = 0.0
            llm_span_start_mono: float | None = None
            db_poll_in_flight = False

            def _current_llm_active_ms(now_m: float) -> int:
                extra_ms = (now_m - llm_span_start_mono) * 1000.0 if llm_span_start_mono is not None else 0.0
                return max(0, int(round(llm_active_ms_acc + extra_ms)))

            def _sync_current_tool_unlocked(act: dict[str, Any]) -> None:
                if active_tools:
                    last_active = list(active_tools.values())[-1]
                    act["currentTool"] = {
                        "name": last_active["name"],
                        "preview": last_active["preview"],
                        "startedAt": last_active["startedAt"],
                    }
                else:
                    act["currentTool"] = None

            async def _refresh_live_db_stats(*, force: bool = False) -> bool:
                nonlocal last_db_poll, db_poll_in_flight, init_model
                if not hermes_session_id or db_poll_in_flight:
                    return False
                now_m = time.monotonic()
                if not force and (now_m - last_db_poll) < 1.5:
                    return False
                last_db_poll = now_m
                db_poll_in_flight = True
                try:
                    db_toks, db_cost, db_extra = await asyncio.to_thread(
                        _query_hermes_session_stats,
                        hermes_session_id,
                        model=init_model or cfg.model,
                        provider=cfg.provider,
                        base_url=cfg.base_url,
                    )
                    db_model = str((db_extra or {}).get("model") or init_model or cfg.model or "").strip() or None
                    db_prov = str((db_extra or {}).get("billingProvider") or (db_extra or {}).get("gatewayProvider") or cfg.provider or "").strip() or None
                    if db_model and not init_model:
                        init_model = db_model
                    changed = False
                    board_model_sync: tuple[str | None, str | None] | None = None
                    if db_toks or db_cost is not None or db_model or db_prov:
                        now_m = time.monotonic()
                        with self._state_rlock:
                            t = self._tasks.get(task_id)
                            if t:
                                meta = t.setdefault("metadata", {})
                                act = meta.setdefault("liveActivity", {})
                                wcfg_live = meta.setdefault("workerConfig", {})
                                el_ms = max(0, int((now_m - turn_started_mono) * 1000))
                                cur_llm_ms = _current_llm_active_ms(now_m)
                                act["elapsedMs"] = el_ms
                                act["llmActiveMs"] = cur_llm_ms
                                prev_live_model = str(wcfg_live.get("model") or meta.get("resolvedModel") or "").strip() or None
                                prev_live_prov = str(wcfg_live.get("provider") or meta.get("resolvedProvider") or "").strip() or None
                                if db_model:
                                    meta["resolvedModel"] = db_model
                                    act["model"] = db_model
                                    if not wcfg_live.get("model"):
                                        wcfg_live["model"] = db_model
                                if db_prov:
                                    meta["resolvedProvider"] = db_prov
                                    if not wcfg_live.get("provider"):
                                        wcfg_live["provider"] = db_prov
                                if (db_model and db_model != prev_live_model) or (db_prov and db_prov != prev_live_prov):
                                    board_model_sync = (db_model, db_prov)
                                    changed = True
                                if db_toks:
                                    prev_tot = (meta.get("tokens") or {}).get("total")
                                    prev_ctx = (meta.get("tokens") or {}).get("contextUsed")
                                    meta["tokens"] = db_toks
                                    turn_out = max(0, int(db_toks.get("output", 0)) - prior_output_tokens)
                                    act["turnOutputTokens"] = turn_out
                                    tps_ms = cur_llm_ms if cur_llm_ms >= 250 else el_ms
                                    if turn_out > 0:
                                        act["tps"] = _compute_tps(turn_out, tps_ms)
                                        act["tpsEstimated"] = False
                                    elif cur_llm_ms > 200 and streamed_chars > 0:
                                        act["tps"] = round(max(1, streamed_chars // 4) / (cur_llm_ms / 1000.0), 1)
                                        act["tpsEstimated"] = True
                                    else:
                                        act["tps"] = 0.0
                                        act["tpsEstimated"] = False
                                    if prev_tot != db_toks.get("total") or prev_ctx != db_toks.get("contextUsed"):
                                        act["lastEventAt"] = models.now_iso()
                                        changed = True
                                if db_cost is not None:
                                    meta["costUsd"] = db_cost
                    if board_model_sync is not None:
                        try:
                            await asyncio.to_thread(
                                self._board.upsert_member,
                                worker_name,
                                role=cfg.role,
                                model=board_model_sync[0],
                                provider=board_model_sync[1],
                                session_id=hermes_session_id or None,
                                swarm_id=self.swarm_id,
                            )
                        except Exception:
                            pass
                    running_subs = [
                        (sid, rec) for sid, rec in active_subagents.items()
                        if str(rec.get("status") or rec.get("state") or "") == "working"
                    ]
                    if running_subs:
                        child_rows = await asyncio.to_thread(_query_hermes_subagent_sessions, hermes_session_id)
                        if child_rows:
                            used_row_indices: set[int] = set()
                            updated_subs: list[dict[str, Any]] = []
                            for sid, rec in running_subs:
                                matched_idx: int | None = None
                                rec_id = str(rec.get("id") or "")
                                rec_sess = str(rec.get("sessionId") or "")
                                for r_idx, crow in enumerate(child_rows):
                                    if r_idx in used_row_indices:
                                        continue
                                    c_sub = str(crow.get("subagentId") or "")
                                    c_sess = str(crow.get("sessionId") or "")
                                    if (c_sub and (c_sub == rec_id or c_sub == sid)) or (rec_sess and c_sess == rec_sess):
                                        matched_idx = r_idx
                                        break
                                if matched_idx is None:
                                    rec_started_mono = subagent_start_mono.get(sid)
                                    target_wall = (
                                        turn_started_wall + (rec_started_mono - turn_started_mono)
                                        if rec_started_mono is not None
                                        else turn_started_wall
                                    )
                                    best_delta: float | None = None
                                    for r_idx, crow in enumerate(child_rows):
                                        if r_idx in used_row_indices or crow.get("done"):
                                            continue
                                        c_started = float(crow.get("startedAt") or 0.0)
                                        if c_started < (turn_started_wall - 2.0):
                                            continue
                                        delta = abs(c_started - target_wall)
                                        if best_delta is None or delta < best_delta:
                                            best_delta = delta
                                            matched_idx = r_idx
                                if matched_idx is None:
                                    continue
                                used_row_indices.add(matched_idx)
                                crow = child_rows[matched_idx]
                                if crow.get("sessionId"):
                                    rec["sessionId"] = crow["sessionId"]
                                rec["steps"] = max(int(rec.get("steps") or 0), int(crow.get("steps") or 0))
                                rec["toolCalls"] = max(int(rec.get("toolCalls") or 0), int(crow.get("toolCalls") or 0))
                                rec["tokens"] = dict(crow.get("tokens") or rec.get("tokens") or {"input": 0, "output": 0, "total": 0})
                                rec["elapsedMs"] = max(int(rec.get("elapsedMs") or 0), int(crow.get("elapsedMs") or 0))
                                rec["tps"] = float(crow.get("tps") or 0.0)
                                rec["updatedAt"] = models.now_iso()
                                updated_subs.append(rec)
                            if updated_subs:
                                try:
                                    await asyncio.to_thread(
                                        self._board.upsert_subagents,
                                        worker_name,
                                        updated_subs,
                                        swarm_id=self.swarm_id,
                                    )
                                    with self._state_rlock:
                                        t_sub = self._tasks.get(task_id)
                                        if t_sub:
                                            t_sub.setdefault("metadata", {}).setdefault("liveActivity", {})["lastEventAt"] = models.now_iso()
                                    changed = True
                                except Exception:
                                    pass
                    return changed
                finally:
                    db_poll_in_flight = False

            def _throttled_progress() -> None:
                nonlocal last_board_emit
                now_m = time.monotonic()
                if (now_m - last_board_emit) >= 0.4:
                    last_board_emit = now_m
                    self._emit_observer_event("task_progress", {"taskId": task_id})

            proc: asyncio.subprocess.Process | None = None
            try:
                proc = await win_launcher.spawn_hermes_subprocess(
                    cmd,
                    cwd=cwd,
                    env=env,
                    stream_limit=_SUBPROCESS_STREAM_LIMIT,
                    hidden=True,
                    new_process_group=True,
                )
                self._procs[task_id] = proc

                async def _read_stderr() -> None:
                    nonlocal hermes_session_id
                    assert proc is not None and proc.stderr is not None
                    async for raw_line, was_truncated in _iter_stream_lines(proc.stderr):
                        line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                        if was_truncated:
                            line = f"{line[:500]} ... [stderr line truncated]"
                        if not line:
                            continue
                        stderr_lines.append(line)
                        if len(stderr_lines) > 60:
                            stderr_lines.pop(0)
                        if line.startswith("Session: "):
                            sid = line.split("Session: ", 1)[1].strip()
                            if sid:
                                hermes_session_id = sid
                        with self._state_rlock:
                            t = self._tasks.get(task_id)
                            if t:
                                act = t["metadata"]["liveActivity"]
                                act["stderrTail"] = stderr_lines[-10:]
                                if hermes_session_id:
                                    act["hermesSessionId"] = hermes_session_id

                async def _read_stdout() -> None:
                    nonlocal init_model, hermes_session_id, result_event, steps_count, saw_tool_result
                    nonlocal llm_active_ms_acc, llm_span_start_mono, streamed_chars
                    assert proc is not None and proc.stdout is not None
                    async for raw_line, was_truncated in _iter_stream_lines(proc.stdout):
                        if was_truncated:
                            stderr_lines.append("[stdout line exceeded stream limit and was truncated]")
                            if len(stderr_lines) > 60:
                                stderr_lines.pop(0)
                            continue
                        line = raw_line.decode("utf-8", errors="replace").strip()
                        if not line:
                            continue
                        try:
                            ev = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(ev, dict):
                            continue

                        ev_type = ev.get("type")
                        if ev_type == "system" and ev.get("subtype") == "init":
                            ev_model = str(ev.get("model") or "").strip()
                            if ev_model:
                                init_model = ev_model
                            hermes_session_id = ev.get("session_id") or hermes_session_id
                            if llm_span_start_mono is None:
                                llm_span_start_mono = time.monotonic()
                            with self._state_rlock:
                                t = self._tasks.get(task_id)
                                if t:
                                    t_meta = t.setdefault("metadata", {})
                                    act = t_meta["liveActivity"]
                                    act["lastEventAt"] = models.now_iso()
                                    act["steps"] = max(int(act.get("steps") or 0), steps_count)
                                    if hermes_session_id:
                                        act["hermesSessionId"] = hermes_session_id
                                    if init_model:
                                        t_meta["resolvedModel"] = init_model
                                        act["model"] = init_model
                                        wcfg_live = t_meta.setdefault("workerConfig", {})
                                        if not wcfg_live.get("model"):
                                            wcfg_live["model"] = init_model
                            self._emit_observer_event("task_progress", {"taskId": task_id})

                        elif ev_type in ("thinking", "reasoning", "thought"):
                            r_delta = str(
                                ev.get("text")
                                or ev.get("thinking")
                                or ev.get("reasoning")
                                or ev.get("thought")
                                or ev.get("delta")
                                or ev.get("content")
                                or ""
                            )
                            if r_delta:
                                if llm_span_start_mono is None:
                                    llm_span_start_mono = time.monotonic()
                                if saw_tool_result:
                                    steps_count += 1
                                    saw_tool_result = False
                                    self._reset_live_step_buffers(task_id)
                                self._append_live_reasoning(task_id, r_delta)
                                streamed_chars += len(r_delta)
                                with self._state_rlock:
                                    t = self._tasks.get(task_id)
                                    if t:
                                        now_think_m = time.monotonic()
                                        cur_llm_ms = _current_llm_active_ms(now_think_m)
                                        act = t["metadata"]["liveActivity"]
                                        act["lastEventAt"] = models.now_iso()
                                        act["steps"] = max(int(act.get("steps") or 0), steps_count)
                                        act["streamedChars"] = streamed_chars
                                        act["elapsedMs"] = max(0, int((now_think_m - turn_started_mono) * 1000))
                                        act["llmActiveMs"] = cur_llm_ms
                                        if int(act.get("turnOutputTokens") or 0) == 0 and cur_llm_ms > 200:
                                            act["tps"] = round(max(1, streamed_chars // 4) / (cur_llm_ms / 1000.0), 1)
                                            act["tpsEstimated"] = True
                                _throttled_progress()

                        elif ev_type == "text":
                            delta = str(ev.get("text") or "")
                            if delta:
                                if llm_span_start_mono is None:
                                    llm_span_start_mono = time.monotonic()
                                text_chunks.append(delta)
                                if saw_tool_result:
                                    steps_count += 1
                                    saw_tool_result = False
                                    self._reset_live_step_buffers(task_id)
                                self._append_live_transcript(task_id, delta)
                                streamed_chars += len(delta)
                                with self._state_rlock:
                                    t = self._tasks.get(task_id)
                                    if t:
                                        now_text_m = time.monotonic()
                                        cur_llm_ms = _current_llm_active_ms(now_text_m)
                                        act = t["metadata"]["liveActivity"]
                                        act["lastEventAt"] = models.now_iso()
                                        act["steps"] = max(int(act.get("steps") or 0), steps_count)
                                        act["streamedChars"] = streamed_chars
                                        act["lastTextPreview"] = "".join(text_chunks)[-280:]
                                        act["elapsedMs"] = max(0, int((now_text_m - turn_started_mono) * 1000))
                                        act["llmActiveMs"] = cur_llm_ms
                                        if int(act.get("turnOutputTokens") or 0) == 0 and cur_llm_ms > 200:
                                            act["tps"] = round(max(1, streamed_chars // 4) / (cur_llm_ms / 1000.0), 1)
                                            act["tpsEstimated"] = True
                                self._publish_stream_event(
                                    task_id,
                                    {
                                        "artifactUpdate": {
                                            "taskId": task_id,
                                            "contextId": context_id_val,
                                            "artifact": models.new_artifact("response", [models.text_part(delta)]),
                                            "append": True,
                                            "lastChunk": False,
                                        }
                                    },
                                )
                                _throttled_progress()

                        elif ev_type == "tool_use":
                            now_tool_m = time.monotonic()
                            if llm_span_start_mono is not None:
                                llm_active_ms_acc += max(0.0, (now_tool_m - llm_span_start_mono) * 1000.0)
                                llm_span_start_mono = None
                            tname = str(ev.get("name") or "unknown")
                            tc_id = str(ev.get("tool_call_id") or models.new_id("tc-"))
                            tinput = ev.get("input")
                            preview = _format_tool_preview(tname, tinput)
                            now_iso = models.now_iso()
                            if saw_tool_result:
                                steps_count += 1
                                saw_tool_result = False
                                self._reset_live_step_buffers(task_id)
                            entry = {
                                "event": "tool_use",
                                "name": tname,
                                "tool_call_id": tc_id,
                                "input": _sanitize_tool_input(tinput),
                                "timestamp": ev.get("timestamp") or now_iso,
                            }
                            if len(tool_trace) < _TOOL_TRACE_LIMIT:
                                tool_trace.append(entry)
                            self._append_live_tool_trace(task_id, entry)
                            active_tools[tc_id] = {
                                "name": tname,
                                "preview": preview,
                                "startedAt": now_iso,
                                "startedMono": time.monotonic(),
                            }
                            recent_item = {
                                "name": tname,
                                "preview": preview,
                                "status": "running",
                                "durationMs": 0,
                                "at": now_iso,
                            }
                            recent_entries.append((tc_id, recent_item))
                            if len(recent_entries) > _RECENT_TOOLS_LIMIT:
                                del recent_entries[: len(recent_entries) - _RECENT_TOOLS_LIMIT]
                            with self._state_rlock:
                                t = self._tasks.get(task_id)
                                if t:
                                    act = t["metadata"]["liveActivity"]
                                    act["lastEventAt"] = now_iso
                                    act["steps"] = max(int(act.get("steps") or 0), steps_count)
                                    act["toolCalls"] = act.get("toolCalls", 0) + 1
                                    act["lastTool"] = tname
                                    act["elapsedMs"] = max(0, int((time.monotonic() - turn_started_mono) * 1000))
                                    if hermes_session_id:
                                        act["hermesSessionId"] = hermes_session_id
                                    _sync_current_tool_unlocked(act)
                                    act["recentTools"] = [dict(item) for _, item in recent_entries]
                                    self._maybe_save_progress()

                            if tname == "delegate_task":
                                deleg_action = str(tinput.get("action") or "").strip().lower() if isinstance(tinput, dict) else ""
                                is_deleg_spawn = (not deleg_action) or deleg_action in ("spawn", "run", "start", "create", "delegate")
                                if is_deleg_spawn:
                                    spawned_delegate_tc_ids.add(tc_id)
                                sub_recs: list[dict[str, Any]] = []
                                now_mono_sub = time.monotonic()
                                if is_deleg_spawn and isinstance(tinput, dict) and isinstance(tinput.get("tasks"), list):
                                    for idx, item in enumerate(tinput["tasks"]):
                                        if not isinstance(item, dict):
                                            continue
                                        sid = f"{tc_id}-{idx}"
                                        goal_str = str(item.get("goal") or item.get("task") or f"subagent-{idx + 1}")
                                        rec = models.new_subagent_record(
                                            sid,
                                            parent_worker=worker_name,
                                            parent_task_id=task_id,
                                            label=goal_str[:120],
                                            state=models.MemberState.WORKING.value,
                                            model=item.get("model") or cfg.model,
                                        )
                                        rec.update({
                                            "name": f"subagent-{idx + 1}",
                                            "task": goal_str[:180],
                                            "status": models.MemberState.WORKING.value,
                                            "steps": 0,
                                            "toolCalls": 0,
                                            "recentTools": [],
                                            "tokens": {"input": 0, "output": 0, "total": 0},
                                            "tps": 0.0,
                                            "elapsedMs": 0,
                                        })
                                        active_subagents[sid] = rec
                                        subagent_start_mono[sid] = now_mono_sub
                                        sub_recs.append(rec)
                                elif is_deleg_spawn:
                                    goal_txt = ""
                                    sub_model = cfg.model
                                    if isinstance(tinput, dict):
                                        goal_txt = str(tinput.get("goal") or tinput.get("task") or "")
                                        sub_model = tinput.get("model") or cfg.model
                                    rec = models.new_subagent_record(
                                        tc_id,
                                        parent_worker=worker_name,
                                        parent_task_id=task_id,
                                        label=(goal_txt or "delegate_task")[:120],
                                        state=models.MemberState.WORKING.value,
                                        model=sub_model,
                                    )
                                    rec.update({
                                        "name": "subagent",
                                        "task": (goal_txt or "delegate_task")[:180],
                                        "status": models.MemberState.WORKING.value,
                                        "steps": 0,
                                        "toolCalls": 0,
                                        "recentTools": [],
                                        "tokens": {"input": 0, "output": 0, "total": 0},
                                        "tps": 0.0,
                                        "elapsedMs": 0,
                                    })
                                    active_subagents[tc_id] = rec
                                    subagent_start_mono[tc_id] = now_mono_sub
                                    sub_recs.append(rec)
                                if sub_recs:
                                    try:
                                        await asyncio.to_thread(
                                            self._board.upsert_subagents,
                                            worker_name,
                                            sub_recs,
                                            swarm_id=self.swarm_id,
                                        )
                                    except Exception:
                                        pass

                            self._publish_stream_event(
                                task_id,
                                {
                                    "statusUpdate": {
                                        "taskId": task_id,
                                        "contextId": context_id_val,
                                        "status": {
                                            "state": models.TaskState.WORKING.value,
                                            "message": models.new_message(
                                                "ROLE_AGENT",
                                                [models.text_part(f"Running tool: {tname}")],
                                                context_id=context_id_val,
                                                task_id=task_id,
                                            ),
                                            "timestamp": now_iso,
                                        },
                                    }
                                },
                            )
                            last_board_emit = time.monotonic()
                            self._emit_observer_event("task_progress", {"taskId": task_id})

                        elif ev_type == "tool_result":
                            tname = str(ev.get("name") or "tool")
                            tc_id = str(ev.get("tool_call_id") or "")
                            is_err = bool(ev.get("is_error", False))
                            raw_output = str(ev.get("output") or "")
                            out_preview = raw_output[:600]
                            saw_tool_result = True

                            matched_active = active_tools.pop(tc_id, None)
                            if matched_active is None:
                                for ak, av in reversed(list(active_tools.items())):
                                    if av.get("name") == tname:
                                        matched_active = active_tools.pop(ak, None)
                                        break
                            raw_dur = ev.get("duration_ms")
                            if raw_dur is not None:
                                try:
                                    dur_ms = max(0, int(raw_dur))
                                except (TypeError, ValueError):
                                    dur_ms = 0
                            elif matched_active is not None:
                                dur_ms = max(0, int((time.monotonic() - float(matched_active["startedMono"])) * 1000))
                            else:
                                dur_ms = 0

                            entry = {
                                "event": "tool_result",
                                "name": tname,
                                "tool_call_id": tc_id,
                                "duration_ms": dur_ms,
                                "is_error": is_err,
                                "output_preview": out_preview,
                            }
                            if len(tool_trace) < _TOOL_TRACE_LIMIT:
                                tool_trace.append(entry)
                            self._append_live_tool_trace(task_id, entry)

                            matched_recent = False
                            if tc_id:
                                for r_tc, r_item in reversed(recent_entries):
                                    if r_tc == tc_id:
                                        r_item["status"] = "error" if is_err else "ok"
                                        r_item["durationMs"] = dur_ms
                                        matched_recent = True
                                        break
                            if not matched_recent:
                                for _, r_item in reversed(recent_entries):
                                    if r_item.get("name") == tname and r_item.get("status") == "running":
                                        r_item["status"] = "error" if is_err else "ok"
                                        r_item["durationMs"] = dur_ms
                                        break

                            with self._state_rlock:
                                t = self._tasks.get(task_id)
                                if t:
                                    act = t["metadata"]["liveActivity"]
                                    act["lastEventAt"] = models.now_iso()
                                    act["elapsedMs"] = max(0, int((time.monotonic() - turn_started_mono) * 1000))
                                    _sync_current_tool_unlocked(act)
                                    act["recentTools"] = [dict(item) for _, item in recent_entries]
                                    self._maybe_save_progress()

                            if tname == "delegate_task" and tc_id in spawned_delegate_tc_ids:
                                parsed_results = _parse_delegate_task_results(raw_output)
                                updated_recs: list[dict[str, Any]] = []
                                sub_state_default = models.MemberState.FAILED.value if is_err else models.MemberState.DONE.value
                                matching_subs = [
                                    (sid, rec) for sid, rec in active_subagents.items()
                                    if sid == tc_id or sid.startswith(f"{tc_id}-")
                                ]
                                for idx, (sid, rec) in enumerate(matching_subs):
                                    res_item = None
                                    if parsed_results:
                                        for cand in parsed_results:
                                            if cand.get("task_index") == idx or (
                                                cand.get("subagent_id") and cand.get("subagent_id") == rec.get("id")
                                            ):
                                                res_item = cand
                                                break
                                        if res_item is None and idx < len(parsed_results):
                                            res_item = parsed_results[idx]

                                    if isinstance(res_item, dict):
                                        r_st = str(res_item.get("status") or "").strip().lower()
                                        sub_st = (
                                            models.MemberState.FAILED.value
                                            if r_st in ("failed", "error", "timeout", "interrupted")
                                            else models.MemberState.DONE.value
                                        )
                                        sub_tok = res_item.get("tokens") if isinstance(res_item.get("tokens"), dict) else {}
                                        in_t = int(sub_tok.get("input") or sub_tok.get("input_tokens") or 0)
                                        out_t = int(sub_tok.get("output") or sub_tok.get("output_tokens") or 0)
                                        tot_t = int(sub_tok.get("total") or sub_tok.get("total_tokens") or (in_t + out_t))
                                        if res_item.get("duration_seconds") is not None:
                                            sub_el = max(0, int(round(float(res_item["duration_seconds"]) * 1000)))
                                        else:
                                            sub_el = max(0, int((time.monotonic() - subagent_start_mono.get(sid, turn_started_mono)) * 1000))
                                        raw_trace = res_item.get("tool_trace") if isinstance(res_item.get("tool_trace"), list) else []
                                        sub_recent: list[dict[str, Any]] = []
                                        for tr in raw_trace[-_SUBAGENT_RECENT_TOOLS_LIMIT:]:
                                            if not isinstance(tr, dict):
                                                continue
                                            tr_name = str(tr.get("tool") or tr.get("name") or "tool")
                                            tr_prev = _format_tool_preview(tr_name, tr.get("input_summary") or tr.get("args"))
                                            tr_st = str(tr.get("status") or "ok").lower()
                                            sub_recent.append({
                                                "name": tr_name,
                                                "preview": tr_prev,
                                                "status": "error" if tr_st in ("error", "failed") else "ok",
                                                "durationMs": int(tr.get("duration_ms") or 0),
                                                "at": models.now_iso(),
                                            })
                                        rec["state"] = sub_st
                                        rec["status"] = sub_st
                                        rec["model"] = res_item.get("model") or rec.get("model")
                                        rec["steps"] = int(res_item.get("api_calls") or rec.get("steps") or 1)
                                        rec["toolCalls"] = max(int(rec.get("toolCalls") or 0), len(raw_trace))
                                        rec["recentTools"] = sub_recent
                                        rec["tokens"] = {"input": in_t, "output": out_t, "total": tot_t}
                                        rec["elapsedMs"] = sub_el
                                        rec["tps"] = _compute_tps(out_t, sub_el)
                                        rec["summary"] = str(res_item.get("summary") or res_item.get("error") or out_preview)[:240]
                                        rec["updatedAt"] = models.now_iso()
                                    else:
                                        sub_el = max(0, dur_ms or int((time.monotonic() - subagent_start_mono.get(sid, turn_started_mono)) * 1000))
                                        rec["state"] = sub_state_default
                                        rec["status"] = sub_state_default
                                        rec["elapsedMs"] = max(int(rec.get("elapsedMs") or 0), sub_el)
                                        rec["summary"] = out_preview[:240]
                                        rec["updatedAt"] = models.now_iso()
                                    updated_recs.append(rec)

                                if not updated_recs and tc_id:
                                    fallback_rec = models.new_subagent_record(
                                        tc_id,
                                        parent_worker=worker_name,
                                        parent_task_id=task_id,
                                        label="delegate_task",
                                        state=sub_state_default,
                                        model=cfg.model,
                                        summary=out_preview[:240],
                                    )
                                    fallback_rec.update({
                                        "name": "subagent",
                                        "task": "delegate_task",
                                        "status": sub_state_default,
                                        "steps": 1,
                                        "toolCalls": 0,
                                        "recentTools": [],
                                        "tokens": {"input": 0, "output": 0, "total": 0},
                                        "tps": 0.0,
                                        "elapsedMs": dur_ms,
                                    })
                                    updated_recs.append(fallback_rec)
                                if updated_recs:
                                    try:
                                        await asyncio.to_thread(
                                            self._board.upsert_subagents,
                                            worker_name,
                                            updated_recs,
                                            swarm_id=self.swarm_id,
                                        )
                                    except Exception:
                                        pass

                            await _refresh_live_db_stats()
                            if not active_tools and llm_span_start_mono is None:
                                llm_span_start_mono = time.monotonic()
                            self._publish_stream_event(
                                task_id,
                                {
                                    "statusUpdate": {
                                        "taskId": task_id,
                                        "contextId": context_id_val,
                                        "status": {
                                            "state": models.TaskState.WORKING.value,
                                            "message": models.new_message(
                                                "ROLE_AGENT",
                                                [models.text_part(f"Tool finished: {tname}")],
                                                context_id=context_id_val,
                                                task_id=task_id,
                                                metadata={"toolResult": entry},
                                            ),
                                            "timestamp": models.now_iso(),
                                        },
                                    },
                                    "toolResult": entry,
                                },
                            )
                            last_board_emit = time.monotonic()
                            self._emit_observer_event("task_progress", {"taskId": task_id})

                        elif ev_type == "result":
                            if llm_span_start_mono is not None:
                                llm_active_ms_acc += max(0.0, (time.monotonic() - llm_span_start_mono) * 1000.0)
                                llm_span_start_mono = None
                            active_tools.clear()
                            result_event = ev
                            hermes_session_id = ev.get("session_id") or hermes_session_id

                async def _live_ticker() -> None:
                    while True:
                        await asyncio.sleep(1.5)
                        try:
                            if await _refresh_live_db_stats():
                                self._emit_observer_event("task_progress", {"taskId": task_id})
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            pass

                stderr_task = asyncio.create_task(_read_stderr())
                stdout_task = asyncio.create_task(_read_stdout())
                ticker_task = asyncio.create_task(_live_ticker())
                try:
                    if cfg.run_budget_seconds is not None and int(cfg.run_budget_seconds) > 0:
                        try:
                            await asyncio.wait_for(
                                asyncio.gather(stderr_task, stdout_task),
                                timeout=float(int(cfg.run_budget_seconds) + 15),
                            )
                            rc = await proc.wait()
                        except asyncio.TimeoutError:
                            await win_launcher.kill_process_tree(proc)
                            await self._finalize_failure(
                                task_id,
                                f"Worker exceeded run_budget_seconds={int(cfg.run_budget_seconds)}s.",
                                stderr_lines,
                            )
                            return
                    else:
                        await asyncio.gather(stderr_task, stdout_task)
                        rc = await proc.wait()
                finally:
                    for pump in (ticker_task, stderr_task, stdout_task):
                        if not pump.done():
                            pump.cancel()
                    await asyncio.gather(ticker_task, stderr_task, stdout_task, return_exceptions=True)

            except asyncio.CancelledError:
                if proc is not None and proc.returncode is None:
                    try:
                        await win_launcher.kill_process_tree(proc)
                    except Exception:
                        pass
                raise
            except Exception as exc:
                if proc is not None and proc.returncode is None:
                    try:
                        await win_launcher.kill_process_tree(proc)
                    except Exception:
                        pass
                await self._finalize_failure(task_id, f"Failed to run Hermes worker: {exc}", stderr_lines)
                return
            finally:
                self._procs.pop(task_id, None)
                try:
                    prompt_path.unlink(missing_ok=True)
                except OSError:
                    pass

            wt_diff: dict[str, str] = {}
            if worktree_path:
                wt_diff = await worktree_mod.capture_worktree_summary(worktree_path)

            raw_result_tokens: dict[str, Any] = dict((result_event or {}).get("tokens") or {})
            cost_usd: float | None = None
            for ckey in ("cost_usd", "total_cost_usd"):
                if (result_event or {}).get(ckey) is not None:
                    try:
                        cost_usd = round(float((result_event or {})[ckey]), 6)
                        break
                    except Exception:
                        pass

            db_toks: dict[str, Any] = {}
            db_cost: float | None = None
            db_extra: dict[str, Any] = {}
            if hermes_session_id:
                db_toks, db_cost, db_extra = await asyncio.to_thread(
                    _query_hermes_session_stats,
                    hermes_session_id,
                    model=init_model or cfg.model,
                    provider=cfg.provider,
                    base_url=cfg.base_url,
                )
            final_model = str(init_model or db_extra.get("model") or cfg.model or "").strip() or None
            final_provider = str(db_extra.get("billingProvider") or db_extra.get("gatewayProvider") or cfg.provider or "").strip() or None
            eff_prov_final = str(final_provider or "").lower()
            if cost_usd == 0.0 and (eff_prov_final == "custom" or db_cost is None):
                cost_usd = None
            if cost_usd is None and db_cost is not None:
                cost_usd = db_cost

            ctx_limit_final = _resolve_context_limit(final_model, final_provider, cfg.base_url)
            if raw_result_tokens and any(raw_result_tokens.values()):
                merged_tok = dict(raw_result_tokens)
                raw_had_cache_read = any(
                    raw_result_tokens.get(k) not in (None, 0, False)
                    for k in ("cacheRead", "cache_read", "cache_read_tokens")
                )
                db_supplied_cache = False
                if db_toks:
                    for fk in ("cacheRead", "cacheWrite", "reasoning", "contextUsed", "contextLimit", "contextPct"):
                        if not merged_tok.get(fk) and db_toks.get(fk):
                            merged_tok[fk] = db_toks[fk]
                            if fk == "cacheRead":
                                db_supplied_cache = True
                if (
                    not raw_had_cache_read
                    and "uncachedInput" not in merged_tok
                    and "uncached_input" not in merged_tok
                    and "uncached_input_tokens" not in merged_tok
                ):
                    raw_in_val = merged_tok.get("input") or merged_tok.get("input_tokens") or merged_tok.get("prompt_tokens")
                    if raw_in_val is not None:
                        try:
                            raw_in_int = int(raw_in_val)
                        except (TypeError, ValueError):
                            raw_in_int = 0
                        db_pure_int = int(db_toks.get("pureInput") or 0) if db_toks else 0
                        db_cache_int = int(merged_tok.get("cacheRead") or 0)
                        if db_supplied_cache and db_cache_int > 0 and raw_in_int >= (db_pure_int + db_cache_int):
                            # raw_in_val already includes cacheRead; let _normalize_tokens_dict deduct cacheRead
                            pass
                        else:
                            merged_tok["uncachedInput"] = raw_in_val
                tokens_out = _normalize_tokens_dict(merged_tok, context_limit=ctx_limit_final)
            elif db_toks:
                tokens_out = db_toks
            else:
                tokens_out = _normalize_tokens_dict({}, context_limit=ctx_limit_final)

            final_duration_ms = (result_event or {}).get("duration_ms")
            if final_duration_ms is not None:
                try:
                    final_elapsed_ms = max(0, int(final_duration_ms))
                except (TypeError, ValueError):
                    final_elapsed_ms = max(0, int((time.monotonic() - turn_started_mono) * 1000))
            else:
                final_elapsed_ms = max(0, int((time.monotonic() - turn_started_mono) * 1000))
            final_llm_ms = _current_llm_active_ms(time.monotonic())
            tps_ms = final_llm_ms if final_llm_ms >= 250 else final_elapsed_ms
            turn_output_tokens = max(0, int(tokens_out.get("output", 0)) - prior_output_tokens)
            if turn_output_tokens > 0:
                final_tps = _compute_tps(turn_output_tokens, tps_ms)
                final_tps_estimated = False
            elif streamed_chars > 0 and tps_ms > 200:
                final_tps = round(max(1, streamed_chars // 4) / (tps_ms / 1000.0), 1)
                final_tps_estimated = True
            else:
                final_tps = 0.0
                final_tps_estimated = False

            async with self._async_lock:
                with self._state_rlock:
                    task = self._tasks.get(task_id)
                    if not task:
                        return
                    if (
                        (task.get("status") or {}).get("state") == models.TaskState.CANCELED.value
                        or self._is_worker_killed(worker_name)
                    ):
                        kill_ts = models.now_iso()
                        if (task.get("status") or {}).get("state") not in models.TERMINAL_STATES:
                            task["status"] = {
                                "state": models.TaskState.CANCELED.value,
                                "message": models.new_message(
                                    "ROLE_AGENT",
                                    [models.text_part("Killed by coordinator.")],
                                    context_id=task["contextId"],
                                    task_id=task_id,
                                ),
                                "timestamp": kill_ts,
                            }
                        act_k = (task.get("metadata") or {}).get("liveActivity")
                        if isinstance(act_k, dict):
                            act_k["currentTool"] = None
                            act_k.setdefault("endedAt", kill_ts)
                            act_k["recentTools"] = [
                                {**r, "status": "error"} if isinstance(r, dict) and r.get("status") == "running" else r
                                for r in (act_k.get("recentTools") or [])
                            ]
                        self._last_active_worker_count = self._count_active_workers_unlocked()
                        ev = self._events.pop(task_id, None)
                        if ev:
                            ev.set()
                        self._publish_stream_event(task_id, None)
                        self._emit_observer_event("task_updated", {"taskId": task_id, "state": models.TaskState.CANCELED.value})
                        return

                    final_text = ""
                    if result_event and result_event.get("text"):
                        final_text = str(result_event["text"])
                    elif text_chunks:
                        final_text = "".join(text_chunks)

                    result_rc = int((result_event or {}).get("exit_code", rc or 0))
                    err_msg = str((result_event or {}).get("error") or "")

                    if hermes_session_id:
                        self._context_sessions[task["contextId"]] = hermes_session_id

                    has_input_req = any(ln.lstrip().startswith("[INPUT_REQUIRED]") for ln in final_text.splitlines())
                    if result_rc == 3 or has_input_req:
                        state = models.TaskState.INPUT_REQUIRED.value
                        prompt_msg = final_text or err_msg or "Hermes worker requested additional input."
                    elif result_rc == 0 and not err_msg:
                        state = models.TaskState.COMPLETED.value
                        prompt_msg = "Task completed."
                    else:
                        state = models.TaskState.FAILED.value
                        real_errs = [l for l in stderr_lines if not l.startswith(("Session: ", "session_id: "))]
                        prompt_msg = err_msg or "\n".join(real_errs[-8:]) or f"Hermes exited with code {result_rc}."

                    turn_num = max(
                        1,
                        sum(1 for m in (task.get("history") or []) if isinstance(m, dict) and m.get("role") == "ROLE_USER"),
                    )

                    preserved: list[dict[str, Any]] = []
                    for prev_art in task.get("artifacts") or []:
                        if not isinstance(prev_art, dict):
                            continue
                        art_copy = dict(prev_art)
                        aname = str(art_copy.get("name") or "")
                        if aname in ("response", "tool_trace", "usage_and_session", "worktree_diff"):
                            prev_meta = dict(art_copy.get("metadata") or {})
                            prev_turn = prev_meta.get("turn") or max(1, turn_num - 1)
                            prev_meta.setdefault("originalName", aname)
                            prev_meta.setdefault("turn", prev_turn)
                            art_copy["metadata"] = prev_meta
                            art_copy["name"] = f"{aname}_turn_{prev_turn}"
                        preserved.append(art_copy)

                    new_arts: list[dict[str, Any]] = []
                    if final_text:
                        new_arts.append(
                            models.new_artifact(
                                "response",
                                [models.text_part(final_text)],
                                description="Final text output from Hermes worker",
                                metadata={"turn": turn_num},
                            )
                        )
                    if tool_trace:
                        new_arts.append(
                            models.new_artifact(
                                "tool_trace",
                                [models.data_part({"toolCalls": len([x for x in tool_trace if x.get("event") == "tool_use"]), "events": tool_trace})],
                                description="Structured trace of tools invoked by the Hermes worker",
                                metadata={"turn": turn_num},
                            )
                        )
                    usage_payload = {
                        "hermesSessionId": hermes_session_id or None,
                        "model": final_model,
                        "provider": final_provider,
                        "exitCode": result_rc,
                        "tokens": tokens_out,
                        "costUsd": cost_usd,
                        "durationMs": (result_event or {}).get("duration_ms"),
                        "workdir": cfg.workdir,
                        "worktree": cfg.worktree,
                        "worktreePath": worktree_path,
                        "worktreeBranch": worktree_branch,
                        "worktreeDiffStat": wt_diff.get("diffStat") if worktree_path else None,
                        "turn": turn_num,
                    }
                    if final_model:
                        task["metadata"]["resolvedModel"] = final_model
                        wcfg_final = task["metadata"].setdefault("workerConfig", {})
                        if not wcfg_final.get("model"):
                            wcfg_final["model"] = final_model
                    if final_provider:
                        task["metadata"]["resolvedProvider"] = final_provider
                        wcfg_final = task["metadata"].setdefault("workerConfig", {})
                        if not wcfg_final.get("provider"):
                            wcfg_final["provider"] = final_provider
                    if worktree_path and (wt_diff.get("status") or wt_diff.get("diffStat")):
                        new_arts.append(
                            models.new_artifact(
                                "worktree_diff",
                                [models.data_part({"worktreePath": worktree_path, "worktreeBranch": worktree_branch, **wt_diff})],
                                description="Git status and diff stat in isolated worker worktree",
                                metadata={"turn": turn_num},
                            )
                        )
                    new_arts.append(
                        models.new_artifact(
                            "usage_and_session",
                            [models.data_part(usage_payload)],
                            description="Token usage, timing, and session/worktree metadata",
                            metadata={"turn": turn_num},
                        )
                    )

                    task["artifacts"] = [*preserved, *new_arts]
                    task.setdefault("history", []).append(
                        models.new_message(
                            "ROLE_AGENT",
                            [models.text_part(final_text if final_text else prompt_msg)],
                            context_id=task["contextId"],
                            task_id=task_id,
                        )
                    )
                    done_ts = models.now_iso()
                    task["status"] = {
                        "state": state,
                        "message": models.new_message(
                            "ROLE_AGENT",
                            [models.text_part(prompt_msg if state != models.TaskState.COMPLETED.value else "Completed")],
                            context_id=task["contextId"],
                            task_id=task_id,
                        ),
                        "timestamp": done_ts,
                    }
                    act = task["metadata"]["liveActivity"]
                    act["hermesSessionId"] = hermes_session_id or act.get("hermesSessionId")
                    act["currentTool"] = None
                    act["steps"] = max(int(act.get("steps") or 0), steps_count)
                    act["elapsedMs"] = final_elapsed_ms
                    act["llmActiveMs"] = final_llm_ms
                    act["turnOutputTokens"] = turn_output_tokens
                    act["tps"] = final_tps
                    act["tpsEstimated"] = final_tps_estimated
                    final_status_str = "error" if state == models.TaskState.FAILED.value else "ok"
                    final_recent: list[dict[str, Any]] = []
                    for _, r_item in recent_entries[-_RECENT_TOOLS_LIMIT:]:
                        rcopy = dict(r_item)
                        if rcopy.get("status") == "running":
                            rcopy["status"] = final_status_str
                        final_recent.append(rcopy)
                    act["recentTools"] = final_recent
                    if state in models.TERMINAL_STATES:
                        act["endedAt"] = done_ts
                    if state == models.TaskState.FAILED.value:
                        err_summary = (err_msg or prompt_msg or "")[:300]
                        act["lastError"] = err_summary
                        is_quota = bool(QUOTA_RE.search(err_summary) or any(QUOTA_RE.search(s) for s in stderr_lines[-10:]))
                        task["metadata"]["failure"] = {
                            "kind": "quota" if is_quota else "error",
                            "text": err_summary,
                            "since": done_ts,
                        }
                    task["metadata"]["tokens"] = tokens_out
                    if cost_usd is not None:
                        task["metadata"]["costUsd"] = cost_usd
                    if worktree_path:
                        task["metadata"]["worktreePath"] = worktree_path
                        task["metadata"]["worktreeBranch"] = worktree_branch
                        if wt_diff.get("diffStat"):
                            task["metadata"]["worktreeDiffStat"] = wt_diff["diffStat"]
                    if stderr_lines and state == models.TaskState.FAILED.value:
                        task["metadata"]["stderrTail"] = stderr_lines[-10:]

                await self._save_state_async()
                self._publish_stream_event(
                    task_id,
                    {
                        "statusUpdate": {
                            "taskId": task_id,
                            "contextId": task["contextId"],
                            "status": task["status"],
                        }
                    },
                )
                self._publish_stream_event(task_id, None)
                done_ev = self._events.pop(task_id, None) if state in models.TERMINAL_STATES else self._events.get(task_id)

            try:
                roster_state = (
                    models.MemberState.INPUT_REQUIRED.value
                    if state == models.TaskState.INPUT_REQUIRED.value
                    else (models.MemberState.WAITING.value if state == models.TaskState.COMPLETED.value else models.MemberState.FAILED.value)
                )
                try:
                    await asyncio.to_thread(
                        self._board.upsert_member,
                        worker_name,
                        role=cfg.role,
                        state=roster_state,
                        task_id=task_id,
                        context_id=task["contextId"],
                        session_id=hermes_session_id or None,
                        model=final_model,
                        provider=final_provider,
                        workdir=cfg.workdir,
                        worktree_path=worktree_path,
                        worktree_branch=worktree_branch,
                        swarm_id=self.swarm_id,
                    )
                    if (
                        str(cfg.role or "").strip().lower() == models.Role.NARRATOR.value
                        or worker_name.lower().startswith("narrator")
                    ) and state in (models.TaskState.COMPLETED.value, models.TaskState.INPUT_REQUIRED.value):
                        cur_board_after = await asyncio.to_thread(self._board.read_board, self.swarm_id)
                        cur_head = self._board.head_seq(cur_board_after)
                        if cur_head > 0:
                            await asyncio.to_thread(self._board.mark_member_read, worker_name, cur_head, swarm_id=self.swarm_id)
                except Exception:
                    pass

                # Replay queued @mentions that arrived while this worker was busy.
                if state != models.TaskState.CANCELED.value and not self._is_worker_killed(worker_name):
                    try:
                        await self._router.flush_pending_for_worker(self.swarm_id, worker_name)
                    except Exception:
                        pass

                self._emit_observer_event("task_updated", {"taskId": task_id, "state": state, "worker": worker_name})
                self._emit_task_wake_if_needed(
                    task_id=task_id,
                    worker_name=worker_name,
                    worker_role=str(cfg.role or models.Role.WORKER.value),
                    state=state,
                    error_text=err_msg or (prompt_msg if state == models.TaskState.FAILED.value else ""),
                )
            finally:
                if done_ev:
                    done_ev.set()
        finally:
            self._bg_jobs.pop(task_id, None)

    def _emit_task_wake_if_needed(
        self,
        *,
        task_id: str,
        worker_name: str,
        worker_role: str,
        state: str,
        error_text: str = "",
    ) -> None:
        role_low = str(worker_role or models.Role.WORKER.value).strip().lower()
        if role_low == models.Role.SUBAGENT.value:
            return
        wlow = str(worker_name or "").strip().lower()
        is_killed_board = self._is_member_killed_on_board(worker_name)
        is_canceled = state == models.TaskState.CANCELED.value
        with self._state_rlock:
            is_killed = is_killed_board or (bool(wlow) and wlow in self._killed_workers)
            cur_active = self._count_active_workers_unlocked()
            dropped_to_zero = (
                (not is_killed)
                and (not is_canceled)
                and (not self._suppress_all_finished_wake)
                and self._last_active_worker_count > 0
                and cur_active == 0
            )
            self._last_active_worker_count = cur_active
            if cur_active == 0:
                self._suppress_all_finished_wake = False

        if is_killed or is_canceled:
            return

        is_attention_state = state in (models.TaskState.FAILED.value, models.TaskState.INPUT_REQUIRED.value)
        if not is_attention_state and not dropped_to_zero:
            return

        is_quota = bool(
            state == models.TaskState.FAILED.value
            and QUOTA_RE.search(error_text or "")
        )
        if state == models.TaskState.FAILED.value:
            kind = models.MessageType.ESCALATION.value
            detail = f": {error_text.strip()[:140]}" if error_text.strip() else ""
            summary = f"@{worker_name} entered {state} ({task_id}){detail}"
        elif state == models.TaskState.INPUT_REQUIRED.value:
            kind = models.MessageType.QUESTION.value
            summary = f"@{worker_name} paused in {state} ({task_id})"
        else:
            kind = models.MessageType.POST.value
            summary = f"All active Tier-2 workers in swarm '{self.swarm_id}' finished (latest: @{worker_name} -> {state})."

        payload: dict[str, Any] = {
            "swarmId": self.swarm_id,
            "synthetic": True,
            "seq": 0,
            "worker": worker_name,
            "workerRole": role_low,
            "state": state,
            "kind": kind,
            "summary": summary,
            "task": {
                "taskId": task_id,
                "workerName": worker_name,
                "state": state,
                "activeWorkerCount": cur_active,
            },
        }
        if is_quota:
            payload["reason"] = "quota"
        self._emit_observer_event("coordinator_wake", payload)

    async def _finalize_failure(self, task_id: str, error_text: str, stderr_lines: list[str]) -> None:
        worker_name = ""
        ctx_id: str | None = None
        wcfg: dict[str, Any] = {}
        async with self._async_lock:
            with self._state_rlock:
                task = self._tasks.get(task_id)
                if not task:
                    return
                wcfg = dict((task.get("metadata") or {}).get("workerConfig") or {})
                ctx_id = task.get("contextId")
                worker_name = str(wcfg.get("worker_name") or "")
                if (
                    (task.get("status") or {}).get("state") == models.TaskState.CANCELED.value
                    or self._is_worker_killed(worker_name)
                ):
                    act_k = (task.get("metadata") or {}).get("liveActivity")
                    if isinstance(act_k, dict):
                        act_k["currentTool"] = None
                        act_k.setdefault("endedAt", models.now_iso())
                        act_k["recentTools"] = [
                            {**r, "status": "error"} if isinstance(r, dict) and r.get("status") == "running" else r
                            for r in (act_k.get("recentTools") or [])
                        ]
                    self._last_active_worker_count = self._count_active_workers_unlocked()
                    ev = self._events.pop(task_id, None)
                    if ev:
                        ev.set()
                    self._publish_stream_event(task_id, None)
                    return

                done_ts = models.now_iso()
                task["status"] = {
                    "state": models.TaskState.FAILED.value,
                    "message": models.new_message(
                        "ROLE_AGENT",
                        [models.text_part(error_text)],
                        context_id=task["contextId"],
                        task_id=task_id,
                    ),
                    "timestamp": done_ts,
                }
                if stderr_lines:
                    task["metadata"]["stderrTail"] = stderr_lines[-10:]
                is_quota_fail = bool(QUOTA_RE.search(error_text or "") or any(QUOTA_RE.search(s) for s in stderr_lines[-10:]))
                task["metadata"]["failure"] = {
                    "kind": "quota" if is_quota_fail else "error",
                    "text": error_text[:300],
                    "since": done_ts,
                }
                act = (task.get("metadata") or {}).get("liveActivity")
                if isinstance(act, dict):
                    act["lastError"] = error_text[:300]
                    act["endedAt"] = done_ts
                    act["currentTool"] = None
                    act["elapsedMs"] = _elapsed_ms(act.get("startedAt") or (task.get("metadata") or {}).get("createdAt"), ended_at=done_ts)
                    recent = []
                    for r_item in list(act.get("recentTools") or []):
                        if isinstance(r_item, dict):
                            rcopy = dict(r_item)
                            if rcopy.get("status") == "running":
                                rcopy["status"] = "error"
                            recent.append(rcopy)
                    act["recentTools"] = recent[-_RECENT_TOOLS_LIMIT:]

            await self._save_state_async()
            self._publish_stream_event(task_id, None)
            done_ev = self._events.pop(task_id, None)

        try:
            if worker_name:
                if not self._is_worker_killed(worker_name):
                    try:
                        await asyncio.to_thread(
                            self._board.upsert_member,
                            worker_name,
                            role=str(wcfg.get("role") or models.Role.WORKER.value),
                            state=models.MemberState.FAILED.value,
                            task_id=task_id,
                            context_id=ctx_id,
                            model=wcfg.get("model"),
                            workdir=wcfg.get("workdir"),
                            swarm_id=self.swarm_id,
                        )
                    except Exception:
                        pass
                    try:
                        await self._router.flush_pending_for_worker(self.swarm_id, worker_name)
                    except Exception:
                        pass

            self._emit_observer_event("task_updated", {"taskId": task_id, "state": models.TaskState.FAILED.value})
            self._emit_task_wake_if_needed(
                task_id=task_id,
                worker_name=worker_name or task_id,
                worker_role=str(wcfg.get("role") or models.Role.WORKER.value),
                state=models.TaskState.FAILED.value,
                error_text=error_text,
            )
        finally:
            if done_ev:
                done_ev.set()

    async def get_task(
        self,
        task_id: str,
        *,
        history_length: int | None = None,
        include_artifacts: bool = True,
        wait_seconds: float = 0.0,
    ) -> dict[str, Any]:
        cur_loop = asyncio.get_running_loop()
        if self._loop is not None and not self._loop.is_closed() and self._loop.is_running() and self._loop is not cur_loop:
            return await self.run_on_loop(
                self.get_task,
                task_id,
                history_length=history_length,
                include_artifacts=include_artifacts,
                wait_seconds=wait_seconds,
            )
        self.bind_loop(cur_loop)
        self._ensure_async_lock(cur_loop)

        if wait_seconds and wait_seconds > 0:
            ev = self._events.get(task_id)
            if ev and not ev.is_set():
                try:
                    await asyncio.wait_for(ev.wait(), timeout=float(wait_seconds))
                except asyncio.TimeoutError:
                    pass

        with self._state_rlock:
            task = self._tasks.get(task_id)
            if not task:
                raise KeyError(f"TaskNotFoundError: task '{task_id}' does not exist.")
            view = models.format_task_view(task, history_length=history_length, include_artifacts=include_artifacts)
            buf = self._live_buffers.get(task_id)
            meta = view.setdefault("metadata", {})
            if buf:
                meta["liveTranscript"] = str(buf.get("liveTranscript") or "")
                meta["liveReasoning"] = str(buf.get("liveReasoning") or "")
                meta["liveToolTrace"] = [dict(x) for x in (buf.get("liveToolTrace") or [])]
            self._enrich_view_telemetry_unlocked(view)
            return view

    async def list_tasks(
        self,
        *,
        context_id: str | None = None,
        state: str | None = None,
        include_artifacts: bool = False,
        history_length: int | None = None,
        page_size: int = 50,
        page_token: str = "",
        include_live: bool = False,
    ) -> dict[str, Any]:
        with self._state_rlock:
            items = list(self._tasks.values())
            if context_id:
                items = [t for t in items if t.get("contextId") == context_id]
            if state:
                items = [t for t in items if ((t.get("status") or {}).get("state") == state)]
            items.sort(key=lambda t: ((t.get("status") or {}).get("timestamp") or ""), reverse=True)
            total_size = len(items)
            offset = int(page_token) if page_token.isdigit() else 0
            limit = max(1, min(100, int(page_size)))
            page = items[offset : offset + limit]
            next_token = str(offset + limit) if (offset + limit) < total_size else ""
            views: list[dict[str, Any]] = []
            for t in page:
                v = models.format_task_view(t, history_length=history_length, include_artifacts=include_artifacts)
                if include_live or ((v.get("status") or {}).get("state") == models.TaskState.WORKING.value):
                    buf = self._live_buffers.get(str(v.get("id") or ""))
                    if buf:
                        v.setdefault("metadata", {})["liveTranscript"] = str(buf.get("liveTranscript") or "")
                        v.setdefault("metadata", {})["liveReasoning"] = str(buf.get("liveReasoning") or "")
                        v.setdefault("metadata", {})["liveToolTrace"] = [dict(x) for x in (buf.get("liveToolTrace") or [])]
                self._enrich_view_telemetry_unlocked(v)
                views.append(v)
            return {
                "tasks": views,
                "nextPageToken": next_token,
                "pageSize": limit,
                "totalSize": total_size,
            }

    async def cancel_task(self, task_id: str, reason: str = "Canceled by coordinator.") -> dict[str, Any]:
        cur_loop = asyncio.get_running_loop()
        if self._loop is not None and not self._loop.is_closed() and self._loop.is_running() and self._loop is not cur_loop:
            return await self.run_on_loop(self.cancel_task, task_id, reason=reason)
        self.bind_loop(cur_loop)
        self._ensure_async_lock(cur_loop)

        async with self._async_lock:
            with self._state_rlock:
                task = self._tasks.get(task_id)
                if not task:
                    raise KeyError(f"TaskNotFoundError: task '{task_id}' does not exist.")
                cur_state = (task.get("status") or {}).get("state")
                if cur_state in models.TERMINAL_STATES:
                    raise RuntimeError(f"TaskNotCancelableError: task '{task_id}' is already {cur_state}.")

                done_ts = models.now_iso()
                task["status"] = {
                    "state": models.TaskState.CANCELED.value,
                    "message": models.new_message(
                        "ROLE_AGENT",
                        [models.text_part(reason)],
                        context_id=task["contextId"],
                        task_id=task_id,
                    ),
                    "timestamp": done_ts,
                }
                act = (task.get("metadata") or {}).get("liveActivity")
                if isinstance(act, dict):
                    act["endedAt"] = done_ts
                    act["currentTool"] = None
                    act["elapsedMs"] = _elapsed_ms(act.get("startedAt") or (task.get("metadata") or {}).get("createdAt"), ended_at=done_ts)
                    recent = []
                    for r_item in list(act.get("recentTools") or []):
                        if isinstance(r_item, dict):
                            rcopy = dict(r_item)
                            if rcopy.get("status") == "running":
                                rcopy["status"] = "error"
                            recent.append(rcopy)
                    act["recentTools"] = recent[-_RECENT_TOOLS_LIMIT:]
                proc = self._procs.get(task_id)
                wcfg = dict((task.get("metadata") or {}).get("workerConfig") or {})
                ctx_id = task.get("contextId")
                hermes_sid = str((act.get("hermesSessionId") if isinstance(act, dict) else "") or self._context_sessions.get(ctx_id) or "").strip()
                self._last_active_worker_count = self._count_active_workers_unlocked()
            await self._save_state_async()

        if hermes_sid:
            try:
                await win_launcher.interrupt_hermes_gateway_session(hermes_sid)
            except Exception:
                pass

        if proc:
            await win_launcher.kill_process_tree(proc)

        self._publish_stream_event(task_id, None)
        ev = self._events.pop(task_id, None)
        if ev:
            ev.set()

        wname = str(wcfg.get("worker_name") or "")
        if wname:
            try:
                await asyncio.to_thread(
                    self._board.upsert_member,
                    wname,
                    role=str(wcfg.get("role") or models.Role.WORKER.value),
                    state=models.MemberState.CANCELED.value,
                    task_id=task_id,
                    context_id=ctx_id,
                    swarm_id=self.swarm_id,
                )
            except Exception:
                pass

        self._emit_observer_event("task_updated", {"taskId": task_id, "state": models.TaskState.CANCELED.value})
        return await self.get_task(task_id, include_artifacts=True)

    async def spawn_workers(
        self,
        *,
        name: str,
        prompt: str,
        role: str = models.Role.WORKER.value,
        tasks: list[str] | None = None,
        n: int | None = None,
        fork_from: str | None = None,
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
    ) -> dict[str, Any]:
        """Spawn a single worker or a sharded group (`<name>-1..k`) cleanly."""
        clean_base = store.sanitize_swarm_id(name)
        if not clean_base or clean_base in (models.Role.COORDINATOR.value, models.Role.PRIMARY.value, "all"):
            raise ValueError(f"Invalid worker name: {name!r}")
        validated_role = models.validate_worker_role(role)

        specs: list[tuple[str, str]] = []
        if tasks and len(tasks) > 0:
            for idx, slice_item in enumerate(tasks, start=1):
                wname = f"{clean_base}-{idx}"
                wprompt = f"{prompt.strip()}\n\n[ASSIGNED SLICE {idx}/{len(tasks)}]\n{str(slice_item).strip()}"
                specs.append((wname, wprompt))
        elif n is not None and int(n) > 1:
            count = int(n)
            for idx in range(1, count + 1):
                wname = f"{clean_base}-{idx}"
                wprompt = f"{prompt.strip()}\n\n[SHARD INSTANCE {idx}/{count}]"
                specs.append((wname, wprompt))
        else:
            specs.append((clean_base, prompt.strip()))

        async def _launch_one(wname: str, wprompt: str) -> dict[str, Any]:
            wcfg = models.WorkerConfig(
                model=model,
                provider=provider,
                base_url=base_url,
                profile=profile or "default",
                reasoning=reasoning,
                workdir=workdir,
                worktree=bool(worktree),
                toolsets=toolsets,
                skills=skills,
                max_turns=max_turns,
                run_budget_seconds=run_budget_seconds,
                role=validated_role,
                role_prompt=role_prompt,
                worker_name=wname,
                channels=channels or [models.DEFAULT_CHANNEL],
                parent_worker=fork_from or models.Role.COORDINATOR.value,
                fork_from=fork_from,
            )
            return await self.send_message(
                message_parts=[models.text_part(wprompt)],
                config=wcfg,
                return_immediately=return_immediately,
                wait_seconds=wait_seconds,
                defer_channel_subscribe=bool(channels),
                allow_unkill=True,
            )

        launched = await asyncio.gather(*(_launch_one(wn, wp) for wn, wp in specs))
        if channels:
            worker_names = [wn for wn, _ in specs]
            for raw_ch in channels:
                norm_ch = models.normalize_channel_name(raw_ch)
                if norm_ch not in (models.DEFAULT_CHANNEL, "*"):
                    try:
                        await asyncio.to_thread(
                            self._board.channel_action,
                            "subscribe",
                            channel=norm_ch,
                            members=worker_names,
                            actor=models.Role.COORDINATOR.value,
                            swarm_id=self.swarm_id,
                        )
                    except Exception:
                        pass
        return {
            "swarmId": self.swarm_id,
            "count": len(launched),
            "workers": [
                {
                    "name": wn,
                    "role": validated_role,
                    "taskId": t.get("id"),
                    "contextId": t.get("contextId"),
                    "state": (t.get("status") or {}).get("state"),
                    "worktreePath": (t.get("metadata") or {}).get("worktreePath"),
                }
                for (wn, _), t in zip(specs, launched)
            ],
            "tasks": launched,
        }

    def _is_worker_killed(self, worker_name: str) -> bool:
        wlow = str(worker_name or "").strip().lower()
        if wlow:
            with self._state_rlock:
                if wlow in self._killed_workers:
                    return True
        return self._is_member_killed_on_board(worker_name)

    def _is_member_killed_on_board(self, worker_name: str) -> bool:
        try:
            board = self._board.read_board(self.swarm_id)
            members = board.get("members") if isinstance(board.get("members"), dict) else {}
            mem = members.get(worker_name)
            if not isinstance(mem, dict):
                wlow = str(worker_name or "").strip().lower()
                for mname, mrec in members.items():
                    if mname.lower() == wlow and isinstance(mrec, dict):
                        mem = mrec
                        break
            if isinstance(mem, dict):
                return bool(mem.get("killed")) or str(mem.get("state") or "").strip().lower() == models.MemberState.KILLED.value
        except Exception:
            pass
        return False

    def _find_worker_tasks(self, worker_name: str) -> list[dict[str, Any]]:
        wlow = str(worker_name or "").strip().lower()
        if not wlow:
            return []
        with self._state_rlock:
            matches = [
                t for t in self._tasks.values()
                if str((((t.get("metadata") or {}).get("workerConfig") or {}).get("worker_name") or "")).strip().lower() == wlow
            ]
            matches.sort(
                key=lambda t: str(((t.get("status") or {}).get("timestamp")) or ((t.get("metadata") or {}).get("createdAt")) or ""),
                reverse=True,
            )
            return [dict(m) for m in matches]

    async def resume_worker(
        self,
        name: str,
        prompt: str,
        *,
        return_immediately: bool = True,
        wait_seconds: float = 300.0,
    ) -> dict[str, Any]:
        """Resume an idle/interrupted worker preserving its `contextId` and worktree."""
        cur_loop = asyncio.get_running_loop()
        if self._loop is not None and not self._loop.is_closed() and self._loop.is_running() and self._loop is not cur_loop:
            return await self.run_on_loop(
                self.resume_worker,
                name,
                prompt,
                return_immediately=return_immediately,
                wait_seconds=wait_seconds,
            )
        self.bind_loop(cur_loop)
        self._ensure_async_lock(cur_loop)

        parts = [models.text_part(prompt)]
        prompt_body = models.extract_text_from_parts(parts).strip()
        if not prompt_body:
            raise ValueError("Message parts produced an empty prompt.")

        async with self._async_lock:
            if self._is_worker_killed(name):
                raise RuntimeError(f"Worker '{name}' has been killed and cannot be resumed.")
            latest = self.find_latest_worker_task(name)
            if not latest:
                raise KeyError(f"Worker '{name}' not found in swarm '{self.swarm_id}'.")
            cur_state = ((latest.get("status") or {}).get("state")) or ""
            if cur_state in (models.TaskState.WORKING.value, models.TaskState.SUBMITTED.value):
                raise RuntimeError(f"Worker '{name}' is already active ({cur_state}).")

            meta = latest.get("metadata") or {}
            prev_cfg = dict(meta.get("workerConfig") or {})
            wt_path = str(meta.get("worktreePath") or "").strip()
            if wt_path and Path(wt_path).is_dir():
                prev_cfg["workdir"] = wt_path
                prev_cfg["worktree"] = False
            wcfg = models.WorkerConfig.from_dict(prev_cfg)
            resume_tid = latest["id"] if cur_state in models.INTERRUPTED_STATES else None
            task_id_out, done_event = await self._send_message_locked(
                message_parts=parts,
                prompt_body=prompt_body,
                context_id=latest.get("contextId"),
                task_id=resume_tid,
                config=wcfg,
                preserve_assignment=True,
                allow_unkill=False,
            )

        if not return_immediately:
            try:
                await asyncio.wait_for(done_event.wait(), timeout=max(1.0, float(wait_seconds)))
            except asyncio.TimeoutError:
                pass
        return await self.get_task(task_id_out, include_artifacts=True)

    async def switch_model(
        self,
        name: str,
        *,
        model: str | None = None,
        provider: str | None = None,
        reasoning: str | None = None,
        prompt: str | None = None,
        return_immediately: bool = True,
        wait_seconds: float = 300.0,
    ) -> dict[str, Any]:
        """Switch a waiting worker's (or coordinator's) model/provider/reasoning while preserving its A2A `contextId`."""
        clean_name = str(name or "").strip().lstrip("@")
        if clean_name.lower() == models.Role.COORDINATOR.value:
            await asyncio.to_thread(
                self._board.upsert_member,
                models.Role.COORDINATOR.value,
                role=models.Role.COORDINATOR.value,
                model=model,
                provider=provider,
                swarm_id=self.swarm_id,
            )
            return {
                "switched": True,
                "worker": models.Role.COORDINATOR.value,
                "model": model,
                "provider": provider,
                "reasoning": reasoning,
                "note": "Coordinator model preference updated on board.",
            }

        cur_loop = asyncio.get_running_loop()
        if self._loop is not None and not self._loop.is_closed() and self._loop.is_running() and self._loop is not cur_loop:
            return await self.run_on_loop(
                self.switch_model,
                name,
                model=model,
                provider=provider,
                reasoning=reasoning,
                prompt=prompt,
                return_immediately=return_immediately,
                wait_seconds=wait_seconds,
            )
        self.bind_loop(cur_loop)
        self._ensure_async_lock(cur_loop)

        async with self._async_lock:
            with self._state_rlock:
                if self._is_worker_killed(clean_name):
                    raise RuntimeError(f"Worker '{clean_name}' has been killed and cannot switch models.")
                latest = self.find_latest_worker_task(clean_name)
                if not latest:
                    raise KeyError(f"Worker '{clean_name}' not found in swarm '{self.swarm_id}'.")
                cur_state = ((latest.get("status") or {}).get("state")) or ""
                if cur_state in (models.TaskState.WORKING.value, models.TaskState.SUBMITTED.value):
                    raise RuntimeError(f"Cannot switch model while worker '{clean_name}' is {cur_state}; wait or stop it first.")

                meta = latest.get("metadata") or {}
                prev_cfg = dict(meta.get("workerConfig") or {})
                old_model = prev_cfg.get("model")
                old_provider = prev_cfg.get("provider")
                old_reasoning = prev_cfg.get("reasoning")
                if model is not None:
                    prev_cfg["model"] = model
                if provider is not None:
                    prev_cfg["provider"] = provider
                if reasoning is not None:
                    prev_cfg["reasoning"] = reasoning

                wt_path = str(meta.get("worktreePath") or "").strip()
                if wt_path and Path(wt_path).is_dir():
                    prev_cfg["workdir"] = wt_path
                    prev_cfg["worktree"] = False
                wcfg = models.WorkerConfig.from_dict(prev_cfg)
                ctx_id = str(latest.get("contextId") or models.new_id("ctx-"))

                route_changed = (
                    (model is not None and model != old_model)
                    or (provider is not None and provider != old_provider)
                    or (reasoning is not None and reasoning != old_reasoning)
                )
                carry_note = ""
                if route_changed:
                    self._context_sessions.pop(ctx_id, None)
                    carry_note = self._build_fork_seed_context(clean_name)

            if route_changed:
                await self._save_state_async()

            await asyncio.to_thread(
                self._board.upsert_member,
                clean_name,
                role=wcfg.role,
                model=wcfg.model,
                provider=wcfg.provider,
                context_id=ctx_id,
                swarm_id=self.swarm_id,
            )

            resume_msg = (prompt or "").strip() or (
                f"Your model has been switched to model={wcfg.model or 'default'} "
                f"(provider={wcfg.provider or 'default'}). Resume your unfinished assignment and report on the Swarm Board."
            )
            if carry_note:
                resume_msg = f"{carry_note}\n\n{resume_msg}"

            parts = [models.text_part(resume_msg)]
            task_id_out, done_event = await self._send_message_locked(
                message_parts=parts,
                prompt_body=resume_msg,
                context_id=ctx_id,
                task_id=None,
                config=wcfg,
                preserve_assignment=True,
                allow_unkill=False,
            )

        if not return_immediately:
            try:
                await asyncio.wait_for(done_event.wait(), timeout=max(1.0, float(wait_seconds)))
            except asyncio.TimeoutError:
                pass
        task = await self.get_task(task_id_out, include_artifacts=True)
        return {
            "switched": True,
            "worker": clean_name,
            "contextId": ctx_id,
            "model": wcfg.model,
            "provider": wcfg.provider,
            "reasoning": wcfg.reasoning,
            "task": task,
        }

    async def kill_worker(
        self,
        name: str,
        *,
        reason: str = "Killed by coordinator.",
    ) -> dict[str, Any]:
        """Permanently terminate a worker (or `<name>-1..k` group), mark killed on board, and release claims."""
        clean_name = str(name or "").strip().lstrip("@")
        wlow = clean_name.lower()
        board = await asyncio.to_thread(self._board.read_board, self.swarm_id)
        members_map = board.get("members") or {}
        all_tasks = self._find_worker_tasks(clean_name)
        if clean_name not in members_map and not all_tasks and wlow:
            prefix = f"{wlow}-"
            group_targets = [
                mname for mname in members_map
                if mname.lower().startswith(prefix) and mname[len(prefix):].isdigit()
            ]
            if group_targets:
                with self._state_rlock:
                    for gm in group_targets:
                        self._killed_workers.add(gm.strip().lower())
                    self._suppress_all_finished_wake = True
                results = [await self._kill_single_worker(m, reason=reason) for m in sorted(group_targets)]
                return {"killed": True, "group": clean_name, "count": len(results), "members": results}
        return await self._kill_single_worker(clean_name, reason=reason)

    async def _kill_single_worker(
        self,
        name: str,
        *,
        reason: str = "Killed by coordinator.",
    ) -> dict[str, Any]:
        """Permanently terminate a single worker, mark it killed on the board, and release its scope claims."""
        wlow = str(name or "").strip().lower()
        with self._state_rlock:
            if wlow:
                self._killed_workers.add(wlow)
            self._last_active_worker_count = self._count_active_workers_unlocked()
            if self._last_active_worker_count <= 1:
                self._suppress_all_finished_wake = True

        all_tasks = self._find_worker_tasks(name)
        latest = all_tasks[0] if all_tasks else None

        board = await asyncio.to_thread(self._board.read_board, self.swarm_id)
        existing_mem = (board.get("members") or {}).get(name)
        role_val = (
            (((latest.get("metadata") or {}).get("workerConfig") or {}).get("role") if latest else None)
            or (existing_mem.get("role") if isinstance(existing_mem, dict) else None)
            or models.Role.WORKER.value
        )
        wt_path = (
            ((latest.get("metadata") or {}).get("worktreePath") if latest else None)
            or (existing_mem.get("worktreePath") if isinstance(existing_mem, dict) else None)
        )

        # Atomically mark killed and clear queued mentions FIRST so no router/observer
        # race sees a non-killed window or dispatches pending wakeups while canceling.
        await asyncio.to_thread(
            self._board.upsert_member,
            name,
            role=role_val,
            state=models.MemberState.KILLED.value,
            killed=True,
            swarm_id=self.swarm_id,
        )
        await asyncio.to_thread(self._board.drain_pending_mentions, name, swarm_id=self.swarm_id)

        canceled_task: dict[str, Any] | None = None
        for t in all_tasks:
            tid = str(t.get("id") or "")
            if not tid:
                continue
            st = ((t.get("status") or {}).get("state")) or ""
            if st not in models.TERMINAL_STATES:
                try:
                    res_task = await self.cancel_task(tid, reason=reason)
                    if canceled_task is None:
                        canceled_task = res_task
                except RuntimeError:
                    with self._state_rlock:
                        cur_t = self._tasks.get(tid)
                        if cur_t and canceled_task is None:
                            canceled_task = dict(cur_t)
            elif canceled_task is None:
                canceled_task = t

            job = self._bg_jobs.get(tid)
            if job is not None and not job.done():
                job.cancel()
                try:
                    await asyncio.wait_for(job, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass

        # Sweep lingering `win_launcher.py` / `board_cli.py wait` / worktree-locked subprocesses so Windows releases handles & orphans.
        hermes_sids: list[str] = []
        worker_task_ids: list[str] = []
        with self._state_rlock:
            for t in all_tasks:
                tid_str = str(t.get("id") or "").strip()
                if tid_str:
                    worker_task_ids.append(tid_str)
                md = t.get("metadata") or {}
                act = md.get("liveActivity") or {}
                cid = str(t.get("contextId") or "").strip()
                hsid = str(act.get("hermesSessionId") or (self._context_sessions.get(cid) if cid else "") or "").strip()
                if hsid and hsid not in hermes_sids:
                    hermes_sids.append(hsid)
        for hsid in hermes_sids:
            try:
                await win_launcher.interrupt_hermes_gateway_session(hsid)
            except Exception:
                pass
        killed_orphans = await win_launcher.find_and_kill_orphan_worker_processes(
            swarm_id=self.swarm_id,
            worker_name=name,
            worktree_path=str(wt_path) if wt_path else None,
            task_ids=worker_task_ids,
        )
        if killed_orphans:
            await asyncio.sleep(0.05)

        with self._state_rlock:
            self._last_active_worker_count = self._count_active_workers_unlocked()

        await asyncio.to_thread(
            self._board.upsert_member,
            name,
            role=role_val,
            state=models.MemberState.KILLED.value,
            killed=True,
            swarm_id=self.swarm_id,
        )
        released = await asyncio.to_thread(self._board.release_scope, holder=name, swarm_id=self.swarm_id)
        self._emit_observer_event("member_killed", {"worker": name})
        return {
            "killed": True,
            "worker": name,
            "swarmId": self.swarm_id,
            "releasedScopes": released.get("released") or [],
            "killedOrphanPids": killed_orphans,
            "task": canceled_task,
        }

    async def diff_worker(
        self,
        name_or_task_id: str | None = None,
        *,
        include_patch: bool = True,
        max_bytes: int = 200_000,
    ) -> dict[str, Any]:
        """Inspect git diff for one worker/task ID, or aggregate diffs across all workers when omitted."""
        clean_target = str(name_or_task_id or "").strip()
        if not clean_target:
            with self._state_rlock:
                all_tasks = list(self._tasks.values())
            latest_by_worker: dict[str, dict[str, Any]] = {}
            all_tasks.sort(
                key=lambda t: str(((t.get("status") or {}).get("timestamp")) or ((t.get("metadata") or {}).get("createdAt")) or ""),
                reverse=True,
            )
            for t in all_tasks:
                wcfg = (t.get("metadata") or {}).get("workerConfig") or {}
                wn = str(wcfg.get("worker_name") or t.get("id") or "").strip()
                if wn and wn.lower() not in latest_by_worker:
                    latest_by_worker[wn.lower()] = dict(t)
            if not latest_by_worker:
                return {"workers": [], "count": 0}
            cached_diffs: dict[tuple[str, str], dict[str, Any]] = {}
            diffs_out: list[dict[str, Any]] = []
            for key in sorted(latest_by_worker):
                task = latest_by_worker[key]
                meta = task.get("metadata") or {}
                wcfg = meta.get("workerConfig") or {}
                target_dir = meta.get("worktreePath") or wcfg.get("workdir")
                branch_hint = meta.get("worktreeBranch")
                cache_key = (str(target_dir or ""), str(branch_hint or ""))
                if cache_key not in cached_diffs:
                    cached_diffs[cache_key] = await worktree_mod.inspect_git_diff(
                        target_dir,
                        include_patch=include_patch,
                        max_bytes=max_bytes,
                        branch_hint=branch_hint,
                    )
                diffs_out.append({
                    "taskId": task.get("id"),
                    "worker": wcfg.get("worker_name"),
                    **cached_diffs[cache_key],
                })
            return {"workers": diffs_out, "count": len(diffs_out)}

        with self._state_rlock:
            task = self._tasks.get(clean_target)
        if not task:
            task = self.find_latest_worker_task(clean_target)
        if not task:
            raise KeyError(f"Worker or task '{clean_target}' not found in swarm '{self.swarm_id}'.")
        meta = task.get("metadata") or {}
        wcfg = meta.get("workerConfig") or {}
        target_dir = meta.get("worktreePath") or wcfg.get("workdir")
        res = await worktree_mod.inspect_git_diff(
            target_dir,
            include_patch=include_patch,
            max_bytes=max_bytes,
            branch_hint=meta.get("worktreeBranch"),
        )
        return {"taskId": task.get("id"), "worker": wcfg.get("worker_name"), **res}

    async def wake_worker_from_board(self, worker_name: str, prompt: str) -> bool:
        """Called by MentionRouter when a board @mention targets an idle Tier-2 Hermes worker."""
        if self._is_member_killed_on_board(worker_name) or self.is_worker_busy(worker_name):
            return False
        latest = self.find_latest_worker_task(worker_name)
        if not latest:
            return False
        cur_state = ((latest.get("status") or {}).get("state")) or ""
        if cur_state in (models.TaskState.WORKING.value, models.TaskState.SUBMITTED.value):
            return False
        try:
            await self.resume_worker(worker_name, prompt, return_immediately=True)
            return True
        except Exception:
            return False

    async def clear_finished_tasks(
        self,
        *,
        states: tuple[str, ...] = (
            models.TaskState.COMPLETED.value,
            models.TaskState.FAILED.value,
            models.TaskState.CANCELED.value,
        ),
    ) -> dict[str, Any]:
        removed: list[str] = []
        async with self._async_lock:
            with self._state_rlock:
                for tid, task in list(self._tasks.items()):
                    st = str(((task.get("status") or {}).get("state")) or "")
                    if st in states:
                        self._tasks.pop(tid, None)
                        self._live_buffers.pop(tid, None)
                        self._events.pop(tid, None)
                        self._bg_jobs.pop(tid, None)
                        self._subscribers.pop(tid, None)
                        removed.append(tid)
            if removed:
                await self._save_state_async()
        if removed:
            self._emit_observer_event("tasks_cleared", {"removedTaskIds": removed})
        return {"swarmId": self.swarm_id, "removedCount": len(removed), "removedTaskIds": removed}

    async def shutdown(self) -> None:
        """Terminate any live worker subprocesses in parallel cleanly."""
        procs = list(self._procs.values())
        if procs:
            await asyncio.gather(*(win_launcher.kill_process_tree(p, timeout=3.0) for p in procs), return_exceptions=True)
        for job in list(self._bg_jobs.values()):
            if not job.done():
                job.cancel()
        self._bg_jobs.clear()


class SwarmManager:
    """Coordinator across per-swarm SwarmRuntime, shared BoardService, and MentionRouter."""

    def __init__(
        self,
        *,
        state_dir: Path | str | None = None,
        http_url: str | None = None,
        default_workdir_provider: Callable[[], str | None] | None = None,
    ) -> None:
        self._state_dir = Path(store.normalize_local_path(str(state_dir))) if state_dir else None
        self.store = store.SwarmStore(self._state_dir)
        self.board_service = board_mod.BoardService(self._state_dir)
        self._runtimes: dict[str, SwarmRuntime] = {}
        self._lock = threading.RLock()
        self._host_loop: asyncio.AbstractEventLoop | None = None
        self.http_url: str | None = http_url
        self._default_workdir_provider = default_workdir_provider
        self._board_subscribers: dict[str, list[tuple[asyncio.Queue[dict[str, Any]], asyncio.AbstractEventLoop | None]]] = {}
        self._global_wake_subscribers: list[tuple[asyncio.Queue[dict[str, Any]], asyncio.AbstractEventLoop | None]] = []
        self._wake_history: dict[str, list[dict[str, Any]]] = {}
        self._wake_seq_by_swarm: dict[str, int] = {}
        self._bg_persist_tasks: set[asyncio.Task[Any]] = set()
        self._internal_token: str | None = None
        self._internal_token_file: Path | None = None

        self.mention_router = wakeup_mod.MentionRouter(
            self.board_service,
            is_worker_busy=self._is_worker_busy,
            wake_worker=self._wake_worker,
            emit_event=self._emit_board_event,
            poll_interval=0.5,
        )
        self.board_service.add_listener(self._on_board_service_event)

    def _is_worker_busy(self, swarm_id: str, worker_name: str) -> bool:
        with self._lock:
            rt = self._runtimes.get(swarm_id)
        return rt.is_worker_busy(worker_name) if rt is not None else False

    async def _wake_worker(self, swarm_id: str, worker_name: str, prompt: str) -> bool:
        rt = self.get_runtime(swarm_id)
        return await rt.run_on_loop(rt.wake_worker_from_board, worker_name, prompt)

    def _on_board_service_event(self, swarm_id: str, event_type: str, payload: dict[str, Any]) -> None:
        self._emit_board_event(swarm_id, event_type, payload)
        if event_type == "board_post" and isinstance(payload.get("message"), dict):
            msg = payload["message"]
            loop = self._host_loop
            if loop is not None and not loop.is_closed() and loop.is_running():
                try:
                    cur = asyncio.get_running_loop()
                except RuntimeError:
                    cur = None
                if cur is loop:
                    loop.create_task(self.mention_router.process_message(swarm_id, msg))
                else:
                    asyncio.run_coroutine_threadsafe(self.mention_router.process_message(swarm_id, msg), loop)
            else:
                try:
                    running = asyncio.get_running_loop()
                    running.create_task(self.mention_router.process_message(swarm_id, msg))
                except RuntimeError:
                    pass

    def get_internal_token(self) -> str:
        """Return the configured Bearer token or a per-stateDir internal secret for CLI -> HTTP posts."""
        env_tok = os.environ.get("HERMES_A2A_HTTP_TOKEN", "").strip()
        if env_tok:
            return env_tok
        with self._lock:
            if self._internal_token:
                return self._internal_token
            tok_file = self.store.state_dir / "internal_token"
            tok_file.parent.mkdir(parents=True, exist_ok=True)
            if tok_file.is_file():
                existing = tok_file.read_text(encoding="utf-8", errors="replace").strip()
                if existing:
                    self._internal_token = existing
                    self._internal_token_file = tok_file
                    return existing
            token = secrets.token_urlsafe(32)
            tok_file.write_text(token, encoding="utf-8")
            try:
                os.chmod(tok_file, 0o600)
            except OSError:
                pass
            self._internal_token = token
            self._internal_token_file = tok_file
            return token

    def get_internal_token_file(self) -> Path | None:
        self.get_internal_token()
        with self._lock:
            tok_file = self._internal_token_file or (self.store.state_dir / "internal_token")
            if tok_file.is_file():
                return tok_file
            return None

    def set_host_loop(self, loop: asyncio.AbstractEventLoop | None) -> None:
        self._host_loop = loop
        if loop is not None and not loop.is_closed() and loop.is_running():
            with self._lock:
                for rt in self._runtimes.values():
                    rt.bind_loop(loop)
            self.mention_router.start_watcher(loop)

    def _ensure_host_loop(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        target = loop
        if target is None:
            try:
                target = asyncio.get_running_loop()
            except RuntimeError:
                return
        if target.is_closed() or not target.is_running():
            return
        if self._host_loop is None or self._host_loop.is_closed() or not self._host_loop.is_running():
            self.set_host_loop(target)

    def set_http_url(self, url: str | None) -> None:
        self.http_url = url
        with self._lock:
            for rt in self._runtimes.values():
                rt.http_url = url

    def set_default_workdir_provider(self, provider: Callable[[], str | None] | None) -> None:
        self._default_workdir_provider = provider

    def get_default_workdir(self, swarm_id: str | None = None) -> str | None:
        if swarm_id:
            meta = self.store.read_meta(swarm_id)
            if meta and meta.get("workspace"):
                return str(meta["workspace"])
        if self._default_workdir_provider is not None:
            wd = self._default_workdir_provider()
            if wd:
                return store.normalize_local_path(wd)
        env_dir = (os.environ.get("HERMES_A2A_DEFAULT_WORKDIR") or "").strip()
        if env_dir:
            return store.normalize_local_path(env_dir)
        return None

    def get_runtime(
        self,
        swarm_id: str | None = None,
        *,
        conversation_id: str | None = None,
    ) -> SwarmRuntime:
        self._ensure_host_loop()
        sid = self.resolve_swarm_id(swarm_id=swarm_id, conversation_id=conversation_id)
        with self._lock:
            rt = self._runtimes.get(sid)
            if rt is None:
                rt = SwarmRuntime(
                    swarm_id=sid,
                    board_service=self.board_service,
                    mention_router=self.mention_router,
                    state_dir=self._state_dir,
                    http_url=self.http_url,
                    token_file_provider=self.get_internal_token_file,
                    on_board_event=self._emit_board_event,
                    on_bind_loop=self._ensure_host_loop,
                    loop=self._host_loop,
                )
                self._runtimes[sid] = rt
            elif self.http_url:
                rt.http_url = self.http_url
            return rt

    def resolve_swarm_id(
        self,
        *,
        swarm_id: str | None = None,
        conversation_id: str | None = None,
        workspace: str | None = None,
    ) -> str:
        explicit = store.sanitize_swarm_id(swarm_id)
        if explicit:
            return explicit
        clean_conv = str(conversation_id).strip() if conversation_id and str(conversation_id).strip() else None
        if clean_conv:
            matched = self.store.resolve_swarm_for_conversation(clean_conv)
            if matched:
                return matched
            return store.derive_swarm_id(conversation_id=clean_conv)
        return self.store.resolve_swarm_id(workspace=workspace)

    def get_all_runtime_tasks(self) -> dict[str, list[dict[str, Any]]]:
        with self._lock:
            runtimes = list(self._runtimes.items())
        return {sid: rt.get_task_snapshots() for sid, rt in runtimes}

    def list_swarms(
        self,
        *,
        include_archived: bool = True,
        conversation_id: str | None = None,
        workspace: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.store.list_swarms(
            include_archived=include_archived,
            current_http_url=self.http_url,
            runtime_tasks_by_swarm=self.get_all_runtime_tasks(),
            conversation_id=conversation_id,
            workspace=workspace,
        )

    def subscribe_board(self, swarm_id: str) -> asyncio.Queue[dict[str, Any]]:
        sid = store.sanitize_swarm_id(swarm_id) or "default"
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=128)
        try:
            sub_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            sub_loop = None
        with self._lock:
            self._board_subscribers.setdefault(sid, []).append((q, sub_loop))
        return q

    def unsubscribe_board(self, swarm_id: str, q: asyncio.Queue[dict[str, Any]]) -> None:
        sid = store.sanitize_swarm_id(swarm_id) or "default"
        with self._lock:
            subs = self._board_subscribers.get(sid)
            if not subs:
                return
            self._board_subscribers[sid] = [item for item in subs if item[0] is not q]
            if not self._board_subscribers[sid]:
                self._board_subscribers.pop(sid, None)

    def subscribe_wake_events(self) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=256)
        try:
            sub_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            sub_loop = None
        with self._lock:
            self._global_wake_subscribers.append((q, sub_loop))
        return q

    def unsubscribe_wake_events(self, q: asyncio.Queue[dict[str, Any]]) -> None:
        with self._lock:
            self._global_wake_subscribers = [item for item in self._global_wake_subscribers if item[0] is not q]

    def _active_swarm_ids(self) -> list[str]:
        candidates: set[str] = set()
        swarms_dir = self.store.state_dir / "swarms"
        if swarms_dir.is_dir():
            try:
                for entry in swarms_dir.iterdir():
                    if not entry.is_dir():
                        continue
                    sid = store.sanitize_swarm_id(entry.name)
                    if not sid:
                        continue
                    paths = self.store.paths(sid)
                    if paths.meta_file.is_file() or paths.board_file.is_file() or paths.tasks_file.is_file():
                        candidates.add(sid)
            except OSError:
                pass
        with self._lock:
            candidates.update(self._runtimes.keys())
        return sorted(candidates)

    def list_active_wake_swarms(self) -> list[dict[str, Any]]:
        """Return active (non-archived) swarms with conversation IDs, head seq, and activeWorkerCount."""
        out: list[dict[str, Any]] = []
        for sid in self._active_swarm_ids():
            meta = self.store.read_meta(sid) or models.default_swarm_meta(sid)
            if meta.get("archived"):
                continue
            board = self.board_service.read_board(sid)
            head = self.board_service.head_seq(board)
            with self._lock:
                rt = self._runtimes.get(sid)
                mem_wake_seq = max(0, int(self._wake_seq_by_swarm.get(sid, 0) or 0))
            active_workers = rt.get_active_worker_count() if rt is not None else 0
            disk_wake_seq = max(0, int(meta.get("wakeSeq") or 0))
            updated_at = max(str(meta.get("updatedAt") or ""), str(board.get("updatedAt") or "")) or str(meta.get("createdAt") or "")
            out.append({
                "swarmId": sid,
                "primaryConversationId": meta.get("primaryConversationId"),
                "coordinatorConversationId": meta.get("coordinatorConversationId"),
                "projectId": meta.get("projectId"),
                "head": head,
                "wakeSeq": max(disk_wake_seq, mem_wake_seq),
                "updatedAt": updated_at,
                "activeWorkerCount": active_workers,
            })
        out.sort(key=lambda item: str(item.get("updatedAt") or ""), reverse=True)
        return out

    def _build_wake_event_payload(
        self,
        sid: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        wake_seq: int,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        resolved_meta = meta if isinstance(meta, dict) else (self.store.read_meta(sid) or {})
        raw_msg = payload.get("message")
        msg_in = dict(raw_msg) if isinstance(raw_msg, dict) else None
        is_synthetic = bool(payload.get("synthetic", False)) or (msg_in is None)
        raw_seq = payload.get("seq") if payload.get("seq") is not None else ((msg_in or {}).get("seq") or 0)
        seq_val = max(0, int(raw_seq or 0))

        if msg_in is None:
            summary_txt = str(payload.get("summary") or f"Task event in {sid}")
            worker_sender = str(payload.get("worker") or (payload.get("task") or {}).get("workerName") or "runtime")
            worker_role = str(payload.get("workerRole") or models.Role.WORKER.value)
            default_kind = (
                models.MessageType.ESCALATION.value
                if ("FAILED" in str(payload.get("state") or "") or "INPUT_REQUIRED" in str(payload.get("state") or ""))
                else models.MessageType.POST.value
            )
            msg_kind = str(payload.get("kind") or default_kind)
            msg_obj: dict[str, Any] = {
                "seq": seq_val,
                "wakeSeq": wake_seq,
                "id": str(payload.get("id") or f"wake-{sid}-{wake_seq}"),
                "sender": worker_sender,
                "senderRole": worker_role,
                "senderTier": 2,
                "kind": msg_kind,
                "type": msg_kind,
                "summary": summary_txt,
                "text": summary_txt,
                "mentions": [models.Role.COORDINATOR.value] if event_type == "coordinator_wake" else [models.Role.PRIMARY.value],
                "status": "open",
                "synthetic": True,
            }
        else:
            summary_txt = str(msg_in.get("summary") or "")
            details_txt = str(msg_in.get("details") or "")
            full_text = str(
                msg_in.get("text")
                or (
                    f"{summary_txt}\n\n{details_txt}"
                    if (summary_txt and details_txt and details_txt != summary_txt)
                    else (details_txt or summary_txt)
                )
            )
            msg_kind = str(msg_in.get("kind") or msg_in.get("type") or models.MessageType.POST.value)
            msg_obj = {
                "seq": seq_val,
                "wakeSeq": wake_seq,
                "id": msg_in.get("id"),
                "sender": msg_in.get("sender") or "worker",
                "senderRole": msg_in.get("senderRole") or models.Role.WORKER.value,
                "senderTier": int(msg_in.get("senderTier") or 2),
                "kind": msg_kind,
                "type": str(msg_in.get("type") or msg_kind),
                "summary": summary_txt,
                "text": full_text,
                "mentions": list(msg_in.get("mentions") or []),
                "status": msg_in.get("status") or "open",
                "synthetic": is_synthetic,
            }

        ev: dict[str, Any] = {
            "type": event_type,
            "swarmId": sid,
            "primaryConversationId": payload.get("primaryConversationId") or resolved_meta.get("primaryConversationId"),
            "coordinatorConversationId": payload.get("coordinatorConversationId") or resolved_meta.get("coordinatorConversationId"),
            "projectId": payload.get("projectId") or resolved_meta.get("projectId"),
            "seq": seq_val,
            "wakeSeq": wake_seq,
            "synthetic": is_synthetic,
            "at": models.now_iso(),
            "message": msg_obj,
        }
        if isinstance(payload.get("task"), dict):
            ev["task"] = dict(payload["task"])
        return ev

    def get_wake_history(self, swarm_id: str, *, since_seq: int = 0) -> list[dict[str, Any]]:
        sid = store.sanitize_swarm_id(swarm_id) or "default"
        with self._lock:
            hist = list(self._wake_history.get(sid) or [])
        return [ev for ev in hist if int(ev.get("seq") or 0) > since_seq]

    def get_wake_catchup_events(self, since_map: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Collect missed `coordinator_wake`/`primary_wake` events across active swarms for `since_map`."""
        if not isinstance(since_map, dict) or not since_map:
            return []

        collected: list[dict[str, Any]] = []
        active_sids = set(self._active_swarm_ids())
        for raw_sid, raw_cursor in since_map.items():
            sid = store.sanitize_swarm_id(str(raw_sid))
            if not sid or sid not in active_sids:
                continue
            meta = self.store.read_meta(sid) or models.default_swarm_meta(sid)
            if meta.get("archived"):
                continue

            if isinstance(raw_cursor, dict):
                since_seq = max(0, int(raw_cursor.get("seq") or 0))
                since_wake_seq = max(0, int(raw_cursor.get("wakeSeq") or 0))
            else:
                try:
                    since_seq = max(0, int(raw_cursor or 0))
                except (TypeError, ValueError):
                    since_seq = 0
                since_wake_seq = 0

            seen_board_keys: set[tuple[str, int]] = set()
            with self._lock:
                hist = list(self._wake_history.get(sid) or [])
            for ev in hist:
                ev_type = str(ev.get("type") or "")
                ev_seq = int(ev.get("seq") or 0)
                ev_wseq = int(ev.get("wakeSeq") or 0)
                if ev.get("synthetic"):
                    if ev_wseq > since_wake_seq:
                        collected.append(ev)
                elif ev_seq > since_seq:
                    seen_board_keys.add((ev_type, ev_seq))
                    collected.append(ev)

            board = self.board_service.read_board(sid)
            msgs = [m for m in (board.get("messages") or []) if isinstance(m, dict) and int(m.get("seq") or 0) > since_seq]
            msgs.sort(key=lambda m: int(m.get("seq") or 0))
            for m in msgs:
                wkind = wakeup_mod.classify_wake_event(m)
                if not wkind:
                    continue
                mseq = int(m.get("seq") or 0)
                if (wkind, mseq) in seen_board_keys:
                    continue
                seen_board_keys.add((wkind, mseq))
                collected.append(
                    self._build_wake_event_payload(
                        sid,
                        wkind,
                        {"swarmId": sid, "seq": mseq, "message": m},
                        wake_seq=mseq,
                        meta=meta,
                    )
                )

        collected.sort(key=lambda ev: (int(ev.get("seq") or 0), int(ev.get("wakeSeq") or 0)))
        return collected

    def _schedule_wake_seq_persist(self, loop: asyncio.AbstractEventLoop, sid: str, wake_seq: int) -> None:
        if loop.is_closed() or not loop.is_running():
            return

        async def _persist() -> None:
            try:
                await asyncio.to_thread(self.store.persist_wake_seq, sid, wake_seq)
            except Exception:
                pass

        try:
            task = loop.create_task(_persist())
            self._bg_persist_tasks.add(task)
            task.add_done_callback(self._bg_persist_tasks.discard)
        except RuntimeError:
            pass

    def _emit_board_event(self, swarm_id: str, event_type: str, payload: dict[str, Any]) -> None:
        sid = store.sanitize_swarm_id(swarm_id) or "default"
        is_wake = event_type in ("coordinator_wake", "primary_wake")
        meta = (self.store.read_meta(sid) or {}) if is_wake else None

        try:
            cur_loop = asyncio.get_running_loop()
        except RuntimeError:
            cur_loop = None

        global_subs: list[tuple[asyncio.Queue[dict[str, Any]], asyncio.AbstractEventLoop | None]] = []
        wake_seq = 0
        defer_persist_seq = 0
        if is_wake and cur_loop is None:
            wake_seq = self.store.allocate_wake_seq(sid)
        with self._lock:
            if is_wake:
                if cur_loop is not None:
                    disk_seq = max(0, int((meta or {}).get("wakeSeq") or 0))
                    next_seq = max(self._wake_seq_by_swarm.get(sid, 0), disk_seq) + 1
                    self._wake_seq_by_swarm[sid] = next_seq
                    wake_seq = next_seq
                    defer_persist_seq = next_seq
                else:
                    self._wake_seq_by_swarm[sid] = max(self._wake_seq_by_swarm.get(sid, 0), wake_seq)
                    wake_seq = self._wake_seq_by_swarm[sid]
                ev = self._build_wake_event_payload(sid, event_type, payload or {}, wake_seq=wake_seq, meta=meta)
                hist = self._wake_history.setdefault(sid, [])
                hist.append(ev)
                if len(hist) > 200:
                    del hist[: len(hist) - 200]
                global_subs = list(self._global_wake_subscribers)
            else:
                ev = {"type": event_type, "swarmId": sid, "at": models.now_iso(), **(payload or {})}
            subs = list(self._board_subscribers.get(sid) or [])

        if defer_persist_seq > 0 and cur_loop is not None:
            self._schedule_wake_seq_persist(cur_loop, sid, defer_persist_seq)

        for q, sub_loop in subs:
            if sub_loop is not None and not sub_loop.is_closed() and sub_loop.is_running() and sub_loop is not cur_loop:
                try:
                    sub_loop.call_soon_threadsafe(_enqueue_drop_oldest, q, ev)
                except RuntimeError:
                    pass
            else:
                _enqueue_drop_oldest(q, ev)

        for q, sub_loop in global_subs:
            if sub_loop is not None and not sub_loop.is_closed() and sub_loop.is_running() and sub_loop is not cur_loop:
                try:
                    sub_loop.call_soon_threadsafe(_enqueue_drop_oldest, q, ev)
                except RuntimeError:
                    pass
            else:
                _enqueue_drop_oldest(q, ev)

    async def shutdown(self) -> None:
        await self.mention_router.stop_watcher()
        persist_tasks = [t for t in list(self._bg_persist_tasks) if not t.done()]
        if persist_tasks:
            await asyncio.gather(*persist_tasks, return_exceptions=True)
        with self._lock:
            runtimes = list(self._runtimes.values())
            board_subs = [sub for subs in self._board_subscribers.values() for sub in subs]
            global_subs = list(self._global_wake_subscribers)
            self._board_subscribers.clear()
            self._global_wake_subscribers.clear()

        try:
            cur_loop = asyncio.get_running_loop()
        except RuntimeError:
            cur_loop = None

        for q, sub_loop in [*board_subs, *global_subs]:
            if sub_loop is not None and not sub_loop.is_closed() and sub_loop.is_running() and sub_loop is not cur_loop:
                try:
                    sub_loop.call_soon_threadsafe(_enqueue_drop_oldest, q, None)
                except RuntimeError:
                    pass
            else:
                _enqueue_drop_oldest(q, None)

        if runtimes:
            await asyncio.gather(*(rt.shutdown() for rt in runtimes), return_exceptions=True)


def _safe_json_loads(raw: Any, default: Any) -> Any:
    if isinstance(raw, type(default)):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return default
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, type(default)) else default
    except Exception:
        return default


def _normalize_epoch_ms(raw_ts: Any) -> int | None:
    """Convert epoch seconds, epoch milliseconds, or ISO timestamps to integer epoch ms cleanly."""
    if raw_ts is None or isinstance(raw_ts, bool):
        return None
    if isinstance(raw_ts, (int, float)):
        val = float(raw_ts)
        if not math.isfinite(val) or val <= 0.0:
            return None
        if val < 1e11:
            return int(round(val * 1000.0))
        return int(round(val))
    if isinstance(raw_ts, str):
        s = raw_ts.strip()
        if not s:
            return None
        try:
            val = float(s)
            if not math.isfinite(val) or val <= 0.0:
                return None
            if val < 1e11:
                return int(round(val * 1000.0))
            return int(round(val))
        except ValueError:
            pass
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            epoch_s = dt.timestamp()
            if not math.isfinite(epoch_s) or epoch_s <= 0.0:
                return None
            return int(round(epoch_s * 1000.0))
        except Exception:
            return None
    return None


def _normalize_member_history_state(
    raw_state: Any,
    *,
    killed: bool = False,
    task_state: Any = None,
) -> str:
    """Normalize board member and task states to canonical `working` / `waiting` / `killed` / `failed`."""
    if killed:
        return "killed"

    def _norm_one(val: Any) -> str:
        s = str(val or "").strip().lower()
        if not s:
            return ""
        if s == "killed":
            return "killed"
        if s in (
            "working",
            "running",
            "active",
            "submitted",
            "task_state_working",
            "task_state_submitted",
        ):
            return "working"
        if s in (
            "failed",
            "error",
            "rejected",
            "canceled",
            "cancelled",
            "task_state_failed",
            "task_state_rejected",
            "task_state_canceled",
            "task_state_cancelled",
        ):
            return "failed"
        if s in (
            "waiting",
            "idle",
            "done",
            "stopped",
            "input_required",
            "task_state_completed",
            "task_state_input_required",
            "task_state_auth_required",
            "task_state_unspecified",
        ):
            return "waiting"
        return "waiting"

    mem_norm = _norm_one(raw_state)
    task_norm = _norm_one(task_state)
    if mem_norm == "killed" or task_norm == "killed":
        return "killed"
    if task_norm == "working":
        return "working"
    if task_norm in ("failed", "waiting") and mem_norm == "working":
        return task_norm
    return mem_norm or task_norm or "waiting"


def resolve_antigravity_brain_roots() -> list[Path]:
    """Return candidate Antigravity `brain` directories in priority order (without creating dirs)."""
    candidates: list[Path] = []
    app_data = (os.environ.get("ANTIGRAVITY_APP_DATA_DIR") or "").strip()
    if app_data:
        app_brain = Path(store.normalize_local_path(app_data)) / "brain"
        if app_brain.is_dir():
            candidates.append(app_brain)
            return candidates
    exec_data = (os.environ.get("ANTIGRAVITY_EXECUTABLE_DATA_DIR") or "").strip()
    if exec_data:
        cur = Path(store.normalize_local_path(exec_data))
        for parent in [cur, *cur.parents]:
            if parent.name == "sidecar_data":
                candidates.append(parent.parent / "brain")
                break
    gemini_root = Path.home() / ".gemini"
    default_brain = gemini_root / "antigravity" / "brain"
    candidates.append(default_brain)
    if gemini_root.is_dir():
        try:
            for child in sorted(gemini_root.iterdir()):
                if child.is_dir():
                    cand = child / "brain"
                    if cand not in candidates:
                        candidates.append(cand)
        except OSError:
            pass
    deduped: list[Path] = []
    seen: set[str] = set()
    for c in candidates:
        try:
            key = os.path.normcase(os.path.realpath(str(c)))
        except OSError:
            key = os.path.normcase(str(c))
        if key not in seen:
            seen.add(key)
            deduped.append(c)
    return deduped


def _discover_coordinator_subagent_candidates(primary_cid: str | None) -> list[str]:
    """Read `<brain>/<primary_cid>/.system_generated/subagents/*.json` for valid coordinator IDs."""
    pcid = str(primary_cid or "").strip()
    if not pcid or not _CONV_ID_RE.match(pcid):
        return []
    found: list[str] = []
    for root in resolve_antigravity_brain_roots():
        sub_dir = root / pcid / ".system_generated" / "subagents"
        if not sub_dir.is_dir():
            continue
        try:
            for f in sorted(sub_dir.glob("*.json")):
                try:
                    doc = json.loads(f.read_text(encoding="utf-8", errors="replace"))
                except Exception:
                    doc = None
                cid = str((doc or {}).get("conversationId") or f.stem).strip()
                if cid and cid not in found:
                    found.append(cid)
        except OSError:
            continue
    return found


def validate_coordinator_conversation_id(
    ccid: str,
    primary_cid: str | None = None,
) -> tuple[bool, str | None]:
    """Validate `coordinator_conversation_id` against local Antigravity brain storage when present."""
    clean_ccid = str(ccid or "").strip()
    if not clean_ccid:
        return False, "`coordinator_conversation_id` is required for action='bind_coordinator'."
    if ".." in clean_ccid or "/" in clean_ccid or "\\" in clean_ccid or not _CONV_ID_RE.match(clean_ccid):
        return False, (
            f"Invalid `coordinator_conversation_id` {clean_ccid!r}. "
            "Copy the exact conversationId returned by `invoke_subagent` verbatim."
        )
    if (os.environ.get("HERMES_A2A_SKIP_BRAIN_CHECK") or "").strip() == "1":
        return True, None

    roots = resolve_antigravity_brain_roots()
    existing_roots = [r for r in roots if r.is_dir()]
    if not existing_roots:
        return True, None

    clean_prim = str(primary_cid or "").strip() or None
    for root in existing_roots:
        cand = root / clean_ccid
        if store.is_path_within(cand, [root]) and cand.is_dir():
            return True, None
        if clean_prim and _CONV_ID_RE.match(clean_prim):
            sub_file = root / clean_prim / ".system_generated" / "subagents" / f"{clean_ccid}.json"
            if store.is_path_within(sub_file, [root]) and sub_file.is_file():
                return True, None

    primary_in_brain = bool(
        clean_prim
        and _CONV_ID_RE.match(clean_prim)
        and any((r / clean_prim).is_dir() for r in existing_roots)
    )
    if not primary_in_brain:
        return True, None

    candidates = _discover_coordinator_subagent_candidates(clean_prim)
    hint = f" Valid subagent conversationIds for {clean_prim}: {candidates}." if candidates else ""
    return False, (
        f"Coordinator conversationId '{clean_ccid}' was not found in local Antigravity brain directory "
        f"({existing_roots[0]}). Copy the exact conversationId returned by `invoke_subagent` verbatim "
        f"(never hand-type or splice UUID segments).{hint}"
    )


def resolve_or_heal_antigravity_conversation_id(
    cid: str,
    parent_cid: str | None = None,
    *,
    allow_single_candidate: bool = True,
) -> tuple[bool, str, str | None, str | None]:
    """Validate an Antigravity subagent UUID and auto-heal spliced/truncated IDs when unambiguous.

    Returns `(ok, resolved_cid, auto_corrected_from, error_message)`.
    """
    clean_cid = str(cid or "").strip()
    ok, err = validate_coordinator_conversation_id(clean_cid, parent_cid)
    if ok:
        return True, clean_cid, None, None

    clean_parent = str(parent_cid or "").strip() or None
    if clean_parent and _CONV_ID_RE.match(clean_cid):
        candidates: list[str] = []
        for cand in _discover_coordinator_subagent_candidates(clean_parent):
            if cand and cand not in candidates:
                candidates.append(cand)
        for child in discover_antigravity_child_members(clean_parent):
            cc = str(child.get("conversationId") or "").strip()
            if cc and cc not in candidates:
                candidates.append(cc)

        if candidates:
            low_in = clean_cid.lower()
            best_len = 0
            best_matches: list[str] = []
            for cand in candidates:
                low_c = cand.lower()
                pref = 0
                for a, b in zip(low_in, low_c):
                    if a != b:
                        break
                    pref += 1
                if pref >= 8:
                    if pref > best_len:
                        best_len = pref
                        best_matches = [cand]
                    elif pref == best_len:
                        best_matches.append(cand)
            if len(best_matches) == 1:
                return True, best_matches[0], clean_cid, None
            if allow_single_candidate and len(candidates) == 1:
                return True, candidates[0], clean_cid, None

    return False, clean_cid, None, err


def _resolve_antigravity_transcript_file(
    conv_ids: list[str],
    *,
    primary_cid: str | None = None,
) -> tuple[str | None, Path | None]:
    """Locate `transcript_full.jsonl` (fallback `transcript.jsonl`) safely inside Antigravity brain roots."""
    roots = [r for r in resolve_antigravity_brain_roots() if r.is_dir()]
    if not roots:
        return None, None
    cands: list[str] = []
    for c in conv_ids:
        cs = str(c or "").strip()
        if cs and _CONV_ID_RE.match(cs) and cs not in cands:
            cands.append(cs)
    for cid in cands:
        for root in roots:
            logs_dir = root / cid / ".system_generated" / "logs"
            for fname in ("transcript_full.jsonl", "transcript.jsonl"):
                fpath = logs_dir / fname
                if store.is_path_within(fpath, [root]) and fpath.is_file():
                    return cid, fpath
    # Fallback: look up Swarm Coordinator subagent under primary conversation if stored coord_cid was missing/invalid.
    for cid in _discover_coordinator_subagent_candidates(primary_cid):
        if cid in cands or not _CONV_ID_RE.match(cid):
            continue
        for root in roots:
            logs_dir = root / cid / ".system_generated" / "logs"
            for fname in ("transcript_full.jsonl", "transcript.jsonl"):
                fpath = logs_dir / fname
                if store.is_path_within(fpath, [root]) and fpath.is_file():
                    return cid, fpath
    return None, None


def _empty_parsed_transcript() -> dict[str, Any]:
    return {
        "messages": [],
        "childSubagents": [],
        "model": None,
        "provider": None,
        "tokens": {},
        "tps": 0.0,
        "steps": 0,
        "toolCalls": 0,
        "elapsedMs": 0,
        "llmActiveMs": 0,
        "lastStatus": "",
        "lastStepType": "",
        "pendingToolCalls": 0,
        "hasPendingTools": False,
        "tailPlannerHasToolCalls": False,
        "liveActive": False,
        "mtimeMs": 0,
        "lastTimestampMs": None,
        "live": {
            "active": False,
            "transcript": "",
            "reasoning": "",
            "toolCalls": [],
            "toolTrace": [],
            "currentTool": None,
            "recentTools": [],
        },
    }


def _parse_ag_tool_args(raw_args: Any) -> dict[str, Any]:
    """Decode Antigravity transcript tool_call args (where string values may be JSON-quoted)."""
    parsed_obj: Any = raw_args
    if isinstance(raw_args, str):
        try:
            parsed_obj = json.loads(raw_args)
        except Exception:
            return {}
    if not isinstance(parsed_obj, dict):
        return {}
    out: dict[str, Any] = {}
    for k, v in parsed_obj.items():
        if isinstance(v, str):
            sv = v.strip()
            if (sv.startswith('"') and sv.endswith('"')) or (sv.startswith("[") and sv.endswith("]")) or (sv.startswith("{") and sv.endswith("}")):
                try:
                    out[k] = json.loads(sv)
                    continue
                except Exception:
                    pass
        out[k] = v
    return out


def _extract_ag_subagent_cid(raw_content: str) -> str | None:
    """Extract child `conversationId` from an `invoke_subagent` GENERIC tool result."""
    m = _SUBAGENT_RESULT_CID_RE.search(str(raw_content or ""))
    if not m:
        return None
    cid = m.group(1).strip()
    return cid if _CONV_ID_RE.match(cid) else None


def _derive_ag_member_slug(role_title: str, used_low: set[str]) -> str:
    """Convert an Antigravity subagent `Role` string into a unique <=24-char member name."""
    words = [w for w in _NON_SLUG_RE.split(str(role_title or "").strip().lower()) if w]
    if not words:
        base = "ide-agent"
    else:
        chosen: list[str] = []
        cur_len = 0
        for w in words:
            add_len = len(w) + (1 if chosen else 0)
            if cur_len + add_len <= 24:
                chosen.append(w)
                cur_len += add_len
            elif not chosen:
                chosen.append(w[:24].strip("-"))
                break
            else:
                break
        base = "-".join(chosen).strip("-") or words[0][:24].strip("-") or "ide-agent"
    if base in ("coordinator", "primary", "all") or base.startswith("coordinator-"):
        base = f"ide-{base}"[:24].rstrip("-")
    cand = base
    idx = 2
    while cand.lower() in used_low:
        suf = f"-{idx}"
        cand = f"{base[: max(1, 24 - len(suf))].rstrip('-')}{suf}"
        idx += 1
    used_low.add(cand.lower())
    return cand


def _build_transcript_summary_from_parsed(parsed: dict[str, Any]) -> dict[str, Any]:
    live_blk = parsed.get("live") if isinstance(parsed.get("live"), dict) else {}
    cur_tool = live_blk.get("currentTool")
    return {
        "model": parsed.get("model"),
        "provider": parsed.get("provider"),
        "tokens": dict(parsed.get("tokens") or {}),
        "tps": float(parsed.get("tps") or 0.0),
        "steps": int(parsed.get("steps") or 0),
        "toolCalls": int(parsed.get("toolCalls") or 0),
        "elapsedMs": int(parsed.get("elapsedMs") or 0),
        "llmActiveMs": int(parsed.get("llmActiveMs") or 0),
        "lastStatus": str(parsed.get("lastStatus") or ""),
        "lastStepType": str(parsed.get("lastStepType") or ""),
        "pendingToolCalls": int(parsed.get("pendingToolCalls") or 0),
        "hasPendingTools": bool(parsed.get("hasPendingTools")),
        "tailPlannerHasToolCalls": bool(parsed.get("tailPlannerHasToolCalls")),
        "liveActive": bool(parsed.get("liveActive") or live_blk.get("active")),
        "mtimeMs": int(parsed.get("mtimeMs") or 0),
        "lastTimestampMs": parsed.get("lastTimestampMs"),
        "currentTool": dict(cur_tool) if isinstance(cur_tool, dict) else None,
        "childSubagents": [dict(c) for c in (parsed.get("childSubagents") or []) if isinstance(c, dict)],
    }


def _parse_antigravity_transcript_cached_entry(log_path: Path) -> tuple[dict[str, Any], list[int]]:
    """Parse an Antigravity JSONL transcript with bounded LRU caching keyed by `(str(path), mtime_ns, size, ino)`."""
    try:
        resolved_log = log_path.resolve()
        path_str = str(resolved_log)
        st = resolved_log.stat()
        cache_key = (path_str, int(st.st_mtime_ns), int(st.st_size), int(getattr(st, "st_ino", 0)))
        mtime_ms = int(st.st_mtime * 1000)
    except OSError:
        return _empty_parsed_transcript(), []

    with _TRANSCRIPT_CACHE_LOCK:
        cached = _TRANSCRIPT_PARSE_CACHE.get(cache_key)
        if cached is not None:
            _TRANSCRIPT_PARSE_CACHE.move_to_end(cache_key)
            return cached, cached["_msg_ids"]

    messages: list[dict[str, Any]] = []
    pending_tools: list[tuple[str, str, dict[str, Any]]] = []
    child_subagents: list[dict[str, Any]] = []
    seen_child_cids: set[str] = set()
    used_child_slugs: set[str] = {"coordinator", "primary", "all"}
    model_name: str | None = None
    pure_in_sum = 0
    cached_in_sum = 0
    out_sum = 0
    last_ctx_used = 0
    planner_steps = 0
    tool_calls_count = 0
    first_ts_ms: int | None = None
    last_ts_ms: int | None = None
    prev_ready_ms: int | None = None
    llm_active_ms = 0
    last_step_status = ""
    last_step_type = ""
    tail_planner_has_tools = False
    last_id = 0
    live_active = False
    live_transcript = ""
    live_reasoning = ""
    live_tool_calls: list[dict[str, Any]] = []
    live_tool_trace: list[dict[str, Any]] = []
    live_current_tool: dict[str, Any] | None = None
    live_recent_tools: list[str] = []

    def _clear_live() -> None:
        nonlocal live_active, live_transcript, live_reasoning, live_tool_calls, live_tool_trace, live_current_tool
        live_active = False
        live_transcript = ""
        live_reasoning = ""
        live_tool_calls = []
        live_tool_trace = []
        live_current_tool = None

    try:
        raw_lines = resolved_log.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return _empty_parsed_transcript(), []

    for line_idx, raw_line in enumerate(raw_lines):
        line = raw_line.strip()
        if not line:
            continue
        try:
            step = json.loads(line)
        except ValueError:
            continue
        if not isinstance(step, dict):
            continue

        raw_si = step.get("step_index")
        try:
            step_idx = int(raw_si) if raw_si is not None else line_idx
        except (TypeError, ValueError):
            step_idx = line_idx

        step_type = str(step.get("type") or "").strip().upper()
        step_status = str(step.get("status") or "").strip().upper()
        last_step_type = step_type
        last_step_status = step_status
        created_at = step.get("created_at")
        ts_ms = _normalize_epoch_ms(created_at)
        if ts_ms is not None:
            if first_ts_ms is None or ts_ms < first_ts_ms:
                first_ts_ms = ts_ms
            if last_ts_ms is None or ts_ms > last_ts_ms:
                last_ts_ms = ts_ms
            if prev_ready_ms is None:
                prev_ready_ms = ts_ms

        step_model = str(step.get("model") or "").strip()
        if step_model:
            model_name = step_model

        if step_type == "SYSTEM_MESSAGE":
            _clear_live()
            msg_id = max(last_id + 1, step_idx + 1)
            last_id = msg_id
            messages.append({
                "id": msg_id,
                "role": "system",
                "timestamp": created_at,
                "timestampMs": ts_ms,
                "content": step.get("content") or "",
                "reasoning": None,
                "reasoningTokens": 0,
                "toolCalls": [],
                "toolName": None,
                "toolCallId": None,
                "compacted": False,
            })
        elif step_type == "USER_INPUT":
            _clear_live()
            pending_tools.clear()
            tail_planner_has_tools = False
            msg_id = max(last_id + 1, step_idx + 1)
            last_id = msg_id
            messages.append({
                "id": msg_id,
                "role": "user",
                "timestamp": created_at,
                "timestampMs": ts_ms,
                "content": step.get("content") or "",
                "reasoning": None,
                "reasoningTokens": 0,
                "toolCalls": [],
                "toolName": None,
                "toolCallId": None,
                "compacted": False,
            })
        elif step_type == "PLANNER_RESPONSE":
            raw_tcs = step.get("tool_calls") if isinstance(step.get("tool_calls"), list) else []
            tcs_out: list[dict[str, Any]] = []
            for tc_i, tc in enumerate(raw_tcs):
                if not isinstance(tc, dict):
                    continue
                tc_id = str(tc.get("id") or tc.get("tool_call_id") or f"s{step_idx}-tc{tc_i}")
                tc_name = str(tc.get("name") or "tool")
                tc_args = tc.get("args") if "args" in tc else (tc.get("arguments") or {})
                tcs_out.append({
                    "id": tc_id,
                    "toolCallId": tc_id,
                    "name": tc_name,
                    "arguments": tc_args,
                })

            if step_status == "RUNNING":
                live_active = True
                tail_planner_has_tools = bool(tcs_out)
                live_transcript = str(step.get("content") or "")
                live_reasoning = str(step.get("thinking") or step.get("reasoning") or "")
                live_tool_calls = list(tcs_out)
                live_tool_trace = [
                    {
                        "event": "tool_use",
                        "name": tc["name"],
                        "tool_call_id": tc["id"],
                        "toolCallId": tc["id"],
                        "arguments": tc["arguments"],
                    }
                    for tc in tcs_out
                ]
                if tcs_out:
                    last_tc = tcs_out[-1]
                    live_current_tool = {
                        "name": last_tc["name"],
                        "preview": _format_tool_preview(last_tc["name"], last_tc["arguments"]),
                        "startedAt": created_at,
                    }
                    for tc in tcs_out:
                        if tc["name"] and (not live_recent_tools or live_recent_tools[-1] != tc["name"]):
                            live_recent_tools.append(tc["name"])
                    live_recent_tools = live_recent_tools[-8:]
                else:
                    live_current_tool = None
                continue

            _clear_live()
            is_err = step_status in ("ERROR", "INVALID", "CANCELED", "CANCELLED", "STOPPED", "FAILED") or bool(step.get("error"))
            err_detail = str(step.get("error") or "").strip()
            content_val = step.get("content") or ""
            if not str(content_val).strip() and err_detail:
                content_val = err_detail if err_detail.lower().startswith("error") else f"Error: {err_detail}"
            reasoning_val = step.get("thinking") or step.get("reasoning") or None
            if is_err:
                pending_tools.clear()
                tail_planner_has_tools = False
            if is_err and not str(content_val).strip() and not reasoning_val and not tcs_out:
                continue

            planner_steps += 1
            if ts_ms is not None and prev_ready_ms is not None and ts_ms >= prev_ready_ms:
                llm_active_ms += ts_ms - prev_ready_ms
            if ts_ms is not None:
                prev_ready_ms = ts_ms

            s_in = max(0, int(step.get("input_tokens") or 0))
            s_cached = max(0, int(step.get("cache_read_tokens") or 0))
            s_out = max(0, int(step.get("output_tokens") or 0))
            pure_in_sum += s_in
            cached_in_sum += s_cached
            out_sum += s_out
            if (s_in + s_cached + s_out) > 0:
                last_ctx_used = s_in + s_cached + s_out

            if not is_err:
                if tcs_out:
                    tail_planner_has_tools = True
                    for tc in tcs_out:
                        pending_tools.append((
                            tc["id"],
                            tc["name"],
                            _parse_ag_tool_args(tc["arguments"]) if tc["name"] in ("invoke_subagent", "manage_subagents") else {},
                        ))
                        tool_calls_count += 1
                        if tc["name"] and (not live_recent_tools or live_recent_tools[-1] != tc["name"]):
                            live_recent_tools.append(tc["name"])
                    live_recent_tools = live_recent_tools[-8:]
                else:
                    pending_tools.clear()
                    tail_planner_has_tools = False

            msg_id = max(last_id + 1, step_idx + 1)
            last_id = msg_id
            msg_entry: dict[str, Any] = {
                "id": msg_id,
                "role": "assistant",
                "timestamp": created_at,
                "timestampMs": ts_ms,
                "content": content_val,
                "reasoning": reasoning_val,
                "reasoningTokens": 0,
                "toolCalls": tcs_out,
                "toolName": None,
                "toolCallId": None,
                "compacted": False,
            }
            if is_err:
                msg_entry["status"] = "error"
                msg_entry["isError"] = True
            messages.append(msg_entry)
        elif step_type == "GENERIC":
            tc_id, tc_name, tc_args = pending_tools.pop(0) if pending_tools else (f"s{step_idx}-tool", "tool", {})
            if step_status == "RUNNING":
                live_active = True
                live_current_tool = {
                    "name": tc_name,
                    "preview": "",
                    "startedAt": created_at,
                }
                if not any((x.get("toolCallId") or x.get("id")) == tc_id for x in live_tool_trace):
                    live_tool_trace.append({
                        "event": "tool_use",
                        "name": tc_name,
                        "tool_call_id": tc_id,
                        "toolCallId": tc_id,
                        "arguments": "",
                    })
                if tc_name and (not live_recent_tools or live_recent_tools[-1] != tc_name):
                    live_recent_tools.append(tc_name)
                live_recent_tools = live_recent_tools[-8:]
                continue

            raw_content = str(step.get("content") or "")
            comp_ms: int | None = None
            m_comp = _TRANSCRIPT_COMPLETED_AT_RE.search(raw_content[:300])
            if m_comp:
                comp_ms = _normalize_epoch_ms(m_comp.group(1))
            if comp_ms is not None:
                if last_ts_ms is None or comp_ms > last_ts_ms:
                    last_ts_ms = comp_ms
                prev_ready_ms = comp_ms
            elif ts_ms is not None:
                prev_ready_ms = ts_ms

            is_err = step_status in ("ERROR", "INVALID") or bool(step.get("error"))
            if not is_err and tc_name == "invoke_subagent":
                child_cid = _extract_ag_subagent_cid(raw_content)
                if child_cid and child_cid not in seen_child_cids:
                    seen_child_cids.add(child_cid)
                    role_title = str(tc_args.get("Role") or tc_args.get("role") or tc_args.get("TypeName") or "IDE Agent").strip() or "IDE Agent"
                    req_model_raw = str(tc_args.get("Model") or tc_args.get("model") or "").strip()
                    req_model = None if (not req_model_raw or req_model_raw.lower() == "inherit") else req_model_raw
                    slug = _derive_ag_member_slug(role_title, used_child_slugs)
                    child_subagents.append({
                        "name": slug,
                        "role": models.normalize_member_role(role_title, runtime="antigravity"),
                        "roleTitle": role_title,
                        "assignment": role_title,
                        "conversationId": child_cid,
                        "requestedModel": req_model,
                        "typeName": str(tc_args.get("TypeName") or tc_args.get("typeName") or "self").strip(),
                        "killed": False,
                        "createdAt": created_at,
                    })
            elif not is_err and tc_name == "manage_subagents":
                mg_act = str(tc_args.get("Action") or tc_args.get("action") or "").strip().lower()
                if mg_act == "kill_all":
                    for cs in child_subagents:
                        cs["killed"] = True
                elif mg_act == "kill":
                    kill_ids = tc_args.get("ConversationIds") or tc_args.get("conversationIds") or []
                    if isinstance(kill_ids, str):
                        kill_ids = [kill_ids]
                    kill_set = {str(x).strip() for x in (kill_ids if isinstance(kill_ids, list) else []) if str(x).strip()}
                    for cs in child_subagents:
                        if cs.get("conversationId") in kill_set:
                            cs["killed"] = True
            msg_id = max(last_id + 1, step_idx + 1)
            last_id = msg_id
            messages.append({
                "id": msg_id,
                "role": "tool",
                "timestamp": created_at,
                "timestampMs": comp_ms or ts_ms,
                "content": step.get("error") or raw_content,
                "reasoning": None,
                "reasoningTokens": 0,
                "toolCalls": [],
                "toolName": tc_name,
                "toolCallId": tc_id,
                "status": "error" if is_err else "ok",
                "compacted": False,
            })
        elif step_type == "ERROR_MESSAGE":
            if step_status == "RUNNING":
                continue
            _clear_live()
            pending_tools.clear()
            tail_planner_has_tools = False
            err_raw = str(step.get("content") or "").strip()
            err_detail = str(step.get("error") or "").strip()
            if not err_raw and err_detail:
                err_raw = err_detail if err_detail.lower().startswith("error") else f"Error: {err_detail}"
            if not err_raw:
                continue
            msg_id = max(last_id + 1, step_idx + 1)
            last_id = msg_id
            messages.append({
                "id": msg_id,
                "role": "assistant",
                "timestamp": created_at,
                "timestampMs": ts_ms,
                "content": err_raw,
                "reasoning": None,
                "reasoningTokens": 0,
                "toolCalls": [],
                "toolName": None,
                "toolCallId": None,
                "status": "error",
                "isError": True,
                "stepType": "ERROR_MESSAGE",
                "compacted": False,
            })

    elapsed_ms = max(0, last_ts_ms - first_ts_ms) if (first_ts_ms is not None and last_ts_ms is not None) else 0
    ctx_limit = _resolve_context_limit(model_name, None, None)
    tokens = _normalize_tokens_dict(
        {
            "uncachedInput": pure_in_sum,
            "cacheRead": cached_in_sum,
            "output": out_sum,
        },
        context_used=last_ctx_used,
        context_limit=ctx_limit,
    )
    tps_ms = llm_active_ms if llm_active_ms >= 250 else elapsed_ms
    tps = _compute_tps(out_sum, tps_ms)
    msg_ids = [int(m["id"]) for m in messages]
    pending_count = len(pending_tools)
    has_pending = pending_count > 0 or bool(live_tool_calls) or (live_current_tool is not None)
    entry: dict[str, Any] = {
        "messages": messages,
        "childSubagents": child_subagents,
        "model": model_name,
        "provider": "antigravity" if model_name else None,
        "tokens": tokens,
        "tps": tps,
        "steps": planner_steps,
        "toolCalls": tool_calls_count,
        "elapsedMs": elapsed_ms,
        "llmActiveMs": llm_active_ms,
        "lastStatus": last_step_status,
        "lastStepType": last_step_type,
        "pendingToolCalls": pending_count,
        "hasPendingTools": has_pending,
        "tailPlannerHasToolCalls": tail_planner_has_tools,
        "liveActive": live_active,
        "mtimeMs": mtime_ms,
        "lastTimestampMs": last_ts_ms,
        "live": {
            "active": live_active,
            "transcript": live_transcript,
            "reasoning": live_reasoning,
            "toolCalls": live_tool_calls,
            "toolTrace": live_tool_trace,
            "currentTool": live_current_tool,
            "recentTools": live_recent_tools,
        },
        "_msg_ids": msg_ids,
    }
    summary_entry = _build_transcript_summary_from_parsed(entry)
    try:
        st_after = resolved_log.stat()
        key_after = (path_str, int(st_after.st_mtime_ns), int(st_after.st_size), int(getattr(st_after, "st_ino", 0)))
    except OSError:
        key_after = None

    if key_after == cache_key:
        with _TRANSCRIPT_CACHE_LOCK:
            prev_key = _TRANSCRIPT_PATH_KEYS.get(path_str)
            should_store = True
            if prev_key is not None and prev_key != cache_key:
                prev_rev = (prev_key[1], prev_key[2], prev_key[3])
                cur_rev = (cache_key[1], cache_key[2], cache_key[3])
                if cur_rev[:2] < prev_rev[:2]:
                    should_store = False
                elif cur_rev[:2] == prev_rev[:2]:
                    # Tie on (mtime_ns, size): only evict prev_key if cur_rev >= prev_rev (or disk matches cache_key).
                    if cur_rev[2] < prev_rev[2]:
                        try:
                            st_live = resolved_log.stat()
                            live_key = (
                                path_str,
                                int(st_live.st_mtime_ns),
                                int(st_live.st_size),
                                int(getattr(st_live, "st_ino", 0)),
                            )
                            if live_key != cache_key:
                                should_store = False
                        except OSError:
                            should_store = False
            if should_store:
                if prev_key is not None and prev_key != cache_key:
                    _TRANSCRIPT_PARSE_CACHE.pop(prev_key, None)
                    _TRANSCRIPT_SUMMARY_CACHE.pop(prev_key, None)
                _TRANSCRIPT_PARSE_CACHE[cache_key] = entry
                _TRANSCRIPT_PARSE_CACHE.move_to_end(cache_key)
                _TRANSCRIPT_SUMMARY_CACHE[cache_key] = summary_entry
                _TRANSCRIPT_SUMMARY_CACHE.move_to_end(cache_key)
                _TRANSCRIPT_PATH_KEYS[path_str] = cache_key
                while len(_TRANSCRIPT_PARSE_CACHE) > _TRANSCRIPT_CACHE_MAX:
                    evicted_key, _ = _TRANSCRIPT_PARSE_CACHE.popitem(last=False)
                    if _TRANSCRIPT_PATH_KEYS.get(evicted_key[0]) == evicted_key:
                        _TRANSCRIPT_PATH_KEYS.pop(evicted_key[0], None)
                while len(_TRANSCRIPT_SUMMARY_CACHE) > _TRANSCRIPT_CACHE_MAX:
                    _TRANSCRIPT_SUMMARY_CACHE.popitem(last=False)
    return entry, msg_ids


def _parse_antigravity_transcript(log_path: Path) -> dict[str, Any]:
    """Parse an Antigravity JSONL transcript into Agent Chat messages and telemetry (mtime/size/ino cached)."""
    entry, _ = _parse_antigravity_transcript_cached_entry(log_path)
    out = {k: v for k, v in entry.items() if k != "_msg_ids"}
    out["messages"] = list(entry.get("messages") or [])
    out["childSubagents"] = [dict(c) for c in (entry.get("childSubagents") or []) if isinstance(c, dict)]
    out["tokens"] = dict(entry.get("tokens") or {})
    live = entry.get("live") if isinstance(entry.get("live"), dict) else {}
    out["live"] = {
        **live,
        "toolCalls": list(live.get("toolCalls") or []),
        "toolTrace": list(live.get("toolTrace") or []),
        "recentTools": list(live.get("recentTools") or []),
        "currentTool": dict(live["currentTool"]) if isinstance(live.get("currentTool"), dict) else None,
    }
    return out


def discover_antigravity_child_members(
    coordinator_cid: str | None,
    *,
    primary_cid: str | None = None,
) -> list[dict[str, Any]]:
    """Return cached child Antigravity subagents spawned by Coordinator via `invoke_subagent`."""
    cids = [str(coordinator_cid).strip()] if coordinator_cid and str(coordinator_cid).strip() else []
    if not cids and not (primary_cid and str(primary_cid).strip()):
        return []
    summary = extract_antigravity_transcript_summary(cids, primary_cid=primary_cid)
    raw_children = summary.get("childSubagents") if isinstance(summary, dict) else None
    if not isinstance(raw_children, list):
        return []
    return [dict(c) for c in raw_children if isinstance(c, dict) and c.get("conversationId")]


def extract_antigravity_transcript_summary(
    conv_ids: list[str],
    *,
    primary_cid: str | None = None,
) -> dict[str, Any]:
    """Read Antigravity transcript summary (model, tokens, tps, steps, toolCalls, status) cached by mtime/size/ino."""
    resolved_cid, log_path = _resolve_antigravity_transcript_file(conv_ids, primary_cid=primary_cid)
    if log_path is None:
        return {}
    path_str = str(log_path)
    try:
        st = log_path.stat()
        cache_key = (path_str, int(st.st_mtime_ns), int(st.st_size), int(getattr(st, "st_ino", 0)))
    except OSError:
        return {}
    with _TRANSCRIPT_CACHE_LOCK:
        cached_sum = _TRANSCRIPT_SUMMARY_CACHE.get(cache_key)
        if cached_sum is not None:
            _TRANSCRIPT_SUMMARY_CACHE.move_to_end(cache_key)
            out = dict(cached_sum)
            out["tokens"] = dict(cached_sum.get("tokens") or {})
            if isinstance(cached_sum.get("currentTool"), dict):
                out["currentTool"] = dict(cached_sum["currentTool"])
            out["conversationId"] = resolved_cid
            return out
    parsed, _ = _parse_antigravity_transcript_cached_entry(log_path)
    out = _build_transcript_summary_from_parsed(parsed)
    out["conversationId"] = resolved_cid
    return out


def _compute_coordinator_updated_ms(
    member_rec: dict[str, Any] | None,
    meta: dict[str, Any] | None,
    *,
    last_post_ts_ms: int = 0,
    board_messages: list[Any] | None = None,
    coord_name: str = "coordinator",
) -> int:
    """Compute consistent coordinator last-activity timestamp across observer snapshot and session history."""
    lp_ms = max(0, int(last_post_ts_ms or 0))
    if lp_ms <= 0 and isinstance(board_messages, list):
        cname_low = str(coord_name or models.Role.COORDINATOR.value).strip().lower()
        latest_post_ms = 0
        latest_ctrl_ms = 0
        for m in board_messages:
            if not isinstance(m, dict):
                continue
            sender_low = str(m.get("sender") or m.get("agent") or "").strip().lower()
            srole_low = str(m.get("senderRole") or "").strip().lower()
            if sender_low != cname_low and srole_low != models.Role.COORDINATOR.value:
                continue
            mts = _normalize_epoch_ms(m.get("createdAt") or m.get("time")) or 0
            if isinstance(m.get("control"), dict):
                if mts > latest_ctrl_ms:
                    latest_ctrl_ms = mts
            else:
                if mts > latest_post_ms:
                    latest_post_ms = mts
        lp_ms = latest_post_ms or latest_ctrl_ms
    mrec = member_rec if isinstance(member_rec, dict) else {}
    smeta = meta if isinstance(meta, dict) else {}
    return max(
        _normalize_epoch_ms(mrec.get("lastHeartbeatAt")) or 0,
        _normalize_epoch_ms(mrec.get("updatedAt")) or 0,
        lp_ms,
        _normalize_epoch_ms(smeta.get("updatedAt")) or 0,
    )


def reconcile_coordinator_state(
    raw_ui_status: str,
    *,
    killed: bool = False,
    coord_stats: dict[str, Any] | None = None,
    active_worker_count: int = 0,
    member_updated_ms: int = 0,
    now_ms: int | None = None,
) -> tuple[str, int | None]:
    """Reconcile coordinator UI status (`working` | `waiting` | `killed`) and optional staleness deadline ms.

    - Active `RUNNING` steps and pending/unanswered `tool_calls` stay `working` until stale/crashed (>120s).
    - Terminal text-only `PLANNER_RESPONSE` (zero pending tool calls) or `ERROR_MESSAGE` transitions to `waiting`.
    - Mid-turn `GENERIC` / `USER_INPUT` tails stay `working` while fresh (<60s), transitioning to `waiting` once
      stale (>60s since transcript mtime AND >60s since last post/bind with 0 active workers).
    """
    if killed or raw_ui_status == "killed":
        return "killed", None

    cur_now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    stats = coord_stats if isinstance(coord_stats, dict) and coord_stats else None
    if stats is not None:
        last_status = str(stats.get("lastStatus") or "").strip().upper()
        last_step_type = str(stats.get("lastStepType") or "").strip().upper()
        live_blk = stats.get("live") if isinstance(stats.get("live"), dict) else {}
        live_active = bool(stats.get("liveActive") or live_blk.get("active"))
        pending_tools_cnt = max(0, int(stats.get("pendingToolCalls") or 0))
        has_pending_tools = bool(stats.get("hasPendingTools")) or pending_tools_cnt > 0
        tail_planner_has_tools = bool(stats.get("tailPlannerHasToolCalls"))
        mtime_ms = int(stats.get("mtimeMs") or 0)
        last_ts_ms = int(stats.get("lastTimestampMs") or 0)
        transcript_ms = mtime_ms if mtime_ms > 0 else last_ts_ms

        if live_active or last_status == "RUNNING":
            ref_run_ms = transcript_ms if transcript_ms > 0 else member_updated_ms
            if ref_run_ms > 0:
                if (cur_now_ms - ref_run_ms) >= COORDINATOR_RUNNING_STALE_MS:
                    return "waiting", None
                return "working", ref_run_ms + COORDINATOR_RUNNING_STALE_MS
            return "working", None

        if has_pending_tools:
            ref_tool_ms = transcript_ms if transcript_ms > 0 else member_updated_ms
            if ref_tool_ms > 0:
                if (cur_now_ms - ref_tool_ms) >= COORDINATOR_RUNNING_STALE_MS:
                    return "waiting", None
                return "working", ref_tool_ms + COORDINATOR_RUNNING_STALE_MS
            return "working", None

        is_terminal_err = last_status in ("ERROR", "INVALID", "CANCELED", "CANCELLED", "STOPPED", "FAILED")
        if (
            (last_step_type == "PLANNER_RESPONSE" and last_status in _TERMINAL_TRANSCRIPT_STATUSES and (is_terminal_err or not tail_planner_has_tools))
            or (last_step_type == "ERROR_MESSAGE" and last_status != "RUNNING")
            or (not last_step_type and last_status in _TERMINAL_TRANSCRIPT_STATUSES)
        ):
            return "waiting", None

        is_mid_turn = last_step_type in ("GENERIC", "USER_INPUT") or (
            last_step_type == "PLANNER_RESPONSE" and tail_planner_has_tools
        )
        ref_ms = max(transcript_ms, member_updated_ms)
        if active_worker_count == 0:
            if ref_ms > 0:
                if (cur_now_ms - ref_ms) >= COORDINATOR_STALE_MS:
                    return "waiting", None
                if is_mid_turn or raw_ui_status == "working":
                    return "working", ref_ms + COORDINATOR_STALE_MS
                return raw_ui_status, None
            return raw_ui_status, None

        if is_mid_turn and transcript_ms > 0 and (cur_now_ms - transcript_ms) < COORDINATOR_STALE_MS:
            return "working", transcript_ms + COORDINATOR_STALE_MS
        return raw_ui_status, None

    if raw_ui_status == "working" and active_worker_count == 0 and member_updated_ms > 0:
        if (cur_now_ms - member_updated_ms) >= COORDINATOR_STALE_MS:
            return "waiting", None
        return "working", member_updated_ms + COORDINATOR_STALE_MS
    return raw_ui_status, None


def _sqlite_table_columns(
    con: sqlite3.Connection,
    table: str,
    *,
    db_path: Path | None = None,
    schema_version: int | None = None,
) -> set[str]:
    clean_table = re.sub(r"[^A-Za-z0-9_]", "", str(table or ""))
    if not clean_table:
        return set()
    cache_key: tuple[str, str] | None = None
    ver = schema_version
    if db_path is not None:
        try:
            cache_key = (os.path.normcase(os.path.realpath(str(db_path))), clean_table)
        except OSError:
            cache_key = (os.path.normcase(str(db_path)), clean_table)
        if ver is None:
            try:
                ver_row = con.execute("PRAGMA schema_version").fetchone()
                ver = int(ver_row[0]) if ver_row is not None else -1
            except sqlite3.Error:
                ver = -1
        with _SQLITE_SCHEMA_CACHE_LOCK:
            cached = _SQLITE_SCHEMA_CACHE.get(cache_key)
            if cached is not None and cached[0] == ver and cached[1]:
                return set(cached[1])
    try:
        rows = con.execute(f"PRAGMA table_info({clean_table})").fetchall()
        cols = {str(r[1]) for r in rows if len(r) > 1}
        if cache_key is not None and cols:
            with _SQLITE_SCHEMA_CACHE_LOCK:
                if len(_SQLITE_SCHEMA_CACHE) > 64:
                    _SQLITE_SCHEMA_CACHE.clear()
                _SQLITE_SCHEMA_CACHE[cache_key] = (ver if ver is not None else -1, set(cols))
        return cols
    except sqlite3.Error:
        return set()


def query_member_session_history(
    state_dir: Path,
    swarm_id: str,
    member_name: str,
    *,
    after_id: int = 0,
    before_id: int = 0,
    limit: int = 100,
    subagent_session_id: str | None = None,
    runtime: SwarmRuntime | None = None,
) -> dict[str, Any]:
    """Read paginated message history and live telemetry for a swarm member (Coordinator, worker, or Tier-3 subagent)."""
    clamped_limit = max(1, min(300, int(limit or 100)))
    since_id = max(0, int(after_id or 0))
    before_val = max(0, int(before_id or 0))
    sid = store.sanitize_swarm_id(swarm_id) or "default"
    paths = store.resolve_swarm_paths(sid, state_dir=state_dir)

    swarm_meta = store.read_swarm_meta(sid, state_dir=state_dir) or models.default_swarm_meta(sid)
    meta_primary = swarm_meta.get("primaryConversationId")
    meta_coord = swarm_meta.get("coordinatorConversationId")
    meta_conv_ids = [str(c).strip() for c in (swarm_meta.get("conversationIds") or []) if str(c).strip()]
    for cid_cand in (meta_primary, meta_coord):
        if isinstance(cid_cand, str) and cid_cand.strip() and cid_cand.strip() not in meta_conv_ids:
            meta_conv_ids.append(cid_cand.strip())

    board = store.read_board_raw(sid, state_dir=state_dir)
    members = board.get("members") if isinstance(board, dict) else {}
    member_rec: dict[str, Any] = {}
    canonical_name = str(member_name or "").strip()
    if isinstance(members, dict) and canonical_name:
        hit_name, hit_rec = board_mod.find_member_ci(members, canonical_name)
        if hit_name and isinstance(hit_rec, dict):
            canonical_name = hit_name
            member_rec = hit_rec

    is_coordinator = (
        canonical_name.lower() == models.Role.COORDINATOR.value
        or str(member_rec.get("role") or "").strip().lower() == models.Role.COORDINATOR.value
    )
    is_antigravity = (
        is_coordinator
        or str(member_rec.get("runtime") or "").strip().lower() == "antigravity"
    )
    if not member_rec and not is_coordinator and meta_coord and canonical_name:
        cn_low = canonical_name.lower()
        for child in discover_antigravity_child_members(str(meta_coord), primary_cid=meta_primary):
            if str(child.get("name") or "").strip().lower() == cn_low:
                canonical_name = str(child.get("name") or canonical_name)
                member_rec = {
                    "name": canonical_name,
                    "role": child.get("role") or "engineer",
                    "runtime": "antigravity",
                    "state": models.MemberState.KILLED.value if child.get("killed") else models.MemberState.WORKING.value,
                    "killed": bool(child.get("killed")),
                    "conversationId": child.get("conversationId") or "",
                    "model": child.get("requestedModel"),
                    "provider": "antigravity",
                    "assignment": child.get("assignment") or "",
                }
                is_antigravity = True
                break

    if is_antigravity:
        ag_cid = (
            str(meta_coord or member_rec.get("conversationId") or "").strip()
            if is_coordinator
            else str(member_rec.get("conversationId") or "").strip()
        )
        board_killed = bool(member_rec.get("killed")) or str(member_rec.get("state") or "").strip().lower() == models.MemberState.KILLED.value
        worker_state = _normalize_member_history_state(member_rec.get("state") or "working", killed=board_killed)
        req_sub = str(subagent_session_id or "").strip()
        if req_sub:
            return {
                "ok": True,
                "swarm": sid,
                "swarmId": sid,
                "primaryConversationId": meta_primary,
                "coordinatorConversationId": meta_coord,
                "conversationId": meta_primary or meta_coord,
                "conversationIds": meta_conv_ids,
                "member": canonical_name or models.Role.COORDINATOR.value,
                "sessionId": None,
                "parentSessionId": ag_cid or None,
                "messages": [],
                "hasMore": False,
                "totalCount": 0,
                "lastId": since_id,
                "tokens": {},
                "tps": 0.0,
                "tpsEstimated": False,
                "currentTool": None,
                "state": worker_state,
                "model": member_rec.get("model"),
                "provider": member_rec.get("provider"),
                "workspace": swarm_meta.get("workspace"),
                "workdir": member_rec.get("workdir") or swarm_meta.get("workspace"),
                "worktreePath": None,
                "worktreeBranch": None,
                "subagents": [],
                "subagentRejected": True,
                "live": {
                    "active": False,
                    "transcript": "",
                    "reasoning": "",
                    "currentTool": None,
                    "recentTools": [],
                    "toolTrace": [],
                    "startedAt": None,
                    "elapsedMs": 0,
                },
            }

        lookup_cids = [c for c in (ag_cid, str(member_rec.get("conversationId") or "").strip()) if c]
        resolved_cid, log_path = _resolve_antigravity_transcript_file(
            lookup_cids,
            primary_cid=(meta_primary if is_coordinator else None),
        )
        if log_path is not None:
            parsed, msg_ids = _parse_antigravity_transcript_cached_entry(log_path)
        else:
            parsed, msg_ids = _empty_parsed_transcript(), []
        all_msgs: list[dict[str, Any]] = parsed.get("messages") or []
        total_count = len(all_msgs)
        if before_val > 0:
            end_idx = bisect.bisect_left(msg_ids, before_val)
            has_more = end_idx > clamped_limit
            start_idx = max(0, end_idx - clamped_limit)
            messages = [dict(m) for m in all_msgs[start_idx:end_idx]]
        elif since_id > 0:
            if not msg_ids or msg_ids[-1] <= since_id:
                has_more = False
                messages = []
            else:
                start_idx = bisect.bisect_right(msg_ids, since_id)
                remaining = total_count - start_idx
                has_more = remaining > clamped_limit
                messages = [dict(m) for m in all_msgs[start_idx : start_idx + clamped_limit]]
        else:
            has_more = total_count > clamped_limit
            messages = [dict(m) for m in all_msgs[-clamped_limit:]]
        last_id = max(since_id, int(messages[-1]["id"])) if messages else since_id
        ag_model = parsed.get("model") or member_rec.get("model")
        ag_provider = parsed.get("provider") or member_rec.get("provider") or "antigravity"
        parsed_live = parsed.get("live") if isinstance(parsed.get("live"), dict) else {}
        active_worker_count = runtime.get_active_worker_count() if (is_coordinator and runtime is not None) else 0
        ag_updated_ms = _compute_coordinator_updated_ms(
            member_rec,
            swarm_meta if is_coordinator else {},
            board_messages=board.get("messages") if isinstance(board, dict) else None,
            coord_name=canonical_name or models.Role.COORDINATOR.value,
        )
        worker_state, _ = reconcile_coordinator_state(
            worker_state,
            killed=board_killed,
            coord_stats=parsed if log_path is not None else None,
            active_worker_count=active_worker_count,
            member_updated_ms=ag_updated_ms,
        )
        ag_live_active = (worker_state == "working") and (
            bool(parsed_live.get("active")) if log_path is not None else True
        )
        cur_tool_live = parsed_live.get("currentTool")
        return {
            "ok": True,
            "swarm": sid,
            "swarmId": sid,
            "primaryConversationId": meta_primary,
            "coordinatorConversationId": meta_coord if not is_coordinator else (resolved_cid or meta_coord),
            "conversationId": resolved_cid or ag_cid or meta_primary or meta_coord,
            "conversationIds": meta_conv_ids,
            "member": canonical_name or models.Role.COORDINATOR.value,
            "runtime": "antigravity",
            "sessionId": resolved_cid or ag_cid or None,
            "parentSessionId": resolved_cid or ag_cid or None,
            "messages": messages,
            "hasMore": has_more,
            "totalCount": total_count,
            "lastId": last_id,
            "tokens": dict(parsed.get("tokens") or {}),
            "tps": float(parsed.get("tps") or 0.0),
            "tpsEstimated": False,
            "currentTool": dict(cur_tool_live) if isinstance(cur_tool_live, dict) else None,
            "state": worker_state,
            "model": ag_model,
            "provider": ag_provider,
            "workspace": swarm_meta.get("workspace"),
            "workdir": member_rec.get("workdir") or swarm_meta.get("workspace"),
            "worktreePath": None,
            "worktreeBranch": None,
            "subagents": [],
            "live": {
                "active": ag_live_active,
                "transcript": str(parsed_live.get("transcript") or ""),
                "reasoning": str(parsed_live.get("reasoning") or ""),
                "toolCalls": list(parsed_live.get("toolCalls") or []),
                "currentTool": dict(cur_tool_live) if isinstance(cur_tool_live, dict) else None,
                "recentTools": list(parsed_live.get("recentTools") or []),
                "toolTrace": list(parsed_live.get("toolTrace") or []),
                "startedAt": all_msgs[0].get("timestamp") if all_msgs else None,
                "elapsedMs": int(parsed.get("elapsedMs") or 0),
            },
        }

    raw_tasks_doc = store.read_json_safe(paths.tasks_file, {})
    tasks_map = raw_tasks_doc.get("tasks") if isinstance(raw_tasks_doc, dict) else {}
    ctx_sessions = raw_tasks_doc.get("context_sessions") if isinstance(raw_tasks_doc, dict) else {}

    task_rec: dict[str, Any] = {}
    task_id = str(member_rec.get("taskId") or "").strip()
    if isinstance(tasks_map, dict):
        if task_id and isinstance(tasks_map.get(task_id), dict):
            task_rec = tasks_map[task_id]
        elif canonical_name:
            name_low = canonical_name.lower()
            candidates: list[dict[str, Any]] = []
            for t in tasks_map.values():
                if not isinstance(t, dict):
                    continue
                wcfg = (t.get("metadata") or {}).get("workerConfig") or {}
                if str(wcfg.get("worker_name") or "").strip().lower() == name_low:
                    candidates.append(t)
            if candidates:
                candidates.sort(key=lambda t: str((t.get("status") or {}).get("timestamp") or ""), reverse=True)
                task_rec = candidates[0]

    meta = task_rec.get("metadata") if isinstance(task_rec.get("metadata"), dict) else {}
    act = meta.get("liveActivity") if isinstance(meta.get("liveActivity"), dict) else {}
    wcfg = meta.get("workerConfig") if isinstance(meta.get("workerConfig"), dict) else {}

    live_overlay: dict[str, Any] | None = None
    if runtime is not None:
        try:
            live_overlay = runtime.get_live_worker_overlay(canonical_name or member_name)
        except Exception:
            live_overlay = None

    hermes_session_id = str((live_overlay or {}).get("hermesSessionId") or act.get("hermesSessionId") or "").strip()
    if not hermes_session_id:
        ctx_id = str((live_overlay or {}).get("contextId") or task_rec.get("contextId") or member_rec.get("contextId") or "").strip()
        if ctx_id and isinstance(ctx_sessions, dict):
            hermes_session_id = str(ctx_sessions.get(ctx_id) or "").strip()
    if not hermes_session_id:
        for art in task_rec.get("artifacts") or []:
            if isinstance(art, dict) and str(art.get("name") or "").startswith("usage_and_session"):
                for part in art.get("parts") or []:
                    if isinstance(part, dict) and isinstance(part.get("data"), dict):
                        cand_sid = str(part["data"].get("hermesSessionId") or "").strip()
                        if cand_sid:
                            hermes_session_id = cand_sid
                            break
                if hermes_session_id:
                    break

    tokens = dict((live_overlay or {}).get("tokens") or meta.get("tokens") or {})
    tps = float(live_overlay["tps"] if live_overlay and live_overlay.get("tps") is not None else (act.get("tps") or 0.0))
    tps_estimated = bool(live_overlay["tpsEstimated"] if live_overlay else act.get("tpsEstimated", False))

    board_killed = bool(member_rec.get("killed")) or str(member_rec.get("state") or "").strip().lower() == models.MemberState.KILLED.value
    overlay_killed = bool(live_overlay and live_overlay.get("state") == "killed")
    is_killed = board_killed or overlay_killed
    eff_task_state = (live_overlay.get("rawState") or live_overlay.get("state")) if live_overlay else (task_rec.get("status") or {}).get("state")
    worker_state = _normalize_member_history_state(member_rec.get("state"), killed=is_killed, task_state=eff_task_state)
    worker_model = member_rec.get("model") or (live_overlay or {}).get("model") or meta.get("resolvedModel") or wcfg.get("model")
    worker_provider = member_rec.get("provider") or meta.get("resolvedProvider") or wcfg.get("provider")

    req_sub = str(subagent_session_id or "").strip()
    if worker_state == "working" and not req_sub:
        current_tool = live_overlay.get("currentTool") if live_overlay else act.get("currentTool")
    else:
        current_tool = None

    board_subs = {
        str(s.get("sessionId") or ""): s
        for s in (member_rec.get("subagents") or [])
        if isinstance(s, dict) and s.get("sessionId")
    }

    messages: list[dict[str, Any]] = []
    subagents: list[dict[str, Any]] = []
    has_more = False
    total_count = 0
    last_id = since_id
    target_session_id: str | None = hermes_session_id or None
    subagent_rejected = False
    committed_use_ids: set[str] = set()
    committed_result_ids: set[str] = set()
    last_asst_content = ""
    last_asst_reasoning = ""
    db_sess_model: str | None = None
    db_sess_provider: str | None = None

    if req_sub and not hermes_session_id:
        target_session_id = None
        subagent_rejected = True

    db_path = store.default_hermes_home() / "state.db"
    if req_sub and hermes_session_id and not db_path.is_file():
        target_session_id = None
        subagent_rejected = True

    if hermes_session_id and db_path.is_file():
        con: sqlite3.Connection | None = None
        try:
            db_st = db_path.stat()
            uri = f"{db_path.resolve().as_uri()}?mode=ro"
            con = sqlite3.connect(uri, uri=True, timeout=2.0)
            con.row_factory = sqlite3.Row
            try:
                con.execute("PRAGMA busy_timeout = 2000")
                con.execute("PRAGMA query_only = ON")
            except sqlite3.Error:
                pass

            try:
                sv_row = con.execute("PRAGMA schema_version").fetchone()
                schema_ver = int(sv_row[0]) if sv_row is not None else -1
            except sqlite3.Error:
                schema_ver = -1

            sess_cols = _sqlite_table_columns(con, "sessions", db_path=db_path, schema_version=schema_ver)
            msg_cols = _sqlite_table_columns(con, "messages", db_path=db_path, schema_version=schema_ver)

            valid_sub_ids: set[str] = set()
            now_s = time.time()
            if sess_cols and {"id", "parent_session_id", "source"}.issubset(sess_cols):
                sess_select = [
                    "id",
                    "model" if "model" in sess_cols else "NULL AS model",
                    "title" if "title" in sess_cols else "NULL AS title",
                    "started_at" if "started_at" in sess_cols else "NULL AS started_at",
                    "ended_at" if "ended_at" in sess_cols else "NULL AS ended_at",
                    "message_count" if "message_count" in sess_cols else "0 AS message_count",
                    "tool_call_count" if "tool_call_count" in sess_cols else "0 AS tool_call_count",
                    "input_tokens" if "input_tokens" in sess_cols else "0 AS input_tokens",
                    "output_tokens" if "output_tokens" in sess_cols else "0 AS output_tokens",
                    "cache_read_tokens" if "cache_read_tokens" in sess_cols else "0 AS cache_read_tokens",
                    "cache_write_tokens" if "cache_write_tokens" in sess_cols else "0 AS cache_write_tokens",
                    "reasoning_tokens" if "reasoning_tokens" in sess_cols else "0 AS reasoning_tokens",
                    "model_config" if "model_config" in sess_cols else "NULL AS model_config",
                ]
                order_col = "started_at ASC" if "started_at" in sess_cols else "id ASC"
                sub_sql = (
                    f"SELECT {', '.join(sess_select)} FROM sessions "
                    f"WHERE parent_session_id = ? AND source = 'subagent' ORDER BY {order_col}"
                )
                sub_rows = con.execute(sub_sql, (hermes_session_id,)).fetchall()
                for sr in sub_rows:
                    sd = dict(sr)
                    s_id = str(sd.get("id") or "")
                    if not s_id:
                        continue
                    valid_sub_ids.add(s_id)
                    mc = _safe_json_loads(sd.get("model_config"), {})
                    b_sub = board_subs.get(s_id) or {}
                    label = str(b_sub.get("label") or sd.get("title") or mc.get("_subagent_id") or s_id)
                    title = str(sd.get("title") or label)
                    msg_cnt = int(sd.get("message_count") or 0)
                    tc_cnt = int(sd.get("tool_call_count") or 0)
                    steps = max(1 if (msg_cnt > 0 or tc_cnt > 0) else 0, (msg_cnt + 1) // 3)
                    sub_state = str(
                        b_sub.get("state")
                        or (models.MemberState.DONE.value if sd.get("ended_at") is not None else models.MemberState.WORKING.value)
                    )
                    sub_model = sd.get("model") or b_sub.get("model")
                    sub_started = sd.get("started_at")
                    sub_ended = sd.get("ended_at")
                    if sub_started is not None:
                        sub_el_ms = max(0, int(((float(sub_ended) if sub_ended is not None else now_s) - float(sub_started)) * 1000))
                    else:
                        sub_el_ms = max(0, int(b_sub.get("elapsedMs") or 0))
                    s_in_raw = int(sd.get("input_tokens") or 0)
                    s_out_raw = int(sd.get("output_tokens") or 0)
                    s_cr_raw = int(sd.get("cache_read_tokens") or 0)
                    s_cw_raw = int(sd.get("cache_write_tokens") or 0)
                    s_reas_raw = int(sd.get("reasoning_tokens") or 0)
                    if s_in_raw > 0 or s_out_raw > 0 or s_cr_raw > 0:
                        sub_toks = _normalize_tokens_dict(
                            {
                                "input_tokens": s_in_raw,
                                "uncachedInput": s_in_raw,
                                "output_tokens": s_out_raw,
                                "cache_read_tokens": s_cr_raw,
                                "cache_write_tokens": s_cw_raw,
                                "reasoning_tokens": s_reas_raw,
                            },
                            context_limit=_resolve_context_limit(sub_model, None, None),
                        )
                    elif isinstance(b_sub.get("tokens"), dict) and b_sub["tokens"]:
                        sub_toks = _normalize_tokens_dict(
                            b_sub["tokens"],
                            context_limit=_resolve_context_limit(sub_model, None, None),
                        )
                    else:
                        sub_toks = _normalize_tokens_dict({})
                    sub_tps = float(b_sub.get("tps") or _compute_tps(int(sub_toks.get("output") or 0), sub_el_ms))
                    subagents.append({
                        "sessionId": s_id,
                        "label": label,
                        "title": title,
                        "state": sub_state,
                        "steps": steps,
                        "toolCalls": tc_cnt,
                        "model": sub_model,
                        "tokens": sub_toks,
                        "elapsedMs": sub_el_ms,
                        "tps": sub_tps,
                    })

                # Also collect nested Tier-3 descendants within this worker's own subagent tree (depth-capped)
                frontier = list(valid_sub_ids)
                for _depth in range(3):
                    if not frontier:
                        break
                    placeholders = ",".join("?" for _ in frontier)
                    desc_rows = con.execute(
                        f"SELECT id FROM sessions WHERE parent_session_id IN ({placeholders}) AND source = 'subagent'",
                        tuple(frontier),
                    ).fetchall()
                    next_frontier: list[str] = []
                    for dr in desc_rows:
                        d_id = str(dr["id"] or "")
                        if d_id and d_id not in valid_sub_ids:
                            valid_sub_ids.add(d_id)
                            next_frontier.append(d_id)
                    frontier = next_frontier

            if req_sub:
                if req_sub in valid_sub_ids:
                    target_session_id = req_sub
                else:
                    target_session_id = None
                    subagent_rejected = True

            if target_session_id and sess_cols and "id" in sess_cols:
                m_sel = [
                    "model" if "model" in sess_cols else "NULL AS model",
                    "billing_provider" if "billing_provider" in sess_cols else "NULL AS billing_provider",
                    "model_config" if "model_config" in sess_cols else "NULL AS model_config",
                ]
                s_meta_row = con.execute(
                    f"SELECT {', '.join(m_sel)} FROM sessions WHERE id = ?",
                    (target_session_id,),
                ).fetchone()
                if s_meta_row is not None:
                    db_sess_model = str(s_meta_row["model"] or "").strip() or None
                    db_sess_provider = str(s_meta_row["billing_provider"] or "").strip() or None
                    if not db_sess_provider and s_meta_row["model_config"]:
                        mc_row = _safe_json_loads(s_meta_row["model_config"], {})
                        gw_rt = mc_row.get("gateway_runtime") if isinstance(mc_row, dict) else None
                        if isinstance(gw_rt, dict) and gw_rt.get("provider"):
                            db_sess_provider = str(gw_rt["provider"]).strip() or None

            db_max_id = 0
            if target_session_id and msg_cols and {"id", "session_id"}.issubset(msg_cols):
                cnt_row = con.execute(
                    "SELECT COUNT(*) AS cnt, COALESCE(MAX(id), 0) AS max_id FROM messages WHERE session_id = ?",
                    (target_session_id,),
                ).fetchone()
                total_count = int(cnt_row["cnt"]) if cnt_row else 0
                db_max_id = int(cnt_row["max_id"]) if cnt_row else 0

                msg_select = [
                    "id",
                    "role" if "role" in msg_cols else "'' AS role",
                    "timestamp" if "timestamp" in msg_cols else "NULL AS timestamp",
                    "content" if "content" in msg_cols else "NULL AS content",
                    "reasoning" if "reasoning" in msg_cols else "NULL AS reasoning",
                    "reasoning_content" if "reasoning_content" in msg_cols else "NULL AS reasoning_content",
                    "display_metadata" if "display_metadata" in msg_cols else "NULL AS display_metadata",
                    "tool_calls" if "tool_calls" in msg_cols else "NULL AS tool_calls",
                    "tool_name" if "tool_name" in msg_cols else "NULL AS tool_name",
                    "tool_call_id" if "tool_call_id" in msg_cols else "NULL AS tool_call_id",
                    "compacted" if "compacted" in msg_cols else "0 AS compacted",
                ]
                msg_cols_sql = ", ".join(msg_select)

                if before_val > 0:
                    rows = con.execute(
                        f"SELECT {msg_cols_sql} FROM messages WHERE session_id = ? AND id < ? ORDER BY id DESC LIMIT ?",
                        (target_session_id, before_val, clamped_limit + 1),
                    ).fetchall()
                    has_more = len(rows) > clamped_limit
                    rows = list(reversed(rows[:clamped_limit]))
                elif since_id > 0:
                    rows = con.execute(
                        f"SELECT {msg_cols_sql} FROM messages WHERE session_id = ? AND id > ? ORDER BY id ASC LIMIT ?",
                        (target_session_id, since_id, clamped_limit + 1),
                    ).fetchall()
                    has_more = len(rows) > clamped_limit
                    rows = rows[:clamped_limit]
                else:
                    rows = con.execute(
                        f"SELECT {msg_cols_sql} FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT ?",
                        (target_session_id, clamped_limit + 1),
                    ).fetchall()
                    has_more = len(rows) > clamped_limit
                    rows = list(reversed(rows[:clamped_limit]))

                for mr in rows:
                    md = dict(mr)
                    dmeta = _safe_json_loads(md.get("display_metadata"), {})
                    r_tok = dmeta.get("reasoning_tokens")
                    if r_tok is None:
                        r_tok = dmeta.get("reasoningTokens")
                    if r_tok is None:
                        r_tok = dmeta.get("thinking_tokens")
                    try:
                        reasoning_tokens = max(0, int(r_tok)) if r_tok is not None and not isinstance(r_tok, bool) else 0
                    except (TypeError, ValueError):
                        reasoning_tokens = 0
                    t_calls = _safe_json_loads(md.get("tool_calls"), [])
                    raw_ts = md.get("timestamp")
                    msg_obj = {
                        "id": int(md["id"]),
                        "role": str(md.get("role") or ""),
                        "timestamp": raw_ts,
                        "timestampMs": _normalize_epoch_ms(raw_ts),
                        "content": md.get("content"),
                        "reasoning": md.get("reasoning") or md.get("reasoning_content"),
                        "reasoningTokens": reasoning_tokens,
                        "toolCalls": t_calls,
                        "toolName": md.get("tool_name"),
                        "toolCallId": md.get("tool_call_id"),
                        "compacted": bool(md.get("compacted")),
                    }
                    messages.append(msg_obj)
                    if msg_obj["role"] == "assistant":
                        last_asst_content = str(msg_obj.get("content") or "").strip()
                        last_asst_reasoning = str(msg_obj.get("reasoning") or "").strip()
                if messages:
                    last_id = max(last_id, messages[-1]["id"])

                # Incremental-cached committed toolCallId tracking (avoids full-session scans on `after_id > 0` polls).
                if live_overlay and live_overlay.get("active") and worker_state == "working" and not req_sub:
                    if not last_asst_content and not last_asst_reasoning and "role" in msg_cols:
                        la_sel = [
                            "content" if "content" in msg_cols else "NULL AS content",
                            "reasoning" if "reasoning" in msg_cols else "NULL AS reasoning",
                            "reasoning_content" if "reasoning_content" in msg_cols else "NULL AS reasoning_content",
                        ]
                        la_row = con.execute(
                            f"SELECT {', '.join(la_sel)} FROM messages WHERE session_id = ? AND role = 'assistant' ORDER BY id DESC LIMIT 1",
                            (target_session_id,),
                        ).fetchone()
                        if la_row is not None:
                            last_asst_content = str(la_row["content"] or "").strip()
                            last_asst_reasoning = str(la_row["reasoning"] or la_row["reasoning_content"] or "").strip()

                    resolved_db_key = os.path.normcase(os.path.realpath(str(db_path)))
                    tc_cache_key = (
                        resolved_db_key,
                        target_session_id,
                        int(getattr(db_st, "st_ino", 0)),
                    )
                    with _COMMITTED_TC_CACHE_LOCK:
                        cached_tc = _COMMITTED_TC_CACHE.get(tc_cache_key)
                    cache_valid = False
                    if cached_tc is not None:
                        cached_cnt, cached_max_id, c_use, c_res = cached_tc
                        if total_count == cached_cnt and db_max_id == cached_max_id:
                            committed_use_ids.update(c_use)
                            committed_result_ids.update(c_res)
                            cache_valid = True
                        elif total_count > cached_cnt and db_max_id > cached_max_id:
                            delta_sel = [
                                "id",
                                "tool_call_id" if "tool_call_id" in msg_cols else "NULL AS tool_call_id",
                                "tool_calls" if "tool_calls" in msg_cols else "NULL AS tool_calls",
                            ]
                            delta_rows = con.execute(
                                f"SELECT {', '.join(delta_sel)} FROM messages WHERE session_id = ? AND id > ? ORDER BY id ASC",
                                (target_session_id, cached_max_id),
                            ).fetchall()
                            if cached_cnt + len(delta_rows) == total_count:
                                committed_use_ids.update(c_use)
                                committed_result_ids.update(c_res)
                                new_max_id = cached_max_id
                                for dr in delta_rows:
                                    dr_id = int(dr["id"] or 0)
                                    if dr_id > new_max_id:
                                        new_max_id = dr_id
                                    tid_val = str(dr["tool_call_id"] or "").strip()
                                    if tid_val:
                                        committed_result_ids.add(tid_val)
                                    for tc_item in _safe_json_loads(dr["tool_calls"], []):
                                        if isinstance(tc_item, dict):
                                            tc_cid = str(
                                                tc_item.get("id") or tc_item.get("tool_call_id") or tc_item.get("toolCallId") or ""
                                            ).strip()
                                            if tc_cid:
                                                committed_use_ids.add(tc_cid)
                                with _COMMITTED_TC_CACHE_LOCK:
                                    _COMMITTED_TC_CACHE[tc_cache_key] = (
                                        total_count,
                                        new_max_id,
                                        set(committed_use_ids),
                                        set(committed_result_ids),
                                    )
                                cache_valid = True
                    if not cache_valid:
                        if since_id == 0 and not has_more:
                            new_max_id = 0
                            for m_item in messages:
                                mid = int(m_item.get("id") or 0)
                                if mid > new_max_id:
                                    new_max_id = mid
                                tid_val = str(m_item.get("toolCallId") or "").strip()
                                if tid_val:
                                    committed_result_ids.add(tid_val)
                                for tc_item in m_item.get("toolCalls") or []:
                                    if isinstance(tc_item, dict):
                                        tc_cid = str(
                                            tc_item.get("id") or tc_item.get("tool_call_id") or tc_item.get("toolCallId") or ""
                                        ).strip()
                                        if tc_cid:
                                            committed_use_ids.add(tc_cid)
                            with _COMMITTED_TC_CACHE_LOCK:
                                if len(_COMMITTED_TC_CACHE) > 128:
                                    _COMMITTED_TC_CACHE.clear()
                                _COMMITTED_TC_CACHE[tc_cache_key] = (
                                    total_count,
                                    max(new_max_id, db_max_id),
                                    set(committed_use_ids),
                                    set(committed_result_ids),
                                )
                        else:
                            new_max_id = 0
                            init_sel = [
                                "id",
                                "tool_call_id" if "tool_call_id" in msg_cols else "NULL AS tool_call_id",
                                "tool_calls" if "tool_calls" in msg_cols else "NULL AS tool_calls",
                            ]
                            for ir in con.execute(
                                f"SELECT {', '.join(init_sel)} FROM messages WHERE session_id = ? ORDER BY id ASC",
                                (target_session_id,),
                            ).fetchall():
                                ir_id = int(ir["id"] or 0)
                                if ir_id > new_max_id:
                                    new_max_id = ir_id
                                tid_val = str(ir["tool_call_id"] or "").strip()
                                if tid_val:
                                    committed_result_ids.add(tid_val)
                                for tc_item in _safe_json_loads(ir["tool_calls"], []):
                                    if isinstance(tc_item, dict):
                                        tc_cid = str(
                                            tc_item.get("id") or tc_item.get("tool_call_id") or tc_item.get("toolCallId") or ""
                                        ).strip()
                                        if tc_cid:
                                            committed_use_ids.add(tc_cid)
                            with _COMMITTED_TC_CACHE_LOCK:
                                if len(_COMMITTED_TC_CACHE) > 128:
                                    _COMMITTED_TC_CACHE.clear()
                                _COMMITTED_TC_CACHE[tc_cache_key] = (
                                    total_count,
                                    max(new_max_id, db_max_id),
                                    set(committed_use_ids),
                                    set(committed_result_ids),
                                )
        except (sqlite3.Error, OSError):
            pass
        finally:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass

    if db_sess_model and not req_sub and not worker_model:
        worker_model = db_sess_model
    if db_sess_provider and not req_sub and not worker_provider:
        worker_provider = db_sess_provider

    if subagent_rejected:
        tokens = {}
        tps = 0.0
        tps_estimated = False
        current_tool = None
    elif req_sub and target_session_id:
        sub_toks, _, sub_extra = _query_hermes_session_stats(target_session_id, model=worker_model)
        matched_sub = next((s for s in subagents if s.get("sessionId") == target_session_id), None)
        tokens = sub_toks or (dict(matched_sub.get("tokens") or {}) if matched_sub else {})
        if sub_extra.get("model"):
            worker_model = sub_extra["model"]
        elif matched_sub and matched_sub.get("model"):
            worker_model = matched_sub["model"]
        if sub_extra.get("billingProvider"):
            worker_provider = sub_extra["billingProvider"]
        sub_out_tok = int(tokens.get("output") or 0)
        sub_started = sub_extra.get("startedAtEpoch")
        sub_ended = sub_extra.get("endedAtEpoch")
        if sub_started is not None:
            sub_el = max(0, int(((float(sub_ended) if sub_ended is not None else time.time()) - float(sub_started)) * 1000))
        elif matched_sub:
            sub_el = int(matched_sub.get("elapsedMs") or 0)
        else:
            sub_el = 0
        tps = float(matched_sub.get("tps") or 0.0) if (matched_sub and matched_sub.get("tps")) else _compute_tps(sub_out_tok, sub_el)
        tps_estimated = False
        current_tool = None
    elif target_session_id and (not tokens or not worker_model or not worker_provider):
        db_toks, _, db_extra = _query_hermes_session_stats(target_session_id, model=worker_model, provider=worker_provider)
        if not tokens and db_toks:
            tokens = db_toks
        if not worker_model and db_extra.get("model"):
            worker_model = db_extra["model"]
        if not worker_provider and db_extra.get("billingProvider"):
            worker_provider = db_extra["billingProvider"]

    is_main_live = bool(
        live_overlay
        and live_overlay.get("active")
        and worker_state == "working"
        and not req_sub
        and not subagent_rejected
    )
    if is_main_live and live_overlay:
        filtered_trace: list[dict[str, Any]] = []
        for item in live_overlay.get("toolTrace") or []:
            if not isinstance(item, dict):
                continue
            tc_id = str(item.get("tool_call_id") or item.get("toolCallId") or "").strip()
            ev_kind = str(item.get("event") or "").strip()
            if tc_id:
                if ev_kind == "tool_use" and (tc_id in committed_use_ids or tc_id in committed_result_ids):
                    continue
                if ev_kind == "tool_result" and tc_id in committed_result_ids:
                    continue
            entry_copy = dict(item)
            if tc_id and not entry_copy.get("toolCallId"):
                entry_copy["toolCallId"] = tc_id
            filtered_trace.append(entry_copy)
        live_transcript_txt = str(live_overlay.get("transcript") or "")
        live_reasoning_txt = str(live_overlay.get("reasoning") or "")
        if live_transcript_txt.strip() and last_asst_content and live_transcript_txt.strip() == last_asst_content:
            live_transcript_txt = ""
        if live_reasoning_txt.strip() and last_asst_reasoning and live_reasoning_txt.strip() == last_asst_reasoning:
            live_reasoning_txt = ""
        live_payload: dict[str, Any] = {
            "active": True,
            "transcript": live_transcript_txt,
            "reasoning": live_reasoning_txt,
            "currentTool": live_overlay.get("currentTool"),
            "recentTools": list(live_overlay.get("recentTools") or []),
            "toolTrace": filtered_trace,
            "startedAt": live_overlay.get("startedAt"),
            "elapsedMs": max(0, int(live_overlay.get("elapsedMs") or 0)),
        }
    else:
        live_payload = {
            "active": False,
            "transcript": "",
            "reasoning": "",
            "currentTool": None,
            "recentTools": (
                list((live_overlay or {}).get("recentTools") or act.get("recentTools") or [])
                if not req_sub
                else []
            ),
            "toolTrace": [],
            "startedAt": ((live_overlay or {}).get("startedAt") or act.get("startedAt")) if not req_sub else None,
            "elapsedMs": max(0, int((live_overlay or {}).get("elapsedMs") or act.get("elapsedMs") or 0)) if not req_sub else 0,
        }

    member_workdir = member_rec.get("workdir") or wcfg.get("workdir") or swarm_meta.get("workspace")
    member_worktree_path = meta.get("worktreePath") or member_rec.get("worktreePath")
    member_worktree_branch = meta.get("worktreeBranch") or member_rec.get("worktreeBranch")

    result: dict[str, Any] = {
        "ok": True,
        "swarm": sid,
        "swarmId": sid,
        "primaryConversationId": meta_primary,
        "coordinatorConversationId": meta_coord,
        "conversationId": meta_primary or meta_coord,
        "conversationIds": meta_conv_ids,
        "member": canonical_name or member_name,
        "sessionId": target_session_id,
        "parentSessionId": hermes_session_id or None,
        "messages": messages,
        "hasMore": has_more,
        "totalCount": total_count,
        "lastId": last_id,
        "tokens": tokens,
        "tps": tps,
        "tpsEstimated": tps_estimated,
        "currentTool": current_tool,
        "state": worker_state,
        "model": worker_model,
        "provider": worker_provider,
        "workspace": swarm_meta.get("workspace"),
        "workdir": member_workdir,
        "worktreePath": member_worktree_path,
        "worktreeBranch": member_worktree_branch,
        "subagents": subagents,
        "live": live_payload,
    }
    if subagent_rejected:
        result["subagentRejected"] = True
    return result
