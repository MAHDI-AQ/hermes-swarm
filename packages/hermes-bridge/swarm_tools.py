"""Stdlib-only native Swarm Board tools for Tier-2 Hermes swarm workers."""

from __future__ import annotations

from contextlib import contextmanager
import contextvars
import ctypes
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import re
import sys
import threading
import time
from typing import Any, Iterator, Mapping
import urllib.error
import urllib.parse
import urllib.request
import uuid
import weakref

if sys.platform == "win32":
    from ctypes import wintypes
    import msvcrt

    _K32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _K32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _K32.OpenProcess.restype = ctypes.c_void_p
    _K32.GetExitCodeProcess.argtypes = [ctypes.c_void_p, ctypes.POINTER(wintypes.DWORD)]
    _K32.GetExitCodeProcess.restype = wintypes.BOOL
    _K32.CloseHandle.argtypes = [ctypes.c_void_p]
    _K32.CloseHandle.restype = wintypes.BOOL
    _K32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
    ]
    _K32.CreateFileW.restype = ctypes.c_void_p
    _INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
else:
    _K32 = None
    _INVALID_HANDLE_VALUE = -1

SCHEMA_VERSION = 2
DEFAULT_CHANNEL = "#general"
MAX_BOARD_MESSAGES = 1000
MAX_BRIDGE_LOG_BYTES = 1_048_576
HEARTBEAT_THROTTLE_SECONDS = 15.0
CHANNEL_META_REFRESH_SECONDS = 5.0

_SWARM_ID_CLEAN_RE = re.compile(r"[^a-z0-9_-]+")
_CHANNEL_CLEAN_RE = re.compile(r"[^a-z0-9_-]+")
_MENTION_RE = re.compile(r"(?<![\w.-])@([\w][\w-]*)")
_CHANNEL_MENTION_RE = re.compile(r"(?<![\w.-])@#([a-z][a-z0-9-]{0,23})(?![\w-])", re.IGNORECASE)
_FENCED_CODE_RE = re.compile(r"```[\s\S]*?```|~~~[\s\S]*?~~~")
_INLINE_CODE_RE = re.compile(r"(`+)(?:(?!\1)[^\r\n])*\1")
_RESERVED_CHANNELS = frozenset({"main", "general", "all", "here"})
_CHANNEL_SLUG_RE = re.compile(r"^[a-z][a-z0-9-]{0,23}$")
_ALL_CHANNEL_ROLES = frozenset({"coordinator", "monitor", "narrator"})
_ROLE_WORKER_NAME_RE = re.compile(r"^(?:narrator|monitor)(?:-\d+)?$", re.IGNORECASE)

_TURN_SWARM_CONTEXT: contextvars.ContextVar[dict[str, str] | None] = contextvars.ContextVar(
    "hermes_swarm_turn_context",
    default=None,
)
_WAIT_REF_LOCK = threading.Lock()
_WAIT_REFCOUNTS: dict[tuple[str, str], int] = {}
_STATE_LOCK = threading.Lock()
_BRIDGE_STATE: dict[tuple[str, str], dict[str, Any]] = {}
_COMPACTED_SESSIONS: dict[str, float] = {}
_HEARTBEAT_TIMESTAMPS: dict[tuple[str, str], float] = {}
_INBOX_STAT_CACHE: dict[tuple[str, str], tuple[int, int, int]] = {}
_INBOX_SEEN_SEQ: dict[tuple[str, str], int] = {}

_TURN_CTX_KEY_ALIASES: dict[str, tuple[str, ...]] = {
    "swarm_id": ("swarm_id", "swarmId"),
    "agent": ("agent", "worker_name", "workerName"),
    "role": ("role", "worker_role", "workerRole"),
    "http_url": ("http_url", "httpUrl"),
    "board_dir": ("board_dir", "boardDir"),
    "token_file": ("token_file", "tokenFile"),
    "http_token": ("http_token", "httpToken"),
    "conversation_id": ("conversation_id", "conversationId"),
    "task_id": ("task_id", "taskId"),
    "context_id": ("context_id", "contextId"),
    "cwd": ("cwd", "workspace", "workdir"),
}


def _normalize_turn_context(ctx: Mapping[str, Any] | None) -> dict[str, str]:
    if not isinstance(ctx, Mapping):
        return {}
    out: dict[str, str] = {}
    for canonical, aliases in _TURN_CTX_KEY_ALIASES.items():
        for alias in aliases:
            val = ctx.get(alias)
            if val is not None and str(val).strip():
                out[canonical] = str(val).strip()
                break
    return out


def set_turn_context(ctx: Mapping[str, Any] | None) -> contextvars.Token[dict[str, str] | None]:
    """Bind per-turn swarm metadata into a thread/async-safe ContextVar."""
    return _TURN_SWARM_CONTEXT.set(_normalize_turn_context(ctx))


def reset_turn_context(token: contextvars.Token[dict[str, str] | None]) -> None:
    """Restore the previous per-turn swarm metadata ContextVar."""
    _TURN_SWARM_CONTEXT.reset(token)


def get_turn_context() -> dict[str, str]:
    """Return the active per-turn swarm metadata dict (empty dict if unset)."""
    cur = _TURN_SWARM_CONTEXT.get()
    return dict(cur) if cur else {}


@contextmanager
def turn_context(ctx: Mapping[str, Any] | None) -> Iterator[dict[str, str]]:
    """Context manager scoping per-turn swarm metadata for concurrent gateway turns."""
    norm = _normalize_turn_context(ctx)
    token = _TURN_SWARM_CONTEXT.set(norm)
    try:
        yield norm
    finally:
        _TURN_SWARM_CONTEXT.reset(token)

_MESSAGE_TYPE_ALIASES: dict[str, str] = {
    "post": "post",
    "progress": "post",
    "status": "post",
    "result": "post",
    "brief": "post",
    "info": "post",
    "question": "question",
    "ask": "question",
    "approval_request": "question",
    "decision": "decision",
    "directive": "decision",
    "plan": "decision",
    "handoff": "handoff",
    "handoff_ready": "handoff",
    "escalation": "escalation",
    "blocker": "escalation",
    "alert": "escalation",
    "digest": "digest",
    "ack": "ack",
    "system": "system",
}


class _LockBox:
    __slots__ = ("lock", "__weakref__")

    def __init__(self) -> None:
        self.lock = threading.Lock()


_THREAD_LOCKS: weakref.WeakValueDictionary[str, _LockBox] = weakref.WeakValueDictionary()
_LOCKS_GUARD = threading.Lock()


def _acquire_lock_box(path: Path) -> _LockBox:
    key = os.path.normcase(os.path.abspath(str(path)))
    with _LOCKS_GUARD:
        box = _THREAD_LOCKS.get(key)
        if box is None:
            box = _LockBox()
            _THREAD_LOCKS[key] = box
        return box


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _new_id(prefix: str = "") -> str:
    uid = uuid.uuid4().hex
    return f"{prefix}{uid}" if prefix else uid


def _sanitize_swarm_id(raw: str | None) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        return "default"
    cleaned = _SWARM_ID_CLEAN_RE.sub("-", text).strip("-")
    return cleaned or "default"


def _normalize_channel_name(raw: str | None) -> str:
    text = str(raw or "").strip().lower()
    if text == "*":
        return "*"
    if text.startswith("@"):
        text = text[1:]
    if text.startswith("#"):
        text = text[1:]
    if not text or text == "main":
        return DEFAULT_CHANNEL
    slug = _CHANNEL_CLEAN_RE.sub("-", text).strip("-")[:48].strip("-")
    if not slug or slug == "main":
        return DEFAULT_CHANNEL
    return f"#{slug}"


def _channel_display_name(raw: str | None) -> str:
    norm = _normalize_channel_name(raw)
    if norm == "*":
        return "*"
    if norm == DEFAULT_CHANNEL:
        return "main"
    return norm.lstrip("#") or "main"


def _normalize_message_type(raw: str | None, default: str = "post") -> str:
    text = str(raw or "").strip().lower()
    return _MESSAGE_TYPE_ALIASES.get(text, default)


def _strip_code_spans(text: str) -> str:
    no_fence = _FENCED_CODE_RE.sub(" ", text or "")
    return _INLINE_CODE_RE.sub(" ", no_fence)


def _extract_mentions(summary: str, explicit_to: list[str] | None = None) -> list[str]:
    found: list[str] = []
    seen_low: set[str] = set()
    for item in explicit_to or []:
        cleaned = str(item or "").strip().lstrip("@")
        if not cleaned or cleaned.startswith("#"):
            continue
        low = cleaned.lower()
        if low not in seen_low:
            seen_low.add(low)
            found.append(cleaned)
    for match in _MENTION_RE.finditer(_strip_code_spans(summary)):
        cleaned = match.group(1).strip()
        low = cleaned.lower()
        if cleaned and low not in seen_low:
            seen_low.add(low)
            found.append(cleaned)
    return found


def _extract_channel_mentions(summary: str, explicit_to: list[str] | None = None) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    candidates: list[str] = []
    for item in explicit_to or []:
        raw = str(item or "").strip()
        if raw.startswith("@#"):
            candidates.append(raw[2:].lower())
        elif raw.startswith("#"):
            candidates.append(raw[1:].lower())
    for match in _CHANNEL_MENTION_RE.finditer(_strip_code_spans(summary)):
        candidates.append(match.group(1).lower())
    for slug in candidates:
        if slug in _RESERVED_CHANNELS or not _CHANNEL_SLUG_RE.match(slug):
            continue
        norm = f"#{slug}"
        if norm not in seen:
            seen.add(norm)
            found.append(norm)
    return found


def _effective_member_channels(role: str | None, stored_channels: list[str] | None, name: str = "") -> list[str]:
    r = str(role or "").strip().lower()
    if r in _ALL_CHANNEL_ROLES or _ROLE_WORKER_NAME_RE.match(str(name or "").strip()):
        return ["*"]
    raw_list = list(stored_channels or [DEFAULT_CHANNEL])
    if "*" in raw_list:
        return ["*"]
    out: list[str] = []
    for ch in raw_list:
        norm = _normalize_channel_name(str(ch))
        if norm not in out:
            out.append(norm)
    return out or [DEFAULT_CHANNEL]


def _find_registry_state_dir(board_dir_raw: str) -> Path | None:
    state_env = (os.environ.get("HERMES_A2A_STATE_DIR") or "").strip()
    if state_env:
        return Path(state_env)
    if board_dir_raw:
        bp = Path(board_dir_raw)
        if (bp / "registry.json").is_file():
            return bp
        if bp.parent.name == "swarms" and (bp.parent.parent / "registry.json").is_file():
            return bp.parent.parent
        if (bp / "internal_token").is_file():
            return bp
        if bp.parent.name == "swarms" and (bp.parent.parent / "internal_token").is_file():
            return bp.parent.parent
    for default_state in (
        Path.home() / "Tools" / "hermes-a2a-mcp" / "state",
        Path.home() / ".local" / "share" / "hermes-swarm" / "mcp-server" / "state",
    ):
        if (default_state / "registry.json").is_file() or (default_state / "internal_token").is_file():
            return default_state
    return None


def _norm_ws_key(raw_path: str | None) -> str:
    if not raw_path:
        return ""
    try:
        return os.path.normcase(os.path.realpath(str(raw_path))).rstrip("/\\")
    except OSError:
        return os.path.normcase(os.path.abspath(str(raw_path))).rstrip("/\\")


def _entry_conversation_ids(entry: dict[str, Any]) -> set[str]:
    convs: set[str] = set()
    for field in ("conversationId", "primaryConversationId", "coordinatorConversationId"):
        val = str(entry.get(field) or "").strip()
        if val:
            convs.add(val)
    raw_list = entry.get("conversationIds")
    if isinstance(raw_list, list):
        for item in raw_list:
            val = str(item or "").strip()
            if val:
                convs.add(val)
    return convs


def _collect_registry_entries(reg: Any) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    if isinstance(reg, list):
        for idx, item in enumerate(reg):
            if isinstance(item, dict):
                out.append((str(idx), item))
        return out
    if not isinstance(reg, dict):
        return out
    for container_key in ("servers", "instances"):
        container = reg.get(container_key)
        if isinstance(container, dict):
            for k, v in container.items():
                if isinstance(v, dict):
                    out.append((str(k), v))
        elif isinstance(container, list):
            for idx, v in enumerate(container):
                if isinstance(v, dict):
                    out.append((str(idx), v))
    if not out:
        for k, v in reg.items():
            if isinstance(v, dict) and any(f in v for f in ("httpUrl", "port", "swarmId", "conversationId")):
                out.append((str(k), v))
    return out


def _discover_from_registry(
    *,
    swarm_id: str,
    conversation_id: str,
    board_dir: str,
    cwd: str = "",
) -> tuple[dict[str, Any] | None, Path | None]:
    state_dir = _find_registry_state_dir(board_dir)
    if state_dir is None:
        return None, None
    reg_file = state_dir / "registry.json"
    if not reg_file.is_file():
        return None, state_dir
    reg = _read_json_safe(reg_file, {})
    raw_entries = _collect_registry_entries(reg)
    if not raw_entries:
        return None, state_dir

    live_entries: list[dict[str, Any]] = []
    for key, entry in raw_entries:
        try:
            pid_val = int(entry.get("pid") or 0)
        except (TypeError, ValueError):
            pid_val = 0
        if pid_val <= 0:
            try:
                pid_val = int(key)
            except (TypeError, ValueError):
                pid_val = 0
        if pid_val > 0:
            if _pid_alive(pid_val):
                live_entries.append(entry)
        elif entry.get("httpUrl") or entry.get("port"):
            live_entries.append(entry)

    if not live_entries:
        return None, state_dir

    live_entries.sort(key=lambda e: str(e.get("updatedAt") or ""), reverse=True)
    clean_conv = conversation_id.strip()
    if clean_conv:
        for entry in live_entries:
            if clean_conv in _entry_conversation_ids(entry):
                return entry, state_dir

    clean_sid = _sanitize_swarm_id(swarm_id) if swarm_id.strip() else ""
    if clean_sid:
        for entry in live_entries:
            if _sanitize_swarm_id(entry.get("swarmId")) == clean_sid:
                return entry, state_dir

    if not clean_conv and not clean_sid:
        if len(live_entries) == 1:
            return live_entries[0], state_dir
        effective_cwd = cwd.strip() or os.getcwd()
        cwd_key = _norm_ws_key(effective_cwd)
        if cwd_key:
            ws_matches = [
                entry for entry in live_entries
                if entry.get("workspace") and _norm_ws_key(str(entry["workspace"])) == cwd_key
            ]
            if len(ws_matches) == 1:
                return ws_matches[0], state_dir

    return None, state_dir


def _read_text_shared(path: Path) -> str:
    """Read UTF-8 text from `path`, using Windows FILE_SHARE_DELETE/READ/WRITE flags when available."""
    if sys.platform == "win32" and _K32 is not None:
        generic_read = 0x80000000
        share_all = 0x00000001 | 0x00000002 | 0x00000004  # FILE_SHARE_READ | WRITE | DELETE
        open_existing = 3
        file_attr_normal = 0x80
        handle = _K32.CreateFileW(
            str(path),
            generic_read,
            share_all,
            None,
            open_existing,
            file_attr_normal,
            None,
        )
        if not handle or handle == _INVALID_HANDLE_VALUE:
            err = ctypes.get_last_error()
            if err in (2, 3):  # ERROR_FILE_NOT_FOUND / ERROR_PATH_NOT_FOUND
                raise FileNotFoundError(str(path))
            if err == 5:  # ERROR_ACCESS_DENIED
                raise PermissionError(f"Access denied reading {path}")
            raise ctypes.WinError(err)
        try:
            fd = msvcrt.open_osfhandle(handle, os.O_RDONLY)
        except Exception:
            _K32.CloseHandle(handle)
            raise
        with os.fdopen(fd, "r", encoding="utf-8", errors="replace") as fh:
            return fh.read()
    return path.read_text(encoding="utf-8", errors="replace")


def _read_token_file(path_str: str) -> str:
    if not path_str:
        return ""
    p = Path(path_str)
    if not p.is_file():
        return ""
    try:
        return _read_text_shared(p).strip()
    except OSError:
        return ""


def _resolve_config(
    *,
    swarm_id: str | None = None,
    agent: str | None = None,
    http_url: str | None = None,
    board_dir: str | None = None,
    token_file: str | None = None,
    conversation_id: str | None = None,
) -> dict[str, str]:
    turn_ctx = get_turn_context()
    explicit_token_file = str(token_file or "").strip() or turn_ctx.get("token_file", "")
    raw_swarm_id = (
        str(swarm_id or "").strip()
        or turn_ctx.get("swarm_id", "")
        or (os.environ.get("HERMES_SWARM_ID") or "").strip()
        or (os.environ.get("HERMES_A2A_SWARM_ID") or "").strip()
    )
    raw_agent = (
        str(agent or "").strip()
        or turn_ctx.get("agent", "")
        or (os.environ.get("HERMES_SWARM_AGENT") or "").strip()
        or (os.environ.get("HERMES_A2A_WORKER_NAME") or "").strip()
    )
    raw_role = (
        turn_ctx.get("role", "")
        or (os.environ.get("HERMES_SWARM_ROLE") or "").strip()
        or (os.environ.get("HERMES_A2A_WORKER_ROLE") or "").strip()
    )
    raw_http_url = (
        str(http_url or "").strip()
        or turn_ctx.get("http_url", "")
        or (os.environ.get("HERMES_SWARM_HTTP_URL") or "").strip()
        or (os.environ.get("HERMES_A2A_HTTP_URL") or "").strip()
    ).rstrip("/")
    raw_board_dir = (
        str(board_dir or "").strip()
        or turn_ctx.get("board_dir", "")
        or (os.environ.get("HERMES_SWARM_BOARD_DIR") or "").strip()
        or (os.environ.get("HERMES_A2A_SWARM_BOARD_DIR") or "").strip()
    )
    raw_conv_id = (
        str(conversation_id or "").strip()
        or turn_ctx.get("conversation_id", "")
        or (os.environ.get("HERMES_SWARM_CONVERSATION_ID") or "").strip()
        or (os.environ.get("HERMES_A2A_CONVERSATION_ID") or "").strip()
    )
    env_token_file = (
        (os.environ.get("HERMES_SWARM_TOKEN_FILE") or "").strip()
        or (os.environ.get("HERMES_A2A_TOKEN_FILE") or "").strip()
    )
    task_id = (
        turn_ctx.get("task_id", "")
        or (os.environ.get("HERMES_SWARM_TASK_ID") or "").strip()
        or (os.environ.get("HERMES_A2A_TASK_ID") or "").strip()
    )
    context_id = (
        turn_ctx.get("context_id", "")
        or (os.environ.get("HERMES_SWARM_CONTEXT_ID") or "").strip()
        or (os.environ.get("HERMES_A2A_CONTEXT_ID") or "").strip()
    )

    if not raw_swarm_id and raw_board_dir:
        bp = Path(raw_board_dir)
        if bp.parent.name == "swarms" or (bp / "board.json").is_file():
            raw_swarm_id = bp.name

    reg_entry, discovered_state_dir = _discover_from_registry(
        swarm_id=raw_swarm_id,
        conversation_id=raw_conv_id,
        board_dir=raw_board_dir,
        cwd=turn_ctx.get("cwd", ""),
    )
    if reg_entry is not None:
        if not raw_swarm_id and reg_entry.get("swarmId"):
            raw_swarm_id = str(reg_entry["swarmId"]).strip()
        if not raw_http_url and reg_entry.get("httpUrl"):
            raw_http_url = str(reg_entry["httpUrl"]).strip().rstrip("/")
        elif not raw_http_url and reg_entry.get("port"):
            raw_http_url = f"http://127.0.0.1:{int(reg_entry['port'])}"
        if not raw_conv_id:
            conv_candidate = (
                reg_entry.get("conversationId")
                or reg_entry.get("primaryConversationId")
                or reg_entry.get("coordinatorConversationId")
                or ""
            )
            if conv_candidate:
                raw_conv_id = str(conv_candidate).strip()
        if not raw_board_dir and raw_swarm_id and discovered_state_dir is not None:
            cand_bdir = discovered_state_dir / "swarms" / _sanitize_swarm_id(raw_swarm_id)
            if cand_bdir.is_dir():
                raw_board_dir = str(cand_bdir)

    discovered_token_file = ""
    if discovered_state_dir is not None and (discovered_state_dir / "internal_token").is_file():
        discovered_token_file = str(discovered_state_dir / "internal_token")
    elif raw_board_dir:
        bp = Path(raw_board_dir)
        if bp.parent.name == "swarms" and (bp.parent.parent / "internal_token").is_file():
            discovered_token_file = str(bp.parent.parent / "internal_token")

    resolved_token_file = explicit_token_file or env_token_file or discovered_token_file
    env_direct_token = (
        turn_ctx.get("http_token", "")
        or (os.environ.get("HERMES_SWARM_HTTP_TOKEN") or "").strip()
        or (os.environ.get("HERMES_A2A_HTTP_TOKEN") or "").strip()
    )
    token = ""
    has_env_token = False
    if explicit_token_file:
        token = _read_token_file(explicit_token_file)
    if not token and env_direct_token:
        token = env_direct_token
        has_env_token = True
    if not token and env_token_file:
        token = _read_token_file(env_token_file)
    if not token and discovered_token_file:
        token = _read_token_file(discovered_token_file)

    clean_swarm_id = _sanitize_swarm_id(raw_swarm_id) if raw_swarm_id else ""
    if not raw_role:
        raw_role = "coordinator" if raw_agent.lower() == "coordinator" else "worker"

    return {
        "swarm_id": clean_swarm_id,
        "agent": raw_agent,
        "role": raw_role,
        "http_url": raw_http_url,
        "token": token,
        "token_file": resolved_token_file,
        "has_env_token": "1" if has_env_token else "",
        "board_dir": raw_board_dir,
        "conversation_id": raw_conv_id,
        "task_id": task_id,
        "context_id": context_id,
    }


def _resolve_env_config() -> dict[str, str]:
    return _resolve_config()


def _require_config(
    *,
    swarm_id: str | None = None,
    agent: str | None = None,
    http_url: str | None = None,
    board_dir: str | None = None,
    token_file: str | None = None,
    conversation_id: str | None = None,
    require_agent: bool = False,
) -> tuple[dict[str, str], dict[str, Any] | None]:
    cfg = _resolve_config(
        swarm_id=swarm_id,
        agent=agent,
        http_url=http_url,
        board_dir=board_dir,
        token_file=token_file,
        conversation_id=conversation_id,
    )
    if not cfg["swarm_id"]:
        return cfg, {
            "ok": False,
            "error": "swarm_id is unknown in a non-swarm call; pass swarm_id=... (or set HERMES_SWARM_ID).",
        }
    if require_agent and not cfg["agent"]:
        return cfg, {
            "ok": False,
            "error": "agent is unknown in a non-swarm call; pass agent=... (or set HERMES_SWARM_AGENT).",
        }
    return cfg, None


def _resolve_board_paths(cfg: dict[str, str]) -> tuple[Path, Path] | None:
    bdir_raw = cfg.get("board_dir") or ""
    if bdir_raw:
        bdir = Path(bdir_raw)
        if (bdir / "swarms").is_dir() and not (bdir / "board.json").is_file():
            sdir = bdir / "swarms" / cfg["swarm_id"]
            return sdir / "board.json", sdir / "board.lock"
        return bdir / "board.json", bdir / "board.lock"

    state_dir = (os.environ.get("HERMES_A2A_STATE_DIR") or "").strip()
    if state_dir:
        sdir = Path(state_dir) / "swarms" / cfg["swarm_id"]
        return sdir / "board.json", sdir / "board.lock"
    return None


def _pid_alive(pid: int) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    if sys.platform == "win32" and _K32 is not None:
        try:
            handle = _K32.OpenProcess(0x1000, False, int(pid))
            if not handle or handle == _INVALID_HANDLE_VALUE:
                return ctypes.get_last_error() == 5
            try:
                code = wintypes.DWORD()
                ok = _K32.GetExitCodeProcess(handle, ctypes.byref(code))
                return bool(ok and code.value == 259)
            finally:
                _K32.CloseHandle(handle)
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _read_lock_holder(lock_path: Path) -> tuple[int | None, float | None, bool, str | None]:
    try:
        raw = _read_text_shared(lock_path).strip()
        if raw.startswith("{"):
            data = json.loads(raw)
            if isinstance(data, dict):
                pid_raw = data.get("pid")
                ts_raw = data.get("ts")
                released = bool(data.get("released", False))
                token_raw = data.get("token")
                return (
                    int(pid_raw) if pid_raw is not None else None,
                    float(ts_raw) if ts_raw is not None else None,
                    released,
                    str(token_raw) if token_raw is not None else None,
                )
    except Exception:
        pass
    try:
        return None, lock_path.stat().st_mtime, False, None
    except OSError:
        return None, None, False, None


def _unlink_with_retries(path: Path, attempts: int = 20) -> bool:
    for i in range(attempts):
        try:
            path.unlink(missing_ok=True)
            return True
        except PermissionError:
            time.sleep(min(0.05, 0.005 * (i + 1)))
        except OSError:
            return False
    return not path.exists()


def _reap_lock_file(lock_path: Path, expected_token: str | None) -> bool:
    """Re-verify holder token and rename dead/stale/released lock aside before unlinking."""
    _, _, _, cur_token = _read_lock_holder(lock_path)
    if cur_token != expected_token:
        return False
    reap_path = lock_path.with_name(f"{lock_path.name}.reap.{uuid.uuid4().hex[:8]}")
    try:
        os.replace(str(lock_path), str(reap_path))
    except FileNotFoundError:
        return True
    except OSError:
        return False
    _unlink_with_retries(reap_path, attempts=4)
    return True


@contextmanager
def _atomic_file_lock(lock_path: Path, timeout: float = 5.0, stale_seconds: float = 30.0) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    wait_timeout = max(0.05, float(timeout))
    deadline = time.monotonic() + wait_timeout
    box = _acquire_lock_box(lock_path)
    if not box.lock.acquire(timeout=wait_timeout):
        raise TimeoutError(f"Timed out acquiring lock {lock_path}")

    acquired = False
    lock_token = uuid.uuid4().hex
    attempt = 0
    try:
        while True:
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                try:
                    payload = json.dumps({
                        "pid": os.getpid(),
                        "ts": time.time(),
                        "token": lock_token,
                        "released": False,
                    }).encode("utf-8")
                    os.write(fd, payload)
                finally:
                    os.close(fd)
                acquired = True
                break
            except FileExistsError:
                now_mono = time.monotonic()
                pid_val, ts_val, released_flag, cur_token = _read_lock_holder(lock_path)
                if ts_val is None:
                    try:
                        ts_val = lock_path.stat().st_mtime
                    except OSError:
                        ts_val = None

                is_released = released_flag or pid_val == 0
                is_dead_pid = pid_val is not None and pid_val > 0 and not _pid_alive(pid_val)
                is_stale = ts_val is not None and (time.time() - ts_val) > stale_seconds
                if is_released or is_dead_pid or (is_stale and (pid_val is None or not _pid_alive(pid_val))):
                    if _reap_lock_file(lock_path, cur_token):
                        attempt = 0
                        continue

                if now_mono >= deadline:
                    raise TimeoutError(f"Timed out acquiring lock {lock_path}")
                remaining = deadline - now_mono
                base_sleep = min(0.05, 0.003 * (1.4 ** min(attempt, 10)))
                jittered = base_sleep * random.uniform(0.5, 1.5)
                attempt += 1
                time.sleep(min(remaining, max(0.002, jittered)))

        yield
    finally:
        try:
            if acquired:
                _, _, _, on_disk_token = _read_lock_holder(lock_path)
                if on_disk_token == lock_token:
                    removed = _unlink_with_retries(lock_path, attempts=20)
                    if not removed and lock_path.exists():
                        _, _, _, still_token = _read_lock_holder(lock_path)
                        if still_token == lock_token:
                            try:
                                lock_path.write_text(
                                    json.dumps({"pid": 0, "ts": 0.0, "token": lock_token, "released": True}),
                                    encoding="utf-8",
                                )
                            except OSError:
                                pass
        finally:
            box.lock.release()


def _atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex[:8]}")
    encoded = json.dumps(data, indent=2, ensure_ascii=False)
    tmp.write_text(encoded, encoding="utf-8")
    last_exc: Exception | None = None
    try:
        for attempt in range(8):
            try:
                os.replace(str(tmp), str(path))
                return
            except (PermissionError, OSError) as exc:
                last_exc = exc
                time.sleep(0.015 * (attempt + 1))
        if last_exc is not None:
            raise last_exc
    finally:
        if tmp.exists():
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass


def _read_json_safe(path: Path, default: Any = None) -> Any:
    if not path.is_file():
        return default

    for attempt in range(5):
        try:
            raw = _read_text_shared(path)
            if not raw.strip():
                return default
            return json.loads(raw)
        except FileNotFoundError:
            return default
        except (PermissionError, OSError, ValueError):
            if attempt < 4:
                time.sleep(0.01 * (attempt + 1))
    return default


def _state_key(swarm_id: str | None = None, agent: str | None = None) -> tuple[str, str]:
    sid = str(swarm_id or "").strip() or (os.environ.get("HERMES_SWARM_ID") or "").strip() or "default"
    ag = (
        str(agent or "").strip()
        or (os.environ.get("HERMES_SWARM_AGENT") or "").strip()
        or (os.environ.get("HERMES_A2A_WORKER_NAME") or "").strip()
        or "worker"
    )
    return (_sanitize_swarm_id(sid), ag.lower())


def record_bridge_state(
    cfg: Mapping[str, Any] | None = None,
    *,
    swarm_id: str | None = None,
    agent: str | None = None,
    channel: str | None = None,
    claims: list[str] | None = None,
    add_claim: str | None = None,
    remove_claim: str | None = None,
    clear_claims: bool = False,
    last_read_seq: int | None = None,
) -> None:
    """Record compact per-worker swarm state for compaction preservation and pre_verify checks."""
    eff_sid = swarm_id or (cfg.get("swarm_id") if cfg else None)
    eff_ag = agent or (cfg.get("agent") if cfg else None)
    key = _state_key(eff_sid, eff_ag)
    with _STATE_LOCK:
        entry = _BRIDGE_STATE.setdefault(
            key,
            {
                "swarm_id": key[0],
                "agent": str(eff_ag or (os.environ.get("HERMES_SWARM_AGENT") or "").strip() or "worker"),
                "channel": DEFAULT_CHANNEL,
                "claims": [],
                "lastReadSeq": 0,
            },
        )
        if eff_ag:
            entry["agent"] = str(eff_ag).strip()
        if channel is not None and str(channel).strip():
            ch_str = str(channel).strip()
            if ch_str != "*":
                entry["channel"] = _normalize_channel_name(ch_str)
        if clear_claims:
            entry["claims"] = []
        if claims is not None:
            entry["claims"] = sorted({str(c).strip() for c in claims if str(c).strip()})
        if add_claim and str(add_claim).strip():
            cur = set(entry["claims"])
            cur.add(str(add_claim).strip())
            entry["claims"] = sorted(cur)
        if remove_claim and str(remove_claim).strip():
            rem = str(remove_claim).strip()
            entry["claims"] = [c for c in entry["claims"] if c != rem]
        if last_read_seq is not None and int(last_read_seq) > 0:
            seq_int = int(last_read_seq)
            entry["lastReadSeq"] = max(int(entry.get("lastReadSeq") or 0), seq_int)
            _INBOX_SEEN_SEQ[key] = max(int(_INBOX_SEEN_SEQ.get(key) or 0), seq_int)


def get_bridge_briefing(swarm_id: str | None = None, agent: str | None = None) -> dict[str, Any]:
    """Return compact active swarm briefing summary for the current worker."""
    key = _state_key(swarm_id, agent)
    with _STATE_LOCK:
        entry = _BRIDGE_STATE.get(key)
        if entry is not None:
            return {
                "swarm_id": entry["swarm_id"],
                "agent": entry["agent"],
                "channel": entry["channel"],
                "claims": list(entry["claims"]),
                "lastReadSeq": int(entry.get("lastReadSeq") or 0),
            }
    return {
        "swarm_id": key[0],
        "agent": str(agent or (os.environ.get("HERMES_SWARM_AGENT") or "").strip() or "worker"),
        "channel": DEFAULT_CHANNEL,
        "claims": [],
        "lastReadSeq": 0,
    }


def get_unreleased_claims(swarm_id: str | None = None, agent: str | None = None) -> list[str]:
    """Return active unreleased scope claims tracked for the current worker."""
    return list(get_bridge_briefing(swarm_id=swarm_id, agent=agent).get("claims") or [])


def format_compaction_briefing(swarm_id: str | None = None, agent: str | None = None) -> str:
    """Format a compact 1-line swarm briefing summary for post-compaction injection."""
    b = get_bridge_briefing(swarm_id=swarm_id, agent=agent)
    claims_txt = ", ".join(b["claims"]) if b["claims"] else "none"
    return (
        f"[Swarm Bridge State] swarm_id={b['swarm_id']} agent={b['agent']} "
        f"channel={b['channel']} lastReadSeq={b['lastReadSeq']} active_claims=[{claims_txt}]. "
        "Continue using swarm_* tools (swarm_read since lastReadSeq, swarm_release when done editing)."
    )


def mark_compacted(session_id: str = "", **meta: Any) -> None:
    """Mark a session as recently compacted so the next pre_llm_call injects the swarm briefing."""
    sid = str(session_id or "").strip() or "__default__"
    with _STATE_LOCK:
        _COMPACTED_SESSIONS[sid] = time.time()
    try:
        cfg, err = _require_config(require_agent=False)
        if err is None:
            _log_bridge_event(
                cfg,
                "post_compaction",
                sessionId=sid,
                kind=meta.get("kind"),
                reason=meta.get("reason"),
                beforeTokens=meta.get("before_tokens"),
                afterTokens=meta.get("after_tokens"),
                messagesRemoved=meta.get("messages_removed"),
            )
    except Exception:
        pass


def consume_compacted_briefing(session_id: str = "") -> str | None:
    """If compaction occurred for this session (or default), return and clear the briefing reminder."""
    sid = str(session_id or "").strip() or "__default__"
    with _STATE_LOCK:
        ts = _COMPACTED_SESSIONS.pop(sid, None)
        if ts is None and sid != "__default__":
            ts = _COMPACTED_SESSIONS.pop("__default__", None)
    if ts is None:
        return None
    turn_ctx = get_turn_context()
    return format_compaction_briefing(
        swarm_id=turn_ctx.get("swarm_id"),
        agent=turn_ctx.get("agent"),
    )


def poll_pending_mentions_context(
    swarm_id: str | None = None,
    agent: str | None = None,
    *,
    max_chars: int = 2400,
) -> str | None:
    """Stat-cached non-blocking check for unread mid-turn @mentions/pendingMentions targeting this worker."""
    cfg, err = _require_config(swarm_id=swarm_id, agent=agent, require_agent=False)
    if err is not None:
        return None
    eff_agent = str(agent or cfg.get("agent") or "").strip()
    if not eff_agent:
        return None
    agent_low = eff_agent.lower()
    if agent_low in ("coordinator", "primary", "narrator", "monitor"):
        return None
    paths = _resolve_board_paths(cfg)
    if paths is None:
        return None
    bfile = paths[0]
    try:
        st = bfile.stat()
    except OSError:
        return None

    stat_sig = (int(st.st_mtime_ns), int(st.st_size), int(getattr(st, "st_ino", 0)))
    key = _state_key(cfg.get("swarm_id") or swarm_id, eff_agent)
    with _STATE_LOCK:
        if _INBOX_STAT_CACHE.get(key) == stat_sig:
            return None

    try:
        raw = _read_text_shared(bfile)
        board = json.loads(raw) if raw and raw.strip() else None
    except Exception:
        return None
    if not isinstance(board, dict):
        return None

    members = board.get("members") if isinstance(board.get("members"), dict) else {}
    member_rec: dict[str, Any] = {}
    if eff_agent in members and isinstance(members[eff_agent], dict):
        member_rec = members[eff_agent]
    else:
        for mk, mv in members.items():
            if str(mk).strip().lower() == agent_low and isinstance(mv, dict):
                member_rec = mv
                break

    with _STATE_LOCK:
        _INBOX_STAT_CACHE[key] = stat_sig
        seen_seq = max(
            int(_INBOX_SEEN_SEQ.get(key) or 0),
            int(member_rec.get("lastReadSeq") or 0),
            int((_BRIDGE_STATE.get(key) or {}).get("lastReadSeq") or 0),
        )

    candidates_by_seq: dict[int, dict[str, Any]] = {}
    pending_map = board.get("pendingMentions") if isinstance(board.get("pendingMentions"), dict) else {}
    pending_list: list[Any] = []
    for pk, pv in pending_map.items():
        if str(pk).strip().lower() == agent_low and isinstance(pv, list):
            pending_list.extend(pv)
    for item in pending_list:
        if not isinstance(item, dict):
            continue
        try:
            seq = int(item.get("seq") or 0)
        except (TypeError, ValueError):
            continue
        sender = str(item.get("sender") or "").strip()
        if seq <= seen_seq or sender.lower() == agent_low:
            continue
        candidates_by_seq[seq] = item

    for msg in board.get("messages") or []:
        if not isinstance(msg, dict):
            continue
        try:
            seq = int(msg.get("seq") or 0)
        except (TypeError, ValueError):
            continue
        if seq <= seen_seq or seq in candidates_by_seq:
            continue
        sender = str(msg.get("sender") or "").strip()
        if sender.lower() == agent_low:
            continue
        mentions_low = [str(m).strip().lstrip("@").lower() for m in (msg.get("mentions") or []) if str(m).strip()]
        if msg.get("all") is True or agent_low in mentions_low or "all" in mentions_low:
            candidates_by_seq[seq] = msg

    if not candidates_by_seq:
        return None

    unread = [candidates_by_seq[s] for s in sorted(candidates_by_seq)]
    max_new_seq = max(candidates_by_seq)
    with _STATE_LOCK:
        _INBOX_SEEN_SEQ[key] = max(int(_INBOX_SEEN_SEQ.get(key) or 0), max_new_seq)

    lines = [f"[URGENT SWARM BOARD MESSAGE(S) FOR @{eff_agent} — READ BEFORE YOUR NEXT TOOL CALL]"]
    for msg in unread:
        seq = int(msg.get("seq") or 0)
        sender = str(msg.get("sender") or "coordinator").strip()
        summary = str(msg.get("summary") or "").strip()
        details = str(msg.get("details") or "").strip()
        lines.append(f"#{seq} from @{sender}: {summary}")
        if details and details != summary:
            if len(details) > 900:
                details = details[:900].rstrip() + f"... (truncated — call swarm_read(seqs=[{seq}]) for full text)"
            lines.append(details)

    out = "\n".join(lines).strip()
    if len(out) > max_chars:
        out = out[: max(0, max_chars - 64)].rstrip() + f"\n... (truncated — call swarm_read(since={seen_seq}) for full details)"
    return out


def _append_archive_messages(archive_path: Path, messages: list[dict[str, Any]]) -> None:
    """Append overflow messages to board_archive.jsonl (best-effort, UTF-8)."""
    if not messages:
        return
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(m, ensure_ascii=False) + "\n" for m in messages if isinstance(m, dict)]
    if not lines:
        return
    with archive_path.open("a", encoding="utf-8") as fh:
        fh.writelines(lines)


def _read_archive_messages(archive_path: Path) -> list[dict[str, Any]]:
    """Read archived messages from board_archive.jsonl cleanly, skipping malformed lines."""
    if not archive_path.is_file():
        return []
    out: list[dict[str, Any]] = []
    try:
        with archive_path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        out.append(obj)
                except ValueError:
                    continue
    except OSError:
        return []
    return out


def _log_bridge_event(cfg: Mapping[str, Any] | None, event: str, **fields: Any) -> None:
    """Write a structured JSONL event to <swarm_dir>/logs/bridge.jsonl (best-effort, capped, non-fatal)."""
    try:
        if cfg is None:
            return
        paths = _resolve_board_paths(dict(cfg))
        if paths is None:
            return
        swarm_dir = paths[0].parent
        log_dir = swarm_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "bridge.jsonl"
        try:
            if log_file.is_file() and log_file.stat().st_size >= MAX_BRIDGE_LOG_BYTES:
                rotated = log_dir / "bridge.jsonl.1"
                os.replace(str(log_file), str(rotated))
        except OSError:
            pass
        record: dict[str, Any] = {
            "ts": _now_iso(),
            "event": event,
            "swarmId": cfg.get("swarm_id") or "",
            "agent": cfg.get("agent") or "",
        }
        for k, v in fields.items():
            if v is not None:
                record[k] = v
        with log_file.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass


def touch_heartbeat(
    cfg: Mapping[str, Any] | None = None,
    *,
    agent: str | None = None,
    force: bool = False,
) -> None:
    """Update lastHeartbeatAt on disk board.json (throttled, best-effort, non-fatal)."""
    try:
        eff_cfg = dict(cfg) if cfg is not None else None
        if eff_cfg is None:
            eff_cfg, err = _require_config(agent=agent, require_agent=True)
            if err is not None:
                return
        eff_agent = str(agent or eff_cfg.get("agent") or "").strip()
        if not eff_agent:
            return
        key = _state_key(eff_cfg.get("swarm_id"), eff_agent)
        now_mono = time.monotonic()
        with _STATE_LOCK:
            last = _HEARTBEAT_TIMESTAMPS.get(key, 0.0)
            if not force and (now_mono - last) < HEARTBEAT_THROTTLE_SECONDS:
                return
            _HEARTBEAT_TIMESTAMPS[key] = now_mono
        paths = _resolve_board_paths(eff_cfg)
        if paths is None:
            return
        bfile, block = paths
        if not bfile.is_file():
            return
        with _atomic_file_lock(block, timeout=1.5):
            board = _normalize_board(_read_json_safe(bfile, None), str(eff_cfg.get("swarm_id") or "default"))
            b_members = board.setdefault("members", {})
            mkey, mrec = _find_member_ci(b_members, eff_agent)
            if mkey is not None and mrec is not None:
                ts = _now_iso()
                mrec["lastHeartbeatAt"] = ts
                mrec["updatedAt"] = ts
                _atomic_write_json(bfile, board)
    except Exception:
        pass


def _default_board(swarm_id: str) -> dict[str, Any]:
    ts = _now_iso()
    return {
        "schemaVersion": SCHEMA_VERSION,
        "swarmId": swarm_id,
        "title": "Default Swarm" if swarm_id == "default" else swarm_id,
        "goal": "",
        "mission": "",
        "nextSeq": 1,
        "channels": {
            DEFAULT_CHANNEL: {
                "name": DEFAULT_CHANNEL,
                "description": "Primary swarm coordination channel",
                "topic": "Primary swarm coordination channel",
                "archived": False,
                "subscribers": [],
                "createdAt": ts,
                "updatedAt": ts,
            }
        },
        "members": {},
        "claims": {},
        "messages": [],
        "createdAt": ts,
        "updatedAt": ts,
    }


def _normalize_board(raw: Any, swarm_id: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return _default_board(swarm_id)
    board = dict(raw)
    board.setdefault("schemaVersion", SCHEMA_VERSION)
    board.setdefault("swarmId", swarm_id)
    board.setdefault("title", swarm_id)
    board.setdefault("nextSeq", 1)
    if not isinstance(board.get("channels"), dict):
        board["channels"] = _default_board(swarm_id)["channels"]
    if not isinstance(board.get("members"), dict):
        board["members"] = {}
    if not isinstance(board.get("claims"), dict):
        board["claims"] = {}
    if not isinstance(board.get("messages"), list):
        board["messages"] = []
    return board


def _head_seq(board: dict[str, Any]) -> int:
    msgs = board.get("messages") or []
    max_msg_seq = max((int(m.get("seq") or 0) for m in msgs if isinstance(m, dict)), default=0)
    next_seq = int(board.get("nextSeq") or 1)
    return max(max_msg_seq, max(0, next_seq - 1))


def _http_request_json(
    url: str,
    *,
    method: str = "GET",
    token: str = "",
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 4.0,
    token_file: str = "",
    has_env_token: bool = False,
    cfg: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Perform an HTTP JSON request, returning structured `{ok, status, data, error, transport_error}`."""
    def _do_once(auth_token: str) -> dict[str, Any]:
        req_headers: dict[str, str] = {"Accept": "application/json"}
        if headers:
            req_headers.update(headers)
        if auth_token:
            req_headers["Authorization"] = f"Bearer {auth_token}"
        data: bytes | None = None
        if payload is not None:
            req_headers["Content-Type"] = "application/json"
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                parsed = json.loads(raw) if raw.strip() else {}
                body = parsed if isinstance(parsed, dict) else {"result": parsed}
                return {
                    "ok": 200 <= resp.status < 300,
                    "status": int(resp.status),
                    "data": body,
                    "error": "",
                    "transport_error": False,
                }
        except urllib.error.HTTPError as exc:
            err_msg = f"HTTP {exc.code}"
            err_body: dict[str, Any] | None = None
            try:
                raw_err = exc.read().decode("utf-8", errors="replace")
                if raw_err.strip():
                    parsed_err = json.loads(raw_err)
                    if isinstance(parsed_err, dict):
                        err_body = parsed_err
                        if parsed_err.get("error"):
                            err_msg = str(parsed_err["error"])
            except Exception:
                pass
            finally:
                try:
                    exc.close()
                except Exception:
                    pass
            return {
                "ok": False,
                "status": int(exc.code),
                "data": err_body,
                "error": err_msg,
                "transport_error": False,
            }
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            return {
                "ok": False,
                "status": 0,
                "data": None,
                "error": str(exc),
                "transport_error": True,
            }

    res = _do_once(token)
    eff_token_file = token_file or (cfg.get("token_file", "") if cfg else "")
    eff_env_token = has_env_token or bool(cfg and cfg.get("has_env_token"))
    if res["status"] == 401 and eff_token_file:
        _log_bridge_event(
            cfg,
            "http_retry",
            url=url,
            method=method,
            status=401,
            reason="token_file_fallback" if eff_env_token else "token_refresh",
        )
        refreshed = _read_token_file(eff_token_file)
        if refreshed and refreshed != token:
            if cfg is not None:
                cfg["token"] = refreshed
            res = _do_once(refreshed)
    if not res.get("ok"):
        status_code = int(res.get("status") or 0)
        if status_code >= 400 or res.get("transport_error"):
            _log_bridge_event(
                cfg,
                "http_error",
                url=url,
                method=method,
                status=status_code,
                transportError=bool(res.get("transport_error")),
                error=str(res.get("error") or "")[:300],
            )
    return res


def _is_http_client_rejection(res: dict[str, Any]) -> bool:
    status = int(res.get("status") or 0)
    return (not res.get("ok")) and (not res.get("transport_error")) and 400 <= status < 500 and status != 404


def _fetch_http_snapshot(cfg: dict[str, str], *, etag: str = "", timeout: float = 4.0) -> dict[str, Any] | None:
    http_url = cfg.get("http_url") or ""
    conv_id = cfg.get("conversation_id") or ""
    if not http_url or not conv_id:
        return None
    query_params: dict[str, str] = {"conversationId": conv_id}
    if etag:
        query_params["etag"] = etag
    qs = urllib.parse.urlencode(query_params)
    url = f"{http_url}/board?{qs}"
    res = _http_request_json(
        url,
        method="GET",
        token=cfg.get("token") or "",
        headers={"x-conversation-id": conv_id},
        timeout=timeout,
        cfg=cfg,
    )
    if not res.get("ok"):
        return None
    snap = res.get("data")
    if not isinstance(snap, dict):
        return None
    if snap.get("unchanged"):
        return snap
    if not snap.get("swarm"):
        return None
    return snap


def _fetch_http_read(
    cfg: dict[str, str],
    *,
    since: int = 0,
    limit: int = 100,
    timeout: float = 2.5,
) -> dict[str, Any]:
    """Lightweight poll via GET /internal/board/read (avoids full /board observer snapshot)."""
    http_url = cfg.get("http_url") or ""
    swarm_id = cfg.get("swarm_id") or ""
    conv_id = cfg.get("conversation_id") or ""
    if not http_url or not (swarm_id or conv_id):
        return {"ok": False, "status": 0, "data": None}
    query_params: dict[str, str] = {"limit": str(max(1, min(500, int(limit))))}
    if swarm_id:
        query_params["swarmId"] = swarm_id
    if since > 0:
        query_params["since"] = str(int(since))
    if cfg.get("agent"):
        query_params["agent"] = cfg["agent"]
    if conv_id:
        query_params["conversationId"] = conv_id
    qs = urllib.parse.urlencode(query_params)
    headers = {"x-conversation-id": conv_id} if conv_id else None
    res = _http_request_json(
        f"{http_url}/internal/board/read?{qs}",
        method="GET",
        token=cfg.get("token") or "",
        headers=headers,
        timeout=timeout,
        cfg=cfg,
    )
    if not res.get("ok"):
        return {"ok": False, "status": int(res.get("status") or 0), "data": None}
    data = res.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("messages"), list):
        return {"ok": False, "status": int(res.get("status") or 0), "data": None}
    return {"ok": True, "status": int(res.get("status") or 200), "data": data}


def _fetch_http_status(cfg: dict[str, str], *, timeout: float = 2.5) -> dict[str, Any] | None:
    """Lightweight roster/channel metadata read via GET /internal/board/read?status=true."""
    http_url = cfg.get("http_url") or ""
    swarm_id = cfg.get("swarm_id") or ""
    conv_id = cfg.get("conversation_id") or ""
    if not http_url or not (swarm_id or conv_id):
        return None
    query_params: dict[str, str] = {"status": "true"}
    if swarm_id:
        query_params["swarmId"] = swarm_id
    if conv_id:
        query_params["conversationId"] = conv_id
    qs = urllib.parse.urlencode(query_params)
    headers = {"x-conversation-id": conv_id} if conv_id else None
    res = _http_request_json(
        f"{http_url}/internal/board/read?{qs}",
        method="GET",
        token=cfg.get("token") or "",
        headers=headers,
        timeout=timeout,
        cfg=cfg,
    )
    if not res.get("ok"):
        return None
    data = res.get("data")
    return data if isinstance(data, dict) else None


def _find_member_ci(members_map: dict[str, dict[str, Any]], name: str) -> tuple[str | None, dict[str, Any] | None]:
    clean = str(name or "").strip().lstrip("@")
    if not clean:
        return None, None
    if clean in members_map:
        return clean, members_map[clean]
    low = clean.lower()
    for mname, mrec in members_map.items():
        if mname.lower() == low:
            return mname, mrec
    return None, None


def _mention_matches_name(mention_tok: str, name_low: str) -> bool:
    ml = str(mention_tok or "").strip().lstrip("@").lower()
    if not ml or not name_low:
        return False
    if ml == "all" or ml == name_low:
        return True
    if "-" in name_low:
        prefix, _, suffix = name_low.rpartition("-")
        if suffix.isdigit() and ml == prefix:
            return True
    return False


def _sees_in(
    name: str,
    eff_chs: list[str],
    msg: dict[str, Any],
    channels_map: dict[str, dict[str, Any]] | None = None,
) -> bool:
    ch = _normalize_channel_name(msg.get("channel"))
    sender = str(msg.get("sender") or msg.get("agent") or "").strip()
    nlow = str(name or "").strip().lstrip("@").lower()
    if ch == DEFAULT_CHANNEL or (nlow and sender.lower() == nlow):
        return True
    if "*" in eff_chs or ch in eff_chs:
        return True
    raw_mentions = msg.get("mentions")
    if raw_mentions is None:
        raw_mentions = _extract_mentions(str(msg.get("summary") or msg.get("text") or ""))
    if any(_mention_matches_name(str(x), nlow) for x in (raw_mentions or [])):
        return True
    raw_ch_mentions = msg.get("channelMentions") or msg.get("channel_mentions")
    if raw_ch_mentions is None:
        raw_ch_mentions = _extract_channel_mentions(str(msg.get("summary") or msg.get("text") or ""))
    cmap = channels_map or {}
    for cm in raw_ch_mentions or []:
        norm_cm = _normalize_channel_name(str(cm))
        if norm_cm == DEFAULT_CHANNEL:
            continue
        crec = cmap.get(norm_cm)
        if isinstance(crec, dict) and crec.get("archived"):
            continue
        if "*" in eff_chs or norm_cm in eff_chs:
            return True
    return False


def _compute_visible_unread(
    name: str,
    member_rec: dict[str, Any] | None,
    msgs: list[dict[str, Any]],
    channels_map: dict[str, dict[str, Any]] | None = None,
) -> int:
    rec = member_rec or {}
    eff_chs = _effective_member_channels(rec.get("role"), rec.get("channels"), name)
    last_read = int(rec.get("lastReadSeq") or 0)
    count = 0
    for m in msgs:
        if not isinstance(m, dict) or isinstance(m.get("control"), dict):
            continue
        seq = int(m.get("seq") or 0)
        if seq > last_read and _sees_in(name, eff_chs, m, channels_map):
            count += 1
    return count


def _message_matches_target(
    msg: dict[str, Any],
    *,
    target: str,
    members_map: dict[str, dict[str, Any]],
    channels_map: dict[str, dict[str, Any]],
    mentions_only: bool = True,
) -> bool:
    if not isinstance(msg, dict) or isinstance(msg.get("control"), dict):
        return False
    if str(msg.get("senderRole") or "").lower() == "subagent" or int(msg.get("senderTier") or 2) == 3:
        return False
    sender = str(msg.get("sender") or msg.get("agent") or "").strip()
    sender_low = sender.lower()
    tlow = str(target or "coordinator").strip().lstrip("@").lower()
    if sender_low == tlow:
        return False
    if not mentions_only:
        _, mem = _find_member_ci(members_map, target)
        eff_chs = _effective_member_channels((mem or {}).get("role"), (mem or {}).get("channels"), target)
        return _sees_in(target, eff_chs, msg, channels_map)

    raw_mentions = [str(x).strip().lstrip("@").lower() for x in (msg.get("mentions") or []) if str(x).strip()]
    if msg.get("mentions") is None:
        for x in _extract_mentions(str(msg.get("summary") or msg.get("text") or "")):
            if x.lower() not in raw_mentions:
                raw_mentions.append(x.lower())
    if bool(msg.get("all")) or "all" in raw_mentions or tlow in raw_mentions:
        return True

    if "-" in tlow:
        prefix, suffix = tlow.rsplit("-", 1)
        if suffix.isdigit() and prefix in raw_mentions:
            return True

    if tlow == "coordinator":
        mtype = str(msg.get("type") or "").lower()
        raw_kind = str(msg.get("kind") or mtype).lower()
        summary_low = str(msg.get("summary") or msg.get("text") or "").strip().lower()
        if mtype in ("question", "escalation", "handoff"):
            return True
        if raw_kind in ("question", "escalation", "blocker", "handoff", "handoff_ready", "approval_request"):
            return True
        if summary_low.startswith(("[done]", "[stall]")):
            return True

    via_ch = [str(x).strip().lower() for x in (msg.get("viaChannel") or msg.get("via_channel") or []) if str(x).strip()]
    if tlow in via_ch:
        return True

    chan_mentions = [_normalize_channel_name(c) for c in (msg.get("channelMentions") or msg.get("channel_mentions") or [])]
    if chan_mentions:
        mem = members_map.get(target) or {}
        if not mem:
            for mname, mrec in members_map.items():
                if mname.lower() == tlow:
                    mem = mrec
                    break
        eff_chs = _effective_member_channels(mem.get("role"), mem.get("channels"), target)
        for ch in chan_mentions:
            ch_rec = channels_map.get(ch)
            if isinstance(ch_rec, dict) and ch_rec.get("archived"):
                continue
            if "*" in eff_chs or ch in eff_chs:
                return True

    return False


def _members_as_map(raw_members: Any) -> dict[str, dict[str, Any]]:
    if isinstance(raw_members, dict):
        return {str(k): dict(v) for k, v in raw_members.items() if isinstance(v, dict)}
    if isinstance(raw_members, list):
        out: dict[str, dict[str, Any]] = {}
        for item in raw_members:
            if isinstance(item, dict) and item.get("name"):
                out[str(item["name"])] = dict(item)
        return out
    return {}


def _channels_as_map(raw_channels: Any) -> dict[str, dict[str, Any]]:
    if isinstance(raw_channels, dict):
        return {_normalize_channel_name(k): dict(v) for k, v in raw_channels.items() if isinstance(v, dict)}
    if isinstance(raw_channels, list):
        out: dict[str, dict[str, Any]] = {}
        for item in raw_channels:
            if isinstance(item, dict) and item.get("name"):
                out[_normalize_channel_name(str(item["name"]))] = dict(item)
        return out
    return {}


def _normalize_files_arg(files: Any) -> list[str]:
    if not files:
        return []
    if isinstance(files, str):
        return [part.strip() for part in files.split(",") if part.strip()]
    if isinstance(files, list):
        out: list[str] = []
        for item in files:
            s = str(item or "").strip()
            if s and s not in out:
                out.append(s)
        return out
    return []


def swarm_post(
    summary: str,
    details: str = "",
    kind: str = "post",
    channel: str = "",
    files: list[str] | None = None,
    reply_to: int | str | None = None,
    to: str | list[str] | None = None,
    thread_id: str | None = None,
    *,
    swarm_id: str | None = None,
    agent: str | None = None,
    http_url: str | None = None,
    board_dir: str | None = None,
    token_file: str | None = None,
    conversation_id: str | None = None,
) -> dict[str, Any]:
    """Post a message to the Swarm Board via HTTP first, then atomic locked board.json fallback."""
    clean_summary = str(summary or "").strip()
    if not clean_summary:
        return {"ok": False, "error": "summary must be a non-empty string"}

    cfg, err = _require_config(
        swarm_id=swarm_id,
        agent=agent,
        http_url=http_url,
        board_dir=board_dir,
        token_file=token_file,
        conversation_id=conversation_id,
        require_agent=True,
    )
    if err is not None:
        return err

    norm_ch = _normalize_channel_name(channel or "main")
    disp_ch = _channel_display_name(norm_ch)
    clean_details = str(details or "").strip()
    clean_files = _normalize_files_arg(files)
    raw_kind = str(kind or "post").strip().lower() or "post"
    norm_type = _normalize_message_type(raw_kind)
    explicit_to: list[str] = []
    if isinstance(to, str):
        explicit_to = [x.strip() for x in to.split(",") if x.strip()]
    elif isinstance(to, list):
        explicit_to = [str(x).strip() for x in to if str(x).strip()]

    resolved_http_url = cfg["http_url"]
    if resolved_http_url:
        payload: dict[str, Any] = {
            "swarmId": cfg["swarm_id"],
            "sender": cfg["agent"],
            "senderRole": cfg["role"],
            "kind": raw_kind,
            "channel": disp_ch,
            "summary": clean_summary,
            "details": clean_details,
            "files": clean_files,
            "replyTo": reply_to,
            "threadId": thread_id,
            "to": explicit_to or None,
            "taskId": cfg["task_id"] or None,
            "contextId": cfg["context_id"] or None,
        }
        if cfg["conversation_id"]:
            payload["conversationId"] = cfg["conversation_id"]
        http_res = _http_request_json(
            f"{resolved_http_url}/internal/board/post",
            method="POST",
            token=cfg["token"],
            payload=payload,
            cfg=cfg,
        )
        http_data = http_res.get("data")
        if http_res.get("ok") and isinstance(http_data, dict) and http_data.get("seq") is not None:
            record_bridge_state(cfg, channel=norm_ch, last_read_seq=int(http_data.get("seq") or 0))
            return {"ok": True, "source": "http", "swarmId": cfg["swarm_id"], "message": http_data}
        if _is_http_client_rejection(http_res):
            return {"ok": False, "source": "http", "status": http_res["status"], "error": http_res["error"]}
        _log_bridge_event(cfg, "disk_fallback", tool="swarm_post", status=http_res.get("status"), error=http_res.get("error"))

    paths = _resolve_board_paths(cfg)
    if paths is None:
        return {
            "ok": False,
            "error": "Neither HTTP endpoint nor board_dir is reachable for swarm_post",
        }

    bfile, block = paths
    mentions = _extract_mentions(clean_summary, explicit_to=explicit_to)
    chan_mentions = _extract_channel_mentions(clean_summary, explicit_to=explicit_to)
    ts = _now_iso()

    with _atomic_file_lock(block):
        board = _normalize_board(_read_json_safe(bfile, None), cfg["swarm_id"])
        channels = board.setdefault("channels", {})
        existing_ch = channels.get(norm_ch)
        if isinstance(existing_ch, dict) and existing_ch.get("archived"):
            return {"ok": False, "error": f"Channel '{disp_ch}' is archived."}

        seq = max(int(board.get("nextSeq") or 1), _head_seq(board) + 1)
        board["nextSeq"] = seq + 1
        msgs = list(board.get("messages") or [])

        if norm_ch != DEFAULT_CHANNEL and norm_ch not in channels:
            channels[norm_ch] = {
                "name": norm_ch,
                "description": "",
                "topic": "",
                "archived": False,
                "subscribers": [],
                "createdAt": ts,
                "updatedAt": ts,
            }
            ctl_msg = {
                "seq": seq,
                "id": _new_id("bmsg-"),
                "threadId": None,
                "replyTo": None,
                "channel": norm_ch,
                "channels": [norm_ch],
                "type": "system",
                "sender": cfg["agent"],
                "senderRole": cfg["role"],
                "senderTier": 2,
                "mentions": [],
                "channelMentions": [],
                "summary": f"Created channel #{disp_ch}",
                "details": "",
                "files": [],
                "refs": [],
                "status": "open",
                "readBy": [cfg["agent"]],
                "control": {"op": "create", "channel": disp_ch, "topic": ""},
                "createdAt": ts,
                "updatedAt": ts,
            }
            msgs.append(ctl_msg)
            seq += 1
            board["nextSeq"] = seq + 1

        members = board.setdefault("members", {})
        agent_key, agent_rec = _find_member_ci(members, cfg["agent"])
        if agent_key is not None and agent_rec is not None:
            agent_rec["lastPostSeq"] = max(int(agent_rec.get("lastPostSeq") or 0), seq)
            prev_lr = int(agent_rec.get("lastReadSeq") or 0)
            eff_chs = _effective_member_channels(agent_rec.get("role"), agent_rec.get("channels"), agent_key)
            has_earlier_unread = any(
                isinstance(m, dict)
                and not isinstance(m.get("control"), dict)
                and prev_lr < int(m.get("seq") or 0) < seq
                and _sees_in(agent_key, eff_chs, m, channels)
                for m in msgs
            )
            if not has_earlier_unread:
                agent_rec["lastReadSeq"] = max(prev_lr, seq)
            agent_rec["lastHeartbeatAt"] = ts
            agent_rec["updatedAt"] = ts

        sender_tier = 1 if cfg["role"].lower() == "primary" else (3 if cfg["role"].lower() == "subagent" else 2)
        msg: dict[str, Any] = {
            "seq": seq,
            "id": _new_id("bmsg-"),
            "threadId": thread_id,
            "replyTo": reply_to,
            "channel": norm_ch,
            "channels": [norm_ch],
            "type": norm_type,
            "sender": cfg["agent"],
            "senderRole": cfg["role"],
            "senderTier": sender_tier,
            "mentions": mentions,
            "channelMentions": chan_mentions,
            "taskId": cfg["task_id"] or None,
            "contextId": cfg["context_id"] or None,
            "summary": clean_summary,
            "details": clean_details,
            "files": clean_files,
            "refs": [],
            "status": "open",
            "readBy": [cfg["agent"]],
            "createdAt": ts,
            "updatedAt": ts,
        }
        if raw_kind and (raw_kind != norm_type or raw_kind in ("approval_request", "handoff_ready", "blocker")):
            msg["kind"] = raw_kind

        msgs.append(msg)
        if len(msgs) > MAX_BOARD_MESSAGES:
            overflow = msgs[:-MAX_BOARD_MESSAGES]
            msgs = msgs[-MAX_BOARD_MESSAGES:]
            _append_archive_messages(bfile.parent / "board_archive.jsonl", overflow)
        board["messages"] = msgs
        board["schemaVersion"] = SCHEMA_VERSION
        board["swarmId"] = cfg["swarm_id"]
        board["updatedAt"] = ts
        _atomic_write_json(bfile, board)

    record_bridge_state(cfg, channel=norm_ch, last_read_seq=seq)
    return {"ok": True, "source": "disk", "swarmId": cfg["swarm_id"], "message": msg}


def swarm_read(
    since: int = 0,
    channel: str = "",
    limit: int = 100,
    seqs: list[int] | None = None,
    status: bool = False,
    mention: str | None = None,
    sender: str | None = None,
    kind: str | None = None,
    mark_read_by: str | None = None,
    include_archive: bool = False,
    before_seq: int | None = None,
    search: str | None = None,
    full: bool = False,
    *,
    swarm_id: str | None = None,
    agent: str | None = None,
    http_url: str | None = None,
    board_dir: str | None = None,
    token_file: str | None = None,
    conversation_id: str | None = None,
) -> dict[str, Any]:
    """Read messages from the Swarm Board via HTTP or locked/safe board.json fallback."""
    if status:
        return swarm_status(
            swarm_id=swarm_id,
            agent=agent,
            http_url=http_url,
            board_dir=board_dir,
            token_file=token_file,
            conversation_id=conversation_id,
        )

    cfg, err = _require_config(
        swarm_id=swarm_id,
        agent=agent,
        http_url=http_url,
        board_dir=board_dir,
        token_file=token_file,
        conversation_id=conversation_id,
        require_agent=False,
    )
    if err is not None:
        return err

    clamped = max(1, min(500, int(limit if limit is not None else 100)))
    before_seq_int = int(before_seq) if before_seq is not None and int(before_seq) > 0 else 0
    search_clean = str(search or "").strip()

    if cfg["http_url"]:
        http_payload: dict[str, Any] = {
            "swarmId": cfg["swarm_id"],
            "agent": cfg["agent"],
            "since": since if since else None,
            "seqs": seqs,
            "channel": channel or None,
            "limit": clamped,
            "mention": mention,
            "sender": sender,
            "kind": kind,
            "mark_read_by": mark_read_by,
            "include_archive": bool(include_archive),
            "before_seq": before_seq_int if before_seq_int > 0 else None,
            "search": search_clean or None,
            "full": bool(full),
        }
        if cfg["conversation_id"]:
            http_payload["conversationId"] = cfg["conversation_id"]
        http_res = _http_request_json(
            f"{cfg['http_url']}/internal/board/read",
            method="POST",
            token=cfg["token"],
            payload=http_payload,
            cfg=cfg,
        )
        http_data = http_res.get("data")
        if http_res.get("ok") and isinstance(http_data, dict) and http_data.get("ok") and isinstance(http_data.get("messages"), list):
            max_seen = max((int(m.get("seq") or 0) for m in http_data["messages"] if isinstance(m, dict)), default=0)
            record_bridge_state(cfg, channel=channel or None, last_read_seq=max_seen)
            return {"source": "http", **http_data}
        if _is_http_client_rejection(http_res) and http_res.get("status") != 404:
            return {"ok": False, "source": "http", "status": http_res["status"], "error": http_res["error"]}
        _log_bridge_event(cfg, "disk_fallback", tool="swarm_read", status=http_res.get("status"), error=http_res.get("error"))

    since_int = max(0, int(since or 0))
    ch_raw = str(channel or "").strip()
    is_all_channels = ch_raw == "*"
    norm_ch = _normalize_channel_name(ch_raw) if (ch_raw and not is_all_channels) else None
    seq_set = {int(s) for s in seqs if int(s) > 0} if seqs is not None else None
    mention_low = str(mention or "").strip().lstrip("@").lower()
    sender_low = str(sender or "").strip().lower()
    kind_low = _normalize_message_type(kind) if kind else None
    search_low = search_clean.lower()
    reader_agent = str(mark_read_by or agent or cfg.get("agent") or "").strip()

    source = "disk"
    raw_msgs: list[dict[str, Any]] = []
    members_map: dict[str, dict[str, Any]] = {}
    channels_map: dict[str, dict[str, Any]] = {}
    head = 0
    resolved_swarm_id = cfg["swarm_id"]

    snap = _fetch_http_snapshot(cfg)
    if snap is not None and isinstance(snap.get("messages"), list) and not mark_read_by and not include_archive:
        source = "http"
        resolved_swarm_id = str(snap.get("swarmId") or snap.get("swarm") or resolved_swarm_id)
        head = int(snap.get("head") or 0)
        raw_msgs = [m for m in snap["messages"] if isinstance(m, dict)]
        members_map = _members_as_map(snap.get("members"))
        channels_map = _channels_as_map(snap.get("channels"))
    else:
        paths = _resolve_board_paths(cfg)
        if paths is None:
            return {"ok": False, "error": "Neither HTTP /board nor board_dir is available"}
        bfile, block = paths
        if mark_read_by:
            with _atomic_file_lock(block):
                board = _normalize_board(_read_json_safe(bfile, None), resolved_swarm_id)
                head = _head_seq(board)
                raw_msgs = [m for m in (board.get("messages") or []) if isinstance(m, dict)]
                members_map = _members_as_map(board.get("members"))
                channels_map = _channels_as_map(board.get("channels"))
        else:
            board = _normalize_board(_read_json_safe(bfile, None), resolved_swarm_id)
            head = _head_seq(board)
            raw_msgs = [m for m in (board.get("messages") or []) if isinstance(m, dict)]
            members_map = _members_as_map(board.get("members"))
            channels_map = _channels_as_map(board.get("channels"))
        if include_archive:
            archived_msgs = _read_archive_messages(bfile.parent / "board_archive.jsonl")
            if archived_msgs:
                by_seq: dict[int, dict[str, Any]] = {}
                for am in archived_msgs:
                    aseq = int(am.get("seq") or 0)
                    if aseq > 0:
                        by_seq[aseq] = am
                for lm in raw_msgs:
                    lseq = int(lm.get("seq") or 0)
                    if lseq > 0:
                        by_seq[lseq] = lm
                raw_msgs = list(by_seq.values())

    raw_msgs.sort(key=lambda m: int(m.get("seq") or 0))
    if seq_set is not None:
        selected = [m for m in raw_msgs if int(m.get("seq") or 0) in seq_set]
    else:
        filtered = raw_msgs
        if since_int > 0:
            filtered = [m for m in filtered if int(m.get("seq") or 0) > since_int]
        if before_seq_int > 0:
            filtered = [m for m in filtered if int(m.get("seq") or 0) < before_seq_int]
        if norm_ch:
            filtered = [m for m in filtered if _normalize_channel_name(m.get("channel")) == norm_ch]
        elif not is_all_channels and reader_agent and reader_agent in members_map:
            _, mrec = _find_member_ci(members_map, reader_agent)
            eff_chs = _effective_member_channels((mrec or {}).get("role"), (mrec or {}).get("channels"), reader_agent)
            filtered = [m for m in filtered if _sees_in(reader_agent, eff_chs, m, channels_map)]
        if sender_low:
            filtered = [m for m in filtered if str(m.get("sender") or m.get("agent") or "").lower() == sender_low]
        if kind_low:
            filtered = [m for m in filtered if _normalize_message_type(m.get("type") or m.get("kind")) == kind_low]
        if mention_low:
            filtered = [
                m for m in filtered
                if any(
                    _mention_matches_name(str(x), mention_low) or _mention_matches_name(mention_low, str(x).lower())
                    for x in (m.get("mentions") or [])
                )
            ]
        if search_low:
            def _matches_search(msg_obj: dict[str, Any]) -> bool:
                haystack = " ".join(
                    [
                        str(msg_obj.get("summary") or ""),
                        str(msg_obj.get("details") or ""),
                        str(msg_obj.get("sender") or msg_obj.get("agent") or ""),
                        str(msg_obj.get("channel") or ""),
                        " ".join(str(f) for f in (msg_obj.get("files") or [])),
                    ]
                ).lower()
                return search_low in haystack

            filtered = [m for m in filtered if _matches_search(m)]
        selected = filtered[:clamped] if since_int > 0 else filtered[-clamped:]

    max_seen = max((int(m.get("seq") or 0) for m in selected), default=0)
    if mark_read_by and source == "disk":
        paths = _resolve_board_paths(cfg)
        if paths is not None:
            bfile, block = paths
            selected_seqs = {int(m.get("seq") or 0) for m in selected}
            with _atomic_file_lock(block):
                board = _normalize_board(_read_json_safe(bfile, None), resolved_swarm_id)
                b_members = board.setdefault("members", {})
                rkey, rrec = _find_member_ci(b_members, str(mark_read_by))
                if rkey is not None and rrec is not None:
                    ts = _now_iso()
                    rrec["lastReadSeq"] = max(int(rrec.get("lastReadSeq") or 0), max_seen)
                    rrec["lastHeartbeatAt"] = ts
                    rrec["updatedAt"] = ts
                for m in board.get("messages") or []:
                    if isinstance(m, dict) and int(m.get("seq") or 0) in selected_seqs:
                        rb = m.setdefault("readBy", [])
                        if str(mark_read_by) not in rb:
                            rb.append(str(mark_read_by))
                _atomic_write_json(bfile, board)
    elif source == "disk" and reader_agent:
        touch_heartbeat(cfg, agent=reader_agent)

    record_bridge_state(cfg, channel=norm_ch, last_read_seq=max_seen)

    return {
        "ok": True,
        "source": source,
        "swarmId": resolved_swarm_id,
        "head": head,
        "count": len(selected),
        "messages": selected,
    }


def swarm_claim(
    scope: str,
    notes: str = "",
    *,
    swarm_id: str | None = None,
    agent: str | None = None,
    http_url: str | None = None,
    board_dir: str | None = None,
    token_file: str | None = None,
    conversation_id: str | None = None,
) -> dict[str, Any]:
    """Claim an editing scope or path lock for this worker."""
    clean_scope = str(scope or "").strip()
    if not clean_scope:
        return {"ok": False, "error": "scope must be a non-empty string"}

    cfg, err = _require_config(
        swarm_id=swarm_id,
        agent=agent,
        http_url=http_url,
        board_dir=board_dir,
        token_file=token_file,
        conversation_id=conversation_id,
        require_agent=True,
    )
    if err is not None:
        return err

    clean_notes = str(notes or "").strip()

    if cfg["http_url"]:
        payload: dict[str, Any] = {
            "swarmId": cfg["swarm_id"],
            "scope": clean_scope,
            "holder": cfg["agent"],
            "notes": clean_notes,
            "taskId": cfg["task_id"] or None,
        }
        if cfg["conversation_id"]:
            payload["conversationId"] = cfg["conversation_id"]
        http_res = _http_request_json(
            f"{cfg['http_url']}/internal/board/claim",
            method="POST",
            token=cfg["token"],
            payload=payload,
            cfg=cfg,
        )
        http_data = http_res.get("data")
        if http_res.get("ok") and isinstance(http_data, dict) and ("claimed" in http_data or http_data.get("ok")):
            record_bridge_state(cfg, add_claim=clean_scope)
            return {"ok": True, "source": "http", **http_data}
        if _is_http_client_rejection(http_res):
            return {"ok": False, "source": "http", "status": http_res["status"], "error": http_res["error"]}
        _log_bridge_event(cfg, "disk_fallback", tool="swarm_claim", status=http_res.get("status"), error=http_res.get("error"))

    paths = _resolve_board_paths(cfg)
    if paths is None:
        return {"ok": False, "error": "board_dir is required for disk scope claim"}

    bfile, block = paths
    ts = _now_iso()
    conflict: dict[str, Any] | None = None
    entry: dict[str, Any] = {}
    with _atomic_file_lock(block):
        board = _normalize_board(_read_json_safe(bfile, None), cfg["swarm_id"])
        claims = board.setdefault("claims", {})
        prev = claims.get(clean_scope)
        if isinstance(prev, dict) and prev.get("holder") != cfg["agent"]:
            conflict = dict(prev)
        entry = {
            "scope": clean_scope,
            "holder": cfg["agent"],
            "taskId": cfg["task_id"] or None,
            "worktreePath": None,
            "notes": clean_notes,
            "claimedAt": ts,
        }
        claims[clean_scope] = entry
        board["schemaVersion"] = SCHEMA_VERSION
        board["swarmId"] = cfg["swarm_id"]
        board["updatedAt"] = ts
        _atomic_write_json(bfile, board)

    record_bridge_state(cfg, add_claim=clean_scope)
    return {
        "ok": True,
        "source": "disk",
        "swarmId": cfg["swarm_id"],
        "claimed": entry,
        "conflictWarning": conflict,
    }


def swarm_release(
    scope: str = "",
    *,
    swarm_id: str | None = None,
    agent: str | None = None,
    http_url: str | None = None,
    board_dir: str | None = None,
    token_file: str | None = None,
    conversation_id: str | None = None,
) -> dict[str, Any]:
    """Release one editing scope or all scopes held by this worker."""
    clean_scope = str(scope or "").strip() or None
    cfg, err = _require_config(
        swarm_id=swarm_id,
        agent=agent,
        http_url=http_url,
        board_dir=board_dir,
        token_file=token_file,
        conversation_id=conversation_id,
        require_agent=True,
    )
    if err is not None:
        return err

    if cfg["http_url"]:
        payload: dict[str, Any] = {
            "swarmId": cfg["swarm_id"],
            "scope": clean_scope,
            "holder": cfg["agent"],
        }
        if cfg["conversation_id"]:
            payload["conversationId"] = cfg["conversation_id"]
        http_res = _http_request_json(
            f"{cfg['http_url']}/internal/board/release",
            method="POST",
            token=cfg["token"],
            payload=payload,
            cfg=cfg,
        )
        http_data = http_res.get("data")
        if http_res.get("ok") and isinstance(http_data, dict) and ("released" in http_data or http_data.get("ok")):
            if clean_scope:
                record_bridge_state(cfg, remove_claim=clean_scope)
            else:
                record_bridge_state(cfg, clear_claims=True)
            return {"ok": True, "source": "http", **http_data}
        if _is_http_client_rejection(http_res):
            return {"ok": False, "source": "http", "status": http_res["status"], "error": http_res["error"]}
        _log_bridge_event(cfg, "disk_fallback", tool="swarm_release", status=http_res.get("status"), error=http_res.get("error"))

    paths = _resolve_board_paths(cfg)
    if paths is None:
        return {"ok": False, "error": "board_dir is required for disk scope release"}

    bfile, block = paths
    removed: list[str] = []
    ts = _now_iso()
    with _atomic_file_lock(block):
        board = _normalize_board(_read_json_safe(bfile, None), cfg["swarm_id"])
        claims = board.setdefault("claims", {})
        if clean_scope:
            cur = claims.get(clean_scope)
            if isinstance(cur, dict) and cur.get("holder") == cfg["agent"]:
                claims.pop(clean_scope, None)
                removed.append(clean_scope)
        else:
            for key in [k for k, v in claims.items() if isinstance(v, dict) and v.get("holder") == cfg["agent"]]:
                claims.pop(key, None)
                removed.append(key)
        if removed:
            board["schemaVersion"] = SCHEMA_VERSION
            board["swarmId"] = cfg["swarm_id"]
            board["updatedAt"] = ts
            _atomic_write_json(bfile, board)

    if clean_scope:
        record_bridge_state(cfg, remove_claim=clean_scope)
    else:
        record_bridge_state(cfg, clear_claims=True)

    return {
        "ok": True,
        "source": "disk",
        "swarmId": cfg["swarm_id"],
        "released": removed,
    }


def _summarize_member(
    name: str,
    mem: dict[str, Any],
    head: int,
    *,
    msgs: list[dict[str, Any]] | None = None,
    channels_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    state = str(mem.get("status") or mem.get("state") or "waiting")
    last_read = int(mem.get("lastReadSeq") or 0)
    if mem.get("visibleUnread") is not None:
        vis_unread = int(mem["visibleUnread"])
    elif msgs is not None:
        vis_unread = _compute_visible_unread(name, mem, msgs, channels_map)
    else:
        vis_unread = max(0, head - last_read)
    out: dict[str, Any] = {
        "name": name,
        "role": str(mem.get("role") or "worker"),
        "state": state,
        "killed": bool(mem.get("killed", False)) or state == "killed",
        "assignment": str(mem.get("assignment") or ""),
        "lastReadSeq": last_read,
        "lastPostSeq": int(mem.get("lastPostSeq") or 0),
        "unread": vis_unread,
        "visibleUnread": vis_unread,
    }
    for key in ("steps", "toolCalls", "currentTool", "tokens", "tps", "costUsd"):
        if key in mem and mem[key] is not None:
            out[key] = mem[key]
    return out


def swarm_status(
    *,
    swarm_id: str | None = None,
    agent: str | None = None,
    http_url: str | None = None,
    board_dir: str | None = None,
    token_file: str | None = None,
    conversation_id: str | None = None,
) -> dict[str, Any]:
    """Inspect current swarm roster, head seq, claims, channels, and telemetry summary."""
    cfg, err = _require_config(
        swarm_id=swarm_id,
        agent=agent,
        http_url=http_url,
        board_dir=board_dir,
        token_file=token_file,
        conversation_id=conversation_id,
        require_agent=False,
    )
    if err is not None:
        return err

    snap = _fetch_http_snapshot(cfg)
    if snap is not None:
        resolved_swarm_id = str(snap.get("swarmId") or snap.get("swarm") or cfg["swarm_id"])
        head = int(snap.get("head") or 0)
        members_map = _members_as_map(snap.get("members"))
        snap_msgs = [m for m in (snap.get("messages") or []) if isinstance(m, dict)]
        snap_ch_map = _channels_as_map(snap.get("channels"))
        roster = [_summarize_member(n, m, head, msgs=snap_msgs, channels_map=snap_ch_map) for n, m in sorted(members_map.items())]
        telemetry = snap.get("telemetry")
        if not isinstance(telemetry, dict):
            telemetry = {
                "activeWorkers": sum(1 for r in roster if not r["killed"] and r["state"] == "working" and r["role"] != "coordinator"),
                "waitingWorkers": sum(1 for r in roster if not r["killed"] and r["state"] != "working" and r["role"] != "coordinator"),
                "killedWorkers": sum(1 for r in roster if r["killed"]),
            }
        http_channels_map = _channels_as_map(snap.get("channels"))
        http_channels = [_channel_display_name(k) for k in sorted(http_channels_map.keys())]
        return {
            "ok": True,
            "source": "http",
            "swarmId": resolved_swarm_id,
            "title": str(snap.get("title") or resolved_swarm_id),
            "head": head,
            "agent": cfg["agent"],
            "role": cfg["role"],
            "members": roster,
            "claims": dict(snap.get("claims") or {}),
            "channels": http_channels,
            "telemetry": telemetry,
        }

    paths = _resolve_board_paths(cfg)
    if paths is None:
        return {"ok": False, "error": "Neither HTTP /board nor board_dir is available"}
    bfile, _ = paths
    board = _normalize_board(_read_json_safe(bfile, None), cfg["swarm_id"])
    head = _head_seq(board)
    members_map = _members_as_map(board.get("members"))
    board_msgs = [m for m in (board.get("messages") or []) if isinstance(m, dict)]
    board_ch_map = _channels_as_map(board.get("channels"))
    roster = [_summarize_member(n, m, head, msgs=board_msgs, channels_map=board_ch_map) for n, m in sorted(members_map.items())]
    channels = [_channel_display_name(k) for k in sorted((board.get("channels") or {}).keys())]
    telemetry = {
        "activeWorkers": sum(1 for r in roster if not r["killed"] and r["state"] == "working" and r["role"] != "coordinator"),
        "waitingWorkers": sum(1 for r in roster if not r["killed"] and r["state"] != "working" and r["role"] != "coordinator"),
        "killedWorkers": sum(1 for r in roster if r["killed"]),
    }
    return {
        "ok": True,
        "source": "disk",
        "swarmId": cfg["swarm_id"],
        "title": str(board.get("title") or cfg["swarm_id"]),
        "head": head,
        "agent": cfg["agent"],
        "role": cfg["role"],
        "members": roster,
        "claims": dict(board.get("claims") or {}),
        "channels": channels,
        "pendingMentions": {k: len(v) for k, v in (board.get("pendingMentions") or {}).items() if v},
        "telemetry": telemetry,
        "updatedAt": board.get("updatedAt"),
    }


def swarm_wait(
    since: int = 0,
    mentions_only: bool = True,
    timeout_seconds: float = 120.0,
    *,
    swarm_id: str | None = None,
    agent: str | None = None,
    http_url: str | None = None,
    board_dir: str | None = None,
    token_file: str | None = None,
    conversation_id: str | None = None,
) -> dict[str, Any]:
    """Pure in-process wait for new Swarm Board activity after `since` (zero child shells)."""
    cfg, err = _require_config(
        swarm_id=swarm_id,
        agent=agent,
        http_url=http_url,
        board_dir=board_dir,
        token_file=token_file,
        conversation_id=conversation_id,
        require_agent=True,
    )
    if err is not None:
        return err

    since_int = max(0, int(since or 0))
    timeout = max(0.05, min(600.0, float(timeout_seconds if timeout_seconds is not None else 120.0)))
    deadline = time.monotonic() + timeout
    target = cfg["agent"]
    paths = _resolve_board_paths(cfg)
    bfile = paths[0] if paths is not None else None
    stop_event = threading.Event()

    def _check_disk() -> tuple[int, list[dict[str, Any]]] | None:
        if bfile is None or not bfile.is_file():
            return None
        board = _normalize_board(_read_json_safe(bfile, None), cfg["swarm_id"])
        head = _head_seq(board)
        members_map = _members_as_map(board.get("members"))
        channels_map = _channels_as_map(board.get("channels"))
        msgs = [
            m for m in (board.get("messages") or [])
            if isinstance(m, dict) and int(m.get("seq") or 0) > since_int
        ]
        msgs.sort(key=lambda m: int(m.get("seq") or 0))
        matched = [
            m for m in msgs
            if _message_matches_target(
                m,
                target=target,
                members_map=members_map,
                channels_map=channels_map,
                mentions_only=bool(mentions_only),
            )
        ]
        return head, matched

    last_etag = ""
    last_http_head = 0
    cached_members_map: dict[str, dict[str, Any]] = {}
    cached_channels_map: dict[str, dict[str, Any]] = {}
    status_fetched = False
    last_meta_refresh_at = 0.0
    use_snapshot_fallback = False

    def _ensure_http_channel_metadata(msgs: list[dict[str, Any]]) -> None:
        nonlocal cached_members_map, cached_channels_map, status_fetched, last_meta_refresh_at
        now_mono = time.monotonic()
        if status_fetched and (now_mono - last_meta_refresh_at) < CHANNEL_META_REFRESH_SECONDS:
            return
        needs_meta = (not mentions_only) or any(
            m.get("channelMentions") or m.get("channel_mentions")
            for m in msgs
        )
        if not needs_meta:
            return
        if bfile is not None and bfile.is_file():
            board = _normalize_board(_read_json_safe(bfile, None), cfg["swarm_id"])
            cached_members_map = _members_as_map(board.get("members"))
            cached_channels_map = _channels_as_map(board.get("channels"))
            status_fetched = True
            last_meta_refresh_at = now_mono
            return
        st_data = _fetch_http_status(cfg, timeout=2.5)
        if st_data is not None:
            cached_members_map = _members_as_map(st_data.get("members"))
            cached_channels_map = _channels_as_map(st_data.get("channels"))
            status_fetched = True
            last_meta_refresh_at = time.monotonic()

    def _check_http() -> tuple[int, list[dict[str, Any]]] | None:
        nonlocal last_etag, last_http_head, cached_members_map, cached_channels_map, status_fetched, last_meta_refresh_at, use_snapshot_fallback
        if not use_snapshot_fallback:
            read_res = _fetch_http_read(cfg, since=since_int, limit=100, timeout=2.5)
            if read_res["ok"]:
                snap = read_res["data"]
                last_http_head = int(snap.get("head") or 0)
                if "members" in snap:
                    cached_members_map = _members_as_map(snap.get("members"))
                if "channels" in snap:
                    cached_channels_map = _channels_as_map(snap.get("channels"))
                if "members" in snap and "channels" in snap:
                    status_fetched = True
                    last_meta_refresh_at = time.monotonic()
                msgs = [
                    m for m in (snap.get("messages") or [])
                    if isinstance(m, dict) and int(m.get("seq") or 0) > since_int
                ]
                if not msgs:
                    return last_http_head, []
                _ensure_http_channel_metadata(msgs)
                msgs.sort(key=lambda m: int(m.get("seq") or 0))
                matched = [
                    m for m in msgs
                    if _message_matches_target(
                        m,
                        target=target,
                        members_map=cached_members_map,
                        channels_map=cached_channels_map,
                        mentions_only=bool(mentions_only),
                    )
                ]
                return last_http_head, matched
            if read_res["status"] in (401, 403, 404, 405) and cfg.get("conversation_id"):
                use_snapshot_fallback = True
            else:
                return None

        snap = _fetch_http_snapshot(cfg, etag=last_etag, timeout=2.5)
        if snap is None:
            return None
        if snap.get("etag"):
            last_etag = str(snap["etag"])
        if snap.get("unchanged"):
            return last_http_head, []
        last_http_head = int(snap.get("head") or 0)
        members_map = _members_as_map(snap.get("members"))
        channels_map = _channels_as_map(snap.get("channels"))
        msgs = [
            m for m in (snap.get("messages") or [])
            if isinstance(m, dict) and int(m.get("seq") or 0) > since_int
        ]
        msgs.sort(key=lambda m: int(m.get("seq") or 0))
        matched = [
            m for m in msgs
            if _message_matches_target(
                m,
                target=target,
                members_map=members_map,
                channels_map=channels_map,
                mentions_only=bool(mentions_only),
            )
        ]
        return last_http_head, matched

    has_http = bool(cfg["http_url"] and (cfg["conversation_id"] or cfg["swarm_id"]))
    if bfile is None and not has_http:
        return {"ok": False, "error": "Neither board_dir nor HTTP /board is configured"}

    is_coordinator = target.strip().lower() == "coordinator" or (cfg.get("role") or "").strip().lower() == "coordinator"
    wait_ref_key = (cfg["swarm_id"], target.lower())
    wait_entered = False
    wait_exited = False

    def _set_disk_wait(waiting: bool, *, last_read_seq: int | None = None) -> None:
        should_write_wait = False
        if is_coordinator:
            with _WAIT_REF_LOCK:
                cur_count = _WAIT_REFCOUNTS.get(wait_ref_key, 0)
                if waiting:
                    _WAIT_REFCOUNTS[wait_ref_key] = cur_count + 1
                    should_write_wait = True
                elif wait_entered:
                    new_count = max(0, cur_count - 1)
                    if new_count == 0:
                        _WAIT_REFCOUNTS.pop(wait_ref_key, None)
                        should_write_wait = True
                    else:
                        _WAIT_REFCOUNTS[wait_ref_key] = new_count
                        should_write_wait = False
        elif waiting:
            # Non-coordinator workers watch board.json directly; skip waitSince disk writes on entry.
            return

        if paths is None:
            return
        bpath, lpath = paths
        if not bpath.is_file():
            return
        if not waiting and not should_write_wait and (last_read_seq is None or int(last_read_seq) <= 0):
            return
        try:
            with _atomic_file_lock(lpath, timeout=2.0):
                board = _normalize_board(_read_json_safe(bpath, None), cfg["swarm_id"])
                b_members = board.setdefault("members", {})
                mkey, mrec = _find_member_ci(b_members, target)
                if mkey is not None and mrec is not None:
                    changed = False
                    is_killed = bool(mrec.get("killed")) or str(mrec.get("state") or "").strip().lower() == "killed"
                    if is_killed:
                        if mrec.get("waitSince") is not None:
                            mrec["waitSince"] = None
                            changed = True
                        if mrec.get("waiting"):
                            mrec["waiting"] = False
                            changed = True
                    elif waiting:
                        desired_wait = str(since_int)
                        if mrec.get("waitSince") != desired_wait:
                            mrec["waitSince"] = desired_wait
                            changed = True
                        if mrec.get("state") != "waiting":
                            mrec["state"] = "waiting"
                            changed = True
                        if mrec.get("waiting") is not True:
                            mrec["waiting"] = True
                            changed = True
                    elif should_write_wait:
                        if mrec.get("waitSince") is not None:
                            mrec["waitSince"] = None
                            changed = True
                        if "waiting" in mrec and mrec.get("waiting") is not False:
                            mrec["waiting"] = False
                            changed = True
                        if mrec.get("state") == "waiting":
                            mrec["state"] = "working"
                            changed = True
                    if last_read_seq is not None and int(last_read_seq) > 0:
                        new_lr = int(last_read_seq)
                        cur_lr = int(mrec.get("lastReadSeq") or 0)
                        if new_lr > cur_lr:
                            mrec["lastReadSeq"] = new_lr
                            changed = True
                    if changed:
                        ts = _now_iso()
                        mrec["lastHeartbeatAt"] = ts
                        mrec["updatedAt"] = ts
                        _atomic_write_json(bpath, board)
        except Exception:
            pass

    def _finish_wake(source_name: str, head_val: int, matched_msgs: list[dict[str, Any]]) -> dict[str, Any]:
        nonlocal wait_exited
        max_matched = max((int(m.get("seq") or 0) for m in matched_msgs), default=since_int)
        wait_exited = True
        if wait_entered or max_matched > 0:
            _set_disk_wait(False, last_read_seq=max_matched if max_matched > 0 else None)
        record_bridge_state(cfg, last_read_seq=max_matched)
        return {
            "ok": True,
            "source": source_name,
            "woken": True,
            "timedOut": False,
            "swarmId": cfg["swarm_id"],
            "head": head_val,
            "messages": matched_msgs,
        }

    try:
        last_sig: tuple[int, int] | None = None
        last_head = 0
        has_local_board = bool(bfile is not None and bfile.is_file())
        http_poll_interval = 1.5 if has_local_board else 0.4
        http_backoff = http_poll_interval
        next_http_poll_at = 0.0

        if has_local_board and bfile is not None:
            try:
                st = bfile.stat()
                last_sig = (st.st_mtime_ns, st.st_size)
            except OSError:
                pass
            disk_res = _check_disk()
            if disk_res is not None:
                last_head, matched = disk_res
                if matched:
                    return _finish_wake("disk", last_head, matched)
        if has_http:
            http_res = _check_http()
            if http_res is not None:
                http_backoff = http_poll_interval
                last_head, matched = http_res
                if matched:
                    return _finish_wake("http", last_head, matched)
                next_http_poll_at = time.monotonic() + http_poll_interval
            else:
                http_backoff = min(5.0, http_backoff * 2.0)
                next_http_poll_at = time.monotonic() + random.uniform(0.1, http_backoff)

        if is_coordinator:
            _set_disk_wait(True, last_read_seq=since_int if since_int > 0 else None)
            wait_entered = True
            if bfile is not None and bfile.is_file():
                try:
                    st = bfile.stat()
                    last_sig = (st.st_mtime_ns, st.st_size)
                except OSError:
                    pass

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if bfile is not None and bfile.is_file():
                    fin = _check_disk()
                    if fin is not None:
                        last_head = fin[0]
                return {
                    "ok": True,
                    "woken": False,
                    "timedOut": True,
                    "swarmId": cfg["swarm_id"],
                    "head": last_head,
                    "messages": [],
                }

            poll_interval = 0.15 if (bfile is not None) else http_poll_interval
            stop_event.wait(timeout=min(poll_interval, remaining))

            if bfile is not None:
                cur_sig: tuple[int, int] | None = None
                try:
                    if bfile.is_file():
                        st = bfile.stat()
                        cur_sig = (st.st_mtime_ns, st.st_size)
                except OSError:
                    pass
                if cur_sig is not None and cur_sig != last_sig:
                    last_sig = cur_sig
                    disk_res = _check_disk()
                    if disk_res is not None:
                        last_head, matched = disk_res
                        if matched:
                            return _finish_wake("disk", last_head, matched)
            if has_http and time.monotonic() >= next_http_poll_at:
                http_res = _check_http()
                if http_res is not None:
                    http_backoff = http_poll_interval
                    next_http_poll_at = time.monotonic() + http_poll_interval
                    last_head, matched = http_res
                    if matched:
                        return _finish_wake("http", last_head, matched)
                else:
                    http_backoff = min(5.0, http_backoff * 2.0)
                    next_http_poll_at = time.monotonic() + random.uniform(0.1, http_backoff)
    finally:
        if wait_entered and not wait_exited:
            _set_disk_wait(False)


def swarm_channel(
    action: str = "list",
    channel: str = "",
    topic: str = "",
    members: list[str] | None = None,
    *,
    swarm_id: str | None = None,
    agent: str | None = None,
    http_url: str | None = None,
    board_dir: str | None = None,
    token_file: str | None = None,
    conversation_id: str | None = None,
) -> dict[str, Any]:
    """Manage Swarm Board channels (`list|create|subscribe|unsubscribe|archive`) via HTTP or locked board.json."""
    cfg, err = _require_config(
        swarm_id=swarm_id,
        agent=agent,
        http_url=http_url,
        board_dir=board_dir,
        token_file=token_file,
        conversation_id=conversation_id,
        require_agent=False,
    )
    if err is not None:
        return err

    clean_op = str(action or "list").strip().lower()
    if clean_op not in ("list", "create", "subscribe", "unsubscribe", "archive"):
        return {"ok": False, "error": f"Unsupported channel action: {action!r}"}

    if cfg["http_url"]:
        http_payload: dict[str, Any] = {
            "swarmId": cfg["swarm_id"],
            "agent": cfg["agent"] or "coordinator",
            "action": clean_op,
            "channel": channel or None,
            "topic": topic,
            "members": list(members or []) if members is not None else None,
        }
        if cfg["conversation_id"]:
            http_payload["conversationId"] = cfg["conversation_id"]
        http_res = _http_request_json(
            f"{cfg['http_url']}/internal/board/channel",
            method="POST",
            token=cfg["token"],
            payload=http_payload,
            cfg=cfg,
        )
        http_data = http_res.get("data")
        if http_res.get("ok") and isinstance(http_data, dict) and (http_data.get("ok") or http_data.get("channels") is not None):
            return {"ok": True, "source": "http", **http_data}
        if _is_http_client_rejection(http_res):
            return {"ok": False, "source": "http", "status": http_res["status"], "error": http_res["error"]}

    paths = _resolve_board_paths(cfg)
    if paths is None:
        return {"ok": False, "error": "Neither HTTP endpoint nor board_dir is reachable for swarm_channel"}
    bfile, block = paths

    if clean_op == "list":
        board = _normalize_board(_read_json_safe(bfile, None), cfg["swarm_id"])
        msgs = [m for m in (board.get("messages") or []) if isinstance(m, dict)]
        out: list[dict[str, Any]] = []
        for ch_key, crec in sorted((board.get("channels") or {}).items()):
            if not isinstance(crec, dict):
                continue
            out.append({
                **crec,
                "displayName": _channel_display_name(ch_key),
                "messageCount": sum(1 for m in msgs if _normalize_channel_name(m.get("channel")) == ch_key),
            })
        return {"ok": True, "source": "disk", "swarmId": cfg["swarm_id"], "channels": out}

    if not channel or not str(channel).strip():
        return {"ok": False, "error": f"Channel action '{clean_op}' requires a non-empty channel name."}
    norm_ch = _normalize_channel_name(channel)
    disp = _channel_display_name(norm_ch)
    if clean_op in ("create", "archive") and disp == "main":
        return {"ok": False, "error": "Cannot create or archive the primary channel."}

    actor = cfg["agent"] or "coordinator"
    target_members = [str(m).strip().lstrip("@") for m in (members or []) if str(m).strip()]
    ts = _now_iso()

    with _atomic_file_lock(block):
        board = _normalize_board(_read_json_safe(bfile, None), cfg["swarm_id"])
        channels = board.setdefault("channels", {})
        b_members = board.setdefault("members", {})
        msgs = list(board.get("messages") or [])
        seq = max(int(board.get("nextSeq") or 1), _head_seq(board) + 1)
        board["nextSeq"] = seq + 1

        if clean_op == "create":
            existing = channels.get(norm_ch) if isinstance(channels.get(norm_ch), dict) else None
            rec = {
                "name": norm_ch,
                "description": topic or (existing or {}).get("description", ""),
                "topic": topic or (existing or {}).get("topic", ""),
                "archived": False,
                "subscribers": list((existing or {}).get("subscribers") or []),
                "createdBy": (existing or {}).get("createdBy") or actor,
                "createdAt": (existing or {}).get("createdAt") or ts,
                "updatedAt": ts,
            }
            for mname in target_members:
                if mname not in rec["subscribers"]:
                    rec["subscribers"].append(mname)
                _, mem = _find_member_ci(b_members, mname)
                if mem is not None:
                    mchs = list(mem.get("channels") or [])
                    if norm_ch not in mchs:
                        mchs.append(norm_ch)
                    mem["channels"] = mchs
                    mem["updatedAt"] = ts
            channels[norm_ch] = rec
            ctl = {"op": "create", "channel": disp, "topic": rec["topic"]}
            summary_txt = f"Created channel #{disp}" + (f": {rec['topic']}" if rec["topic"] else "")
        elif clean_op in ("subscribe", "unsubscribe"):
            if not target_members:
                return {"ok": False, "error": f"Channel action '{clean_op}' requires members."}
            rec = channels.get(norm_ch)
            if not isinstance(rec, dict):
                if clean_op == "unsubscribe":
                    return {"ok": False, "error": f"Channel '{disp}' does not exist."}
                rec = {
                    "name": norm_ch,
                    "description": topic,
                    "topic": topic,
                    "archived": False,
                    "subscribers": [],
                    "createdBy": actor,
                    "createdAt": ts,
                    "updatedAt": ts,
                }
                channels[norm_ch] = rec
            if rec.get("archived") and clean_op == "subscribe":
                return {"ok": False, "error": f"Channel '{disp}' is archived."}
            prev_map: dict[str, list[str] | None] = {}
            next_map: dict[str, list[str] | None] = {}
            subs = list(rec.get("subscribers") or [])
            for mname in target_members:
                _, mem = _find_member_ci(b_members, mname)
                prev_raw = mem.get("channels") if isinstance(mem, dict) else None
                prev_map[mname] = None if prev_raw is None else [_channel_display_name(c) if c != "*" else "*" for c in prev_raw]
                if clean_op == "subscribe":
                    cur_chs: list[str] | None = list(prev_raw or [])
                    if norm_ch not in cur_chs and "*" not in cur_chs:
                        cur_chs.append(norm_ch)
                    if mname not in subs:
                        subs.append(mname)
                else:
                    cur_list = [c for c in (prev_raw or []) if _normalize_channel_name(c) != norm_ch]
                    cur_chs = cur_list if cur_list else None
                    subs = [s for s in subs if s != mname]
                next_map[mname] = None if cur_chs is None else [_channel_display_name(c) if c != "*" else "*" for c in cur_chs]
                if isinstance(mem, dict):
                    mem["channels"] = cur_chs
                    mem["updatedAt"] = ts
            rec["subscribers"] = subs
            rec["updatedAt"] = ts
            ctl = {"op": clean_op, "channel": disp, "members": target_members, "prev": prev_map, "next": next_map}
            verb = "Subscribed" if clean_op == "subscribe" else "Unsubscribed"
            summary_txt = f"{verb} {', '.join('@' + m for m in target_members)} {'to' if clean_op == 'subscribe' else 'from'} #{disp}"
        else:
            rec = channels.get(norm_ch)
            if not isinstance(rec, dict):
                return {"ok": False, "error": f"Channel '{disp}' does not exist."}
            rec["archived"] = True
            rec["updatedAt"] = ts
            ctl = {"op": "archive", "channel": disp}
            summary_txt = f"Archived channel #{disp}"

        ctl_msg = {
            "seq": seq,
            "id": _new_id("bmsg-"),
            "threadId": None,
            "replyTo": None,
            "channel": norm_ch,
            "channels": [norm_ch],
            "type": "system",
            "sender": actor,
            "senderRole": cfg["role"] or "coordinator",
            "senderTier": 2,
            "mentions": [],
            "channelMentions": [],
            "summary": summary_txt,
            "details": "",
            "files": [],
            "refs": [],
            "status": "open",
            "readBy": [actor],
            "control": ctl,
            "createdAt": ts,
            "updatedAt": ts,
        }
        msgs.append(ctl_msg)
        if len(msgs) > MAX_BOARD_MESSAGES:
            overflow = msgs[:-MAX_BOARD_MESSAGES]
            msgs = msgs[-MAX_BOARD_MESSAGES:]
            _append_archive_messages(bfile.parent / "board_archive.jsonl", overflow)
        board["messages"] = msgs
        board["updatedAt"] = ts
        _atomic_write_json(bfile, board)

    return {"ok": True, "source": "disk", "swarmId": cfg["swarm_id"], "channel": rec, "controlMessage": ctl_msg}


def _p(kind: str, description: str, **extra: Any) -> dict[str, Any]:
    return {"type": kind, "description": description, **extra}


_CONTEXT_PROPERTIES: dict[str, dict[str, Any]] = {
    "swarm_id": _p("string", "Optional Swarm ID (overrides HERMES_SWARM_* env and registry.json discovery)."),
    "agent": _p("string", "Optional caller agent name (overrides HERMES_SWARM_AGENT / HERMES_A2A_WORKER_NAME)."),
    "http_url": _p("string", "Optional Swarm HTTP bridge URL (overrides HERMES_SWARM_HTTP_URL / registry.json)."),
    "board_dir": _p("string", "Optional Swarm Board directory path (overrides HERMES_SWARM_BOARD_DIR)."),
    "token_file": _p("string", "Optional internal bearer token file path (overrides HERMES_SWARM_TOKEN_FILE)."),
    "conversation_id": _p("string", "Optional conversation ID for HTTP /board scoping (overrides HERMES_SWARM_CONVERSATION_ID)."),
}


SPECS: dict[str, tuple[str, dict[str, Any], list[str]]] = {
    "swarm_post": (
        "Post a message to the Swarm Board (uses HTTP bridge with atomic locked board.json fallback). "
        "Put @mentions (for example @coordinator or @peer) in summary. Start final completion posts with [done] after the @mention.",
        {
            "summary": _p("string", "Concise summary line (non-empty). Include @mention targets here.", minLength=1),
            "details": _p("string", "Optional extended markdown details or findings.", default=""),
            "kind": _p(
                "string",
                "Message kind.",
                enum=["post", "question", "decision", "handoff", "escalation", "digest", "ack", "system", "blocker", "approval_request", "handoff_ready"],
                default="post",
            ),
            "channel": _p("string", "Board channel (defaults to main).", default=""),
            "files": _p("array", "Optional list of file paths referenced by this post.", items={"type": "string"}),
            "reply_to": _p("integer", "Optional sequence number this message replies to."),
            "to": _p("array", "Optional explicit @mention targets.", items={"type": "string"}),
            "thread_id": _p("string", "Optional thread identifier."),
            **_CONTEXT_PROPERTIES,
        },
        ["summary"],
    ),
    "swarm_read": (
        "Read Swarm Board messages oldest-first with optional sequence, channel, mention, sender, kind, archive, search, status, and mark_read_by filters.",
        {
            "since": _p("integer", "Return messages with seq > since (0 returns the latest messages).", minimum=0, default=0),
            "seqs": _p("array", "Optional exact sequence numbers to fetch (ignores since and limit).", items={"type": "integer"}),
            "status": _p("boolean", "If true, return roster status instead of messages.", default=False),
            "channel": _p("string", "Optional channel filter ('*' for all channels, or topic slug).", default=""),
            "mention": _p("string", "Optional @mention filter.", default=""),
            "sender": _p("string", "Optional sender filter.", default=""),
            "kind": _p("string", "Optional message kind filter.", default=""),
            "mark_read_by": _p("string", "Optional agent name whose lastReadSeq/readBy should be updated.", default=""),
            "limit": _p("integer", "Maximum number of messages to return (1-500).", minimum=1, maximum=500, default=100),
            "include_archive": _p("boolean", "If true, include archived messages from board_archive.jsonl.", default=False),
            "before_seq": _p("integer", "Return messages with seq < before_seq.", minimum=1),
            "search": _p("string", "Optional case-insensitive substring search across summary, details, sender, channel, and files.", default=""),
            "full": _p("boolean", "If true, return full untruncated message payloads.", default=False),
            **_CONTEXT_PROPERTIES,
        },
        [],
    ),
    "swarm_claim": (
        "Atomically claim an editing scope or file path on the Swarm Board to avoid concurrent edits.",
        {
            "scope": _p("string", "File path, module, or logical scope to claim.", minLength=1),
            "notes": _p("string", "Optional short note about the planned edit.", default=""),
            **_CONTEXT_PROPERTIES,
        },
        ["scope"],
    ),
    "swarm_release": (
        "Release a previously claimed editing scope (or all scopes held by this agent when scope is omitted).",
        {
            "scope": _p("string", "Claimed scope to release; omit or pass empty string to release all held scopes.", default=""),
            **_CONTEXT_PROPERTIES,
        },
        [],
    ),
    "swarm_status": (
        "Inspect current Swarm Board roster, head sequence number, active claims, channels, and telemetry summary.",
        {**_CONTEXT_PROPERTIES},
        [],
    ),
    "swarm_wait": (
        "Wait in-process for new Swarm Board activity after since without spawning child shells.",
        {
            "since": _p("integer", "Sequence number to wait after (0 checks from start).", minimum=0, default=0),
            "mentions_only": _p("boolean", "Wake only on messages mentioning this agent, @all, or subscribed channels.", default=True),
            "timeout_seconds": _p("number", "Maximum seconds to wait before returning timedOut=true.", minimum=0.05, maximum=600, default=120),
            **_CONTEXT_PROPERTIES,
        },
        [],
    ),
    "swarm_channel": (
        "Manage Swarm Board channels (`list`, `create`, `subscribe`, `unsubscribe`, `archive`).",
        {
            "action": _p("string", "Channel operation.", enum=["list", "create", "subscribe", "unsubscribe", "archive"], default="list"),
            "channel": _p("string", "Channel name/slug (required for non-list actions).", default=""),
            "topic": _p("string", "Optional channel topic/description.", default=""),
            "members": _p("array", "Optional list of member names to subscribe/unsubscribe.", items={"type": "string"}),
            **_CONTEXT_PROPERTIES,
        },
        [],
    ),
}


def schema(name: str) -> dict[str, Any]:
    description, properties, required = SPECS[name]
    return {
        "name": name,
        "description": description,
        "parameters": {
            "type": "object",
            "properties": properties,
            "required": required,
        },
    }


def handler(name: str):
    def _handle(args: dict[str, Any] | None = None, **kwargs: Any) -> str:
        params = dict(args) if isinstance(args, dict) else dict(kwargs)
        raw_turn_ctx = (
            params.get("_context")
            or params.get("session_info")
            or kwargs.get("_context")
            or kwargs.get("session_info")
        )
        ctx_kwargs = {
            "swarm_id": params.get("swarm_id"),
            "agent": params.get("agent"),
            "http_url": params.get("http_url"),
            "board_dir": params.get("board_dir"),
            "token_file": params.get("token_file"),
            "conversation_id": params.get("conversation_id"),
        }
        t0 = time.monotonic()
        try:
            with turn_context(raw_turn_ctx if isinstance(raw_turn_ctx, Mapping) else None):
                if name == "swarm_post":
                    result = swarm_post(
                        summary=params.get("summary", ""),
                        details=params.get("details", ""),
                        kind=params.get("kind", "post"),
                        channel=params.get("channel", ""),
                        files=params.get("files"),
                        reply_to=params.get("reply_to"),
                        to=params.get("to"),
                        thread_id=params.get("thread_id"),
                        **ctx_kwargs,
                    )
                elif name == "swarm_read":
                    result = swarm_read(
                        since=params.get("since", 0),
                        channel=params.get("channel", ""),
                        limit=params.get("limit", 100),
                        seqs=params.get("seqs"),
                        status=bool(params.get("status", False)),
                        mention=params.get("mention"),
                        sender=params.get("sender"),
                        kind=params.get("kind"),
                        mark_read_by=params.get("mark_read_by"),
                        include_archive=bool(params.get("include_archive", False)),
                        before_seq=params.get("before_seq"),
                        search=params.get("search"),
                        full=bool(params.get("full", False)),
                        **ctx_kwargs,
                    )
                elif name == "swarm_claim":
                    result = swarm_claim(
                        scope=params.get("scope", ""),
                        notes=params.get("notes", ""),
                        **ctx_kwargs,
                    )
                elif name == "swarm_release":
                    result = swarm_release(scope=params.get("scope", ""), **ctx_kwargs)
                elif name == "swarm_status":
                    result = swarm_status(**ctx_kwargs)
                elif name == "swarm_wait":
                    result = swarm_wait(
                        since=params.get("since", 0),
                        mentions_only=params.get("mentions_only", True),
                        timeout_seconds=params.get("timeout_seconds", 120.0),
                        **ctx_kwargs,
                    )
                elif name == "swarm_channel":
                    result = swarm_channel(
                        action=params.get("action", "list"),
                        channel=params.get("channel", ""),
                        topic=params.get("topic", ""),
                        members=params.get("members"),
                        **ctx_kwargs,
                    )
                else:
                    result = {"ok": False, "error": f"Unknown tool: {name}"}
                try:
                    log_cfg, log_err = _require_config(
                        swarm_id=ctx_kwargs.get("swarm_id"),
                        agent=ctx_kwargs.get("agent"),
                        http_url=ctx_kwargs.get("http_url"),
                        board_dir=ctx_kwargs.get("board_dir"),
                        token_file=ctx_kwargs.get("token_file"),
                        conversation_id=ctx_kwargs.get("conversation_id"),
                        require_agent=False,
                    )
                    if log_err is None:
                        _log_bridge_event(
                            log_cfg,
                            "tool_call",
                            tool=name,
                            ok=bool(result.get("ok")),
                            source=result.get("source"),
                            durationMs=round((time.monotonic() - t0) * 1000, 1),
                            error=result.get("error") if not result.get("ok") else None,
                        )
                except Exception:
                    pass
            return json.dumps(result, ensure_ascii=False)
        except Exception as exc:
            return json.dumps({"ok": False, "error": str(exc)[:2000]}, ensure_ascii=False)

    return _handle
