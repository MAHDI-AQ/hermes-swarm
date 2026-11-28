"""hermes-swarm-bridge plugin entry point: always registers native tools; gates prompt section on HERMES_SWARM_ID."""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping

from . import swarm_tools

logger = logging.getLogger(__name__)

TOOLSET = "hermes-swarm-bridge"
PROMPT_SECTION_ID = "hermes-swarm-bridge.worker"
PROMPT_SECTION_MAX_CHARS = 700


def _worker_prompt_section(swarm_id: str, agent_name: str, role: str):
    def _render(session_info: Mapping[str, Any] | None = None) -> str:
        info = session_info or {}
        if info.get("platform") == "subagent":
            return ""
        turn_ctx = swarm_tools.get_turn_context()
        eff_swarm = str(
            info.get("swarm_id")
            or info.get("swarmId")
            or turn_ctx.get("swarm_id")
            or swarm_id
        ).strip()
        eff_agent = str(
            info.get("agent")
            or info.get("worker_name")
            or turn_ctx.get("agent")
            or agent_name
        ).strip()
        eff_role = str(
            info.get("role")
            or turn_ctx.get("role")
            or role
        ).strip()
        briefing = swarm_tools.get_bridge_briefing(swarm_id=eff_swarm, agent=eff_agent)
        claims_list = briefing.get("claims") or []
        claims_note = f" Active claims: {', '.join(claims_list)}." if claims_list else ""
        seq_note = f" lastReadSeq={briefing['lastReadSeq']}." if briefing.get("lastReadSeq") else ""
        ch_note = f" channel={briefing['channel']}." if briefing.get("channel") else ""
        text = (
            f"Swarm Worker Bridge active — agent: {eff_agent}, role: {eff_role}, swarm: {eff_swarm}.{ch_note}{seq_note}{claims_note} "
            "Use the native swarm_* tools (swarm_post, swarm_read, swarm_claim, swarm_release, "
            "swarm_status, swarm_wait, swarm_channel) for all Swarm Board coordination instead of shelling out to board_cli.py. "
            "Put @mentions in swarm_post summary, start final completion posts with [done] after @mention, "
            "and claim/release shared editing scopes with swarm_claim and swarm_release."
        )
        return text[:PROMPT_SECTION_MAX_CHARS]

    return _render


def _on_post_compaction(**kwargs: Any) -> None:
    try:
        swarm_tools.mark_compacted(
            session_id=str(kwargs.get("session_id") or ""),
            kind=kwargs.get("kind"),
            reason=kwargs.get("reason"),
            before_tokens=kwargs.get("before_tokens"),
            after_tokens=kwargs.get("after_tokens"),
            messages_removed=kwargs.get("messages_removed"),
        )
    except Exception:
        logger.debug("hermes-swarm-bridge: post_compaction hook error", exc_info=True)


def _make_pre_llm_call_hook(swarm_id: str, agent_name: str):
    def _on_pre_llm_call(**kwargs: Any) -> dict[str, str] | None:
        parts: list[str] = []
        try:
            briefing = swarm_tools.consume_compacted_briefing(str(kwargs.get("session_id") or ""))
            if briefing:
                parts.append(briefing)
        except Exception:
            logger.debug("hermes-swarm-bridge: pre_llm_call compaction hook error", exc_info=True)
        try:
            turn_ctx = swarm_tools.get_turn_context()
            eff_swarm = str(turn_ctx.get("swarm_id") or swarm_id).strip()
            eff_agent = str(turn_ctx.get("agent") or agent_name).strip()
            urgent = swarm_tools.poll_pending_mentions_context(swarm_id=eff_swarm, agent=eff_agent)
            if urgent:
                parts.append(urgent)
        except Exception:
            logger.debug("hermes-swarm-bridge: pre_llm_call inbox hook error", exc_info=True)
        if parts:
            return {"context": "\n\n".join(parts)}
        return None

    return _on_pre_llm_call


def _on_pre_llm_call(**kwargs: Any) -> dict[str, str] | None:
    return _make_pre_llm_call_hook("", "")(**kwargs)


def _make_pre_verify_hook(swarm_id: str, agent_name: str):
    def _on_pre_verify(**kwargs: Any) -> dict[str, str] | None:
        try:
            if int(kwargs.get("attempt") or 0) > 0:
                return None
            turn_ctx = swarm_tools.get_turn_context()
            eff_swarm = str(turn_ctx.get("swarm_id") or swarm_id).strip()
            eff_agent = str(turn_ctx.get("agent") or agent_name).strip()
            claims = swarm_tools.get_unreleased_claims(swarm_id=eff_swarm, agent=eff_agent)
            if claims:
                return {
                    "action": "continue",
                    "message": (
                        f"Swarm Bridge warning: unreleased scope claim(s) still held ({', '.join(claims)}). "
                        "Call swarm_release before finishing if your edits are complete."
                    ),
                }
        except Exception:
            logger.debug("hermes-swarm-bridge: pre_verify hook error", exc_info=True)
        return None

    return _on_pre_verify


def _make_post_tool_call_hook(agent_name: str):
    def _on_post_tool_call(**kwargs: Any) -> None:
        try:
            turn_ctx = swarm_tools.get_turn_context()
            eff_agent = str(turn_ctx.get("agent") or agent_name).strip()
            swarm_tools.touch_heartbeat(agent=eff_agent)
        except Exception:
            logger.debug("hermes-swarm-bridge: post_tool_call hook error", exc_info=True)

    return _on_post_tool_call


def _make_transform_tool_result_hook(swarm_id: str, agent_name: str):
    def _on_transform_tool_result(**kwargs: Any) -> str | None:
        try:
            result = kwargs.get("result")
            if not isinstance(result, str):
                return None
            tool_name = str(kwargs.get("tool_name") or "").strip()
            if tool_name in ("swarm_read", "swarm_wait"):
                return None
            turn_ctx = swarm_tools.get_turn_context()
            eff_swarm = str(turn_ctx.get("swarm_id") or swarm_id).strip()
            eff_agent = str(turn_ctx.get("agent") or agent_name).strip()
            urgent = swarm_tools.poll_pending_mentions_context(swarm_id=eff_swarm, agent=eff_agent)
            if urgent:
                return f"{result}\n\n{urgent}" if result else urgent
        except Exception:
            logger.debug("hermes-swarm-bridge: transform_tool_result hook error", exc_info=True)
        return None

    return _on_transform_tool_result


def register(ctx: Any) -> None:
    """Register native Swarm Board tools; register worker prompt section only when HERMES_SWARM_ID is set."""
    for name in swarm_tools.SPECS:
        tool_schema = swarm_tools.schema(name)
        ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema=tool_schema,
            handler=swarm_tools.handler(name),
            description=tool_schema["description"],
        )

    swarm_id = (os.environ.get("HERMES_SWARM_ID") or "").strip()
    if not swarm_id:
        return

    agent_name = (
        (os.environ.get("HERMES_SWARM_AGENT") or "").strip()
        or (os.environ.get("HERMES_A2A_WORKER_NAME") or "").strip()
        or "worker"
    )
    role = (
        (os.environ.get("HERMES_SWARM_ROLE") or "").strip()
        or (os.environ.get("HERMES_A2A_WORKER_ROLE") or "").strip()
        or "worker"
    )

    register_section = getattr(ctx, "register_system_prompt_section", None)
    if callable(register_section):
        try:
            register_section(
                PROMPT_SECTION_ID,
                _worker_prompt_section(swarm_id, agent_name, role),
                position="after_memory",
                max_chars=PROMPT_SECTION_MAX_CHARS,
            )
        except Exception:
            logger.debug("hermes-swarm-bridge: failed to register system prompt section", exc_info=True)

    register_hook = getattr(ctx, "register_hook", None)
    if callable(register_hook):
        hooks = (
            ("post_compaction", _on_post_compaction),
            ("pre_llm_call", _make_pre_llm_call_hook(swarm_id, agent_name)),
            ("pre_verify", _make_pre_verify_hook(swarm_id, agent_name)),
            ("post_tool_call", _make_post_tool_call_hook(agent_name)),
            ("transform_tool_result", _make_transform_tool_result_hook(swarm_id, agent_name)),
        )
        for hook_name, hook_fn in hooks:
            try:
                register_hook(hook_name, hook_fn)
            except Exception:
                logger.debug("hermes-swarm-bridge: failed to register hook %s", hook_name, exc_info=True)
