import json
import logging
from typing import Any

from .llm_client import request_narrative
from .prompt import (
    BULLET_SUMMARY_SYSTEM_PROMPT,
    BULLET_SUMMARY_USER_TEMPLATE,
    NARRATIVE_SYSTEM_PROMPT,
    NARRATIVE_USER_TEMPLATE,
)
from .settings import get_settings

logger = logging.getLogger("narrative_service")


def generate_narrative(
    kpi_results: dict[str, Any],
    flags: list[str],
    context: dict[str, Any] | None = None,
) -> str:
    settings = get_settings()
    context_payload = context or {}
    user_prompt = NARRATIVE_USER_TEMPLATE.format(
        kpi_results=json.dumps(kpi_results, indent=2),
        flags=json.dumps(flags, indent=2),
        context=json.dumps(context_payload, indent=2),
    )
    narrative = request_narrative(NARRATIVE_SYSTEM_PROMPT, user_prompt).strip()
    if settings["log_narrative_output"] != "off":
        logger.info("narrative_output", extra={"extra": {"narrative": narrative}})
    return narrative


def generate_bullet_summary(
    kpi_results: dict[str, Any],
    flags: list[str],
    context: dict[str, Any] | None = None,
    drivers: dict[str, Any] | None = None,
) -> list[str]:
    settings = get_settings()
    context_payload = context or {}
    drivers_payload = drivers or {}
    user_prompt = BULLET_SUMMARY_USER_TEMPLATE.format(
        kpi_results=json.dumps(kpi_results, indent=2),
        flags=json.dumps(flags, indent=2),
        context=json.dumps(context_payload, indent=2),
        drivers=json.dumps(drivers_payload, indent=2),
    )
    summary_text = request_narrative(BULLET_SUMMARY_SYSTEM_PROMPT, user_prompt).strip()
    if settings["log_narrative_output"] != "off":
        logger.info("bullet_summary_output", extra={"extra": {"summary": summary_text}})
    bullets = []
    for line in summary_text.splitlines():
        line = line.strip()
        if line.startswith("- "):
            bullets.append(line[2:].strip())
    return bullets
