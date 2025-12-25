import json
import logging
from typing import Any

from .llm_client import request_narrative
from .prompt import NARRATIVE_SYSTEM_PROMPT, NARRATIVE_USER_TEMPLATE
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
