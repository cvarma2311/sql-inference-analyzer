import json
import logging
import time
from typing import Any, Dict

import httpx

from .settings import get_settings

logger = logging.getLogger("nl_planner_service.llm_client")


def _build_payload(model: str, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 512,
    }


def request_plan(system_prompt: str, user_prompt: str) -> Dict[str, Any]:
    settings = get_settings()
    url = settings["llm_url"]
    model = settings["llm_model"]
    timeout = settings["llm_timeout_s"]
    retries = settings["llm_retries"]
    backoff = settings["llm_retry_backoff_s"]

    payload = _build_payload(model, system_prompt, user_prompt)
    last_exc: Exception | None = None

    for attempt in range(retries + 1):
        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.post(url, json=payload)
                resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                extracted = _extract_json_object(content)
                if extracted:
                    return json.loads(extracted)
                snippet = content[:400].replace("\n", " ")
                raise ValueError(f"LLM returned non-JSON content. Snippet: {snippet}")
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                break
            time.sleep(backoff * (2 ** attempt))

    raise RuntimeError(f"LLM request failed: {last_exc}")


def _extract_json_object(content: str) -> str | None:
    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    return content[start : end + 1]
