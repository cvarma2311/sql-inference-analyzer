import json
import time
from typing import Any, Dict

import httpx

from .settings import get_settings


def _build_payload(model: str, system_prompt: str, user_prompt: str, max_tokens: int) -> Dict[str, Any]:
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }


def request_narrative(system_prompt: str, user_prompt: str) -> str:
    settings = get_settings()
    url = settings["llm_url"]
    model = settings["llm_model"]
    timeout = settings["llm_timeout_s"]
    retries = settings["llm_retries"]
    backoff = settings["llm_retry_backoff_s"]
    max_tokens = settings["narrative_max_tokens"]

    payload = _build_payload(model, system_prompt, user_prompt, max_tokens)
    last_exc: Exception | None = None

    for attempt in range(retries + 1):
        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.post(url, json=payload)
                resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                break
            time.sleep(backoff * (2 ** attempt))

    raise RuntimeError(f"LLM request failed: {last_exc}")
