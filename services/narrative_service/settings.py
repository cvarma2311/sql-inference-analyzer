import os


def get_settings() -> dict:
    return {
        "llm_url": os.getenv("LLM_URL", "http://localhost:8001/v1/chat/completions"),
        "llm_model": os.getenv("LLM_MODEL", "local"),
        "llm_timeout_s": float(os.getenv("LLM_TIMEOUT_S", "30")),
        "llm_retries": int(os.getenv("LLM_RETRIES", "2")),
        "llm_retry_backoff_s": float(os.getenv("LLM_RETRY_BACKOFF_S", "0.5")),
        "narrative_max_tokens": int(os.getenv("NARRATIVE_MAX_TOKENS", "256")),
        "log_narrative_output": os.getenv("LOG_NARRATIVE_OUTPUT", "on").lower(),
    }
