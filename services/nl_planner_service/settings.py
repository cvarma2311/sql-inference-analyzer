import os


def get_settings() -> dict:
    return {
        "llm_url": os.getenv("LLM_URL", "http://localhost:8001/v1/chat/completions"),
        "llm_model": os.getenv("LLM_MODEL", "local"),
        "llm_timeout_s": float(os.getenv("LLM_TIMEOUT_S", "30")),
        "llm_retries": int(os.getenv("LLM_RETRIES", "2")),
        "llm_retry_backoff_s": float(os.getenv("LLM_RETRY_BACKOFF_S", "0.5")),
        "planner_schema_context": os.getenv("PLANNER_SCHEMA_CONTEXT", "on").lower(),
    }
