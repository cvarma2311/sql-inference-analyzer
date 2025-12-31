#!/usr/bin/env bash
set -euo pipefail

MODEL_PATH="${DEEPSEEK_MODEL_PATH:-models/llm/DeepSeek-Coder-V2-Instruct-Q4_K_M.gguf}"
SERVER_BIN="${LLAMA_SERVER_BIN:-external/llama.cpp/build/bin/llama-server}"
LOG_FILE="${DEEPSEEK_LOG_FILE:-/tmp/deepseek-coder-v2.log}"
PORT="${DEEPSEEK_PORT:-8002}"
CTX_SIZE="${DEEPSEEK_CTX_SIZE:-8192}"

if [[ "${1:-}" == "--stop" ]]; then
  pkill -f "${SERVER_BIN}.*${PORT}" || true
  echo "deepseek-coder-v2 server stopped."
  exit 0
fi

if [[ ! -x "${SERVER_BIN}" ]]; then
  echo "llama-server binary not found at ${SERVER_BIN}" >&2
  exit 1
fi

if [[ ! -f "${MODEL_PATH}" ]]; then
  echo "Model file not found at ${MODEL_PATH}" >&2
  exit 1
fi

nohup "${SERVER_BIN}" \
  -m "${MODEL_PATH}" \
  --port "${PORT}" \
  --ctx-size "${CTX_SIZE}" \
  > "${LOG_FILE}" 2>&1 &

echo "deepseek-coder-v2 server started on port ${PORT}."
echo "Log file: ${LOG_FILE}"
