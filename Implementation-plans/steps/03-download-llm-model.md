# Step 03 - Download Llama 3 8B Instruct Model

## Goal
Fetch the chosen GGUF model for offline inference.

## Inputs
- Model choice: Llama-3-8B-Instruct-GGUF.
- Storage location for model files.

## Actions
- Create the model directory:
  - `mkdir -p models/llm`
- Download the GGUF file:
  - `curl -L -o models/llm/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf https://huggingface.co/lmstudio-community/Meta-Llama-3-8B-Instruct-GGUF/resolve/main/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf`
- Verify the file exists and is non-empty:
  - `ls -lh models/llm/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf`
- Optional resume if the download is interrupted:
  - `curl -L -C - -o models/llm/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf https://huggingface.co/lmstudio-community/Meta-Llama-3-8B-Instruct-GGUF/resolve/main/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf`
- Smoke test (start server, call, stop):
  - `nohup external/llama.cpp/build/bin/llama-server -m models/llm/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf --port 8001 --ctx-size 8192 > /tmp/llama-server.log 2>&1 & sleep 5`
  - `curl -s http://localhost:8001/v1/chat/completions -H 'Content-Type: application/json' -d '{"model":"local","messages":[{"role":"user","content":"Say OK."}],"max_tokens":5}'`
  - `pkill -f llama-server`

## How To Run (Launcher Script)

- Start the server:
  - `scripts/run_llama_server.sh`
- Stop the server:
  - `scripts/run_llama_server.sh --stop`
- Logs:
  - `/tmp/llama-server.log`
- Optional overrides:
  - `LLAMA_MODEL_PATH=models/llm/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf`
  - `LLAMA_SERVER_BIN=external/llama.cpp/build/bin/llama-server`
  - `LLAMA_PORT=8001`
  - `LLAMA_CTX_SIZE=8192`
  - `LLAMA_LOG_FILE=/tmp/llama-server.log`

## Exit Criteria
- The GGUF file is present in `models/llm` and matches the expected size.
