#!/bin/bash
# DeepSeek Coder V2 Model
# Generated: 2026-01-06T05:20:35Z

set -e

MODEL="deepseek-coder-v2:latest"
PORT="8002"
LOG_FILE="/Users/thrinesharigela/Desktop/ansible/logs/deepseek-coder-v2.log"

echo "=========================================="
echo "DeepSeek Coder V2 Model"
echo "=========================================="
echo "Model: $MODEL"
echo "Port: $PORT"
echo "Log: $LOG_FILE"
echo "=========================================="

# Check if Ollama is installed
if ! command -v ollama &> /dev/null; then
    echo "ERROR: Ollama is not installed"
    exit 1
fi

# Check if port is in use
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo "ERROR: Port $PORT is already in use"
    exit 1
fi

# Check if model exists
if ! ollama list | grep -q "$MODEL"; then
    echo "ERROR: Model $MODEL not found"
    exit 1
fi

# Set environment
export OLLAMA_HOST=0.0.0.0:$PORT

echo "Starting Ollama on port $PORT..."
nohup ollama serve > "$LOG_FILE" 2>&1 &
OLLAMA_PID=$!

sleep 5

# Verify
if ! lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo "ERROR: Failed to start"
    exit 1
fi

echo ""
echo "✓ Model started successfully"
echo "URL: http://localhost:$PORT"
echo "PID: $OLLAMA_PID"
echo "Log: $LOG_FILE"
echo ""
echo "Test: curl http://localhost:$PORT/api/generate -d '{\"model\": \"$MODEL\", \"prompt\": \"test\"}'"
echo "Stop: pkill -f 'ollama serve.*$PORT'"
echo ""
echo "Logs (Ctrl+C to exit):"
tail -f "$LOG_FILE"
