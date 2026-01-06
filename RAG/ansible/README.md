# Ansible Ollama LLM Setup

Automated Ollama installation and LLM model management for Mac and Ubuntu.

## Overview

- Installs Ollama and required packages (make, gcc, curl, build-essential)
- Downloads 3 LLM models via Ollama
- Generates startup scripts for each model
- Auto-detects OS (Mac/Ubuntu)

## Models

1. **Defog Llama3 SQL Coder** - Port 8001
2. **DeepSeek Coder V2** - Port 8002
3. **Llama 3.1 8B** - Port 8003

## Prerequisites

**Ubuntu:**
```bash
sudo apt install ansible -y
```

**Mac:**
```bash
brew install ansible
```

## Installation

```bash
ansible-playbook -i inventory.ini playbook.yml
```

**Ubuntu with sudo:**
```bash
ansible-playbook -i inventory.ini playbook.yml --ask-become-pass
```

## Folder Structure

**Before running:**
```
ansible-ollama-llm/
├── inventory.ini
├── playbook.yml
├── vars.yml
├── README.md
└── roles/
    ├── envsetup/
    │   ├── tasks/main.yml
    │   └── templates/ansib_env.j2
    └── ollama/
        ├── tasks/main.yml
        └── templates/
            ├── defog-llama3.sh.j2
            ├── deepseek-coder-v2.sh.j2
            └── llama3.sh.j2
```

**After running:**
```
ansible-ollama-llm/
├── ansib_env              # Environment variables (ROOT)
├── scripts/               # Model startup scripts
│   ├── defog-llama3.sh
│   ├── deepseek-coder-v2.sh
│   └── llama3.sh
├── logs/                  # Log files
│   ├── defog-llama3.log
│   ├── deepseek-coder-v2.log
│   └── llama3.log
└── [original files]
```

## Usage

### Run a single model:

```bash
cd scripts/

# Run any model (one at a time)
./defog-llama3.sh       # Port 8001
./deepseek-coder-v2.sh  # Port 8002
./llama3.sh             # Port 8003
```

**Stop:** Press `Ctrl+C` or `pkill -f "ollama serve.*8001"`

### Test a model:

```bash
curl http://localhost:8001/api/generate -d '{
  "model": "mannix/defog-llama3-sqlcoder-8b:latest",
  "prompt": "test",
  "stream": false
}'
```

## File Explanations

- **vars.yml** - Configuration (models, ports, paths)
- **inventory.ini** - Ansible hosts (localhost)
- **playbook.yml** - Main playbook
- **ansib_env** - Generated environment file (project root)
- **roles/envsetup/** - Creates directories, installs packages, generates ansib_env
- **roles/ollama/** - Installs Ollama, downloads models, creates scripts
- **scripts/*.sh** - Model startup scripts (separate directory)
- **logs/*.log** - Application logs (separate directory)

## Configuration

### Change ports:

Edit `vars.yml`:
```yaml
models:
  - name: "defog-llama3"
    port: 9001  # Change port
```

Re-run: `ansible-playbook -i inventory.ini playbook.yml`

### Skip models:

Comment out in `vars.yml`:
```yaml
models:
  - name: "defog-llama3"
    full_name: "mannix/defog-llama3-sqlcoder-8b:latest"
    port: 8001
  # - name: "deepseek-coder-v2"  # Skipped
```

## Troubleshooting

**Port in use:**
```bash
lsof -i :8001
kill <PID>
```

**Model not found:**
```bash
ollama list
ollama pull mannix/defog-llama3-sqlcoder-8b:latest
```

**View logs:**
```bash
tail -f logs/defog-llama3.log
```

**Ollama not found:**
```bash
ollama --version
which ollama
```

## How It Works

1. **envsetup role:**
   - Creates `scripts/` and `logs/` directories
   - Installs packages (make, gcc, curl, build-essential, wget)
   - Generates `ansib_env` in project root

2. **ollama role:**
   - Installs Ollama binary
   - Pulls all configured models
   - Generates `.sh` files in `scripts/` directory

3. **Result:**
   - `ansib_env` → project root
   - `scripts/*.sh` → scripts directory
   - `logs/*.log` → logs directory

## Variables in ansib_env

```bash
PROJECT_ROOT=/path/to/ansible-ollama-llm
SCRIPTS_DIR=/path/to/ansible-ollama-llm/scripts
LOGS_DIR=/path/to/ansible-ollama-llm/logs

DEFOG_LLAMA3_MODEL=mannix/defog-llama3-sqlcoder-8b:latest
DEFOG_LLAMA3_PORT=8001
DEFOG_LLAMA3_LOG=/path/to/logs/defog-llama3.log
DEFOG_LLAMA3_SCRIPT=/path/to/scripts/defog-llama3.sh

# (similar for other models)
```

## Requirements

- Disk: 20GB minimum
- Memory: 8GB minimum
- Ansible 2.9+
- Python 3.x
