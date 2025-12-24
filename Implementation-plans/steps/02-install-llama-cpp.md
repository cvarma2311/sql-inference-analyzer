
# Step 02 - Install llama.cpp

## Goal
Set up the offline LLM runtime for local inference.

## Inputs
- Build toolchain (clang).
- CMake (macOS: `brew install cmake`).
- Target machine (CPU or small GPU).

## Actions
- Install CMake (macOS):
  - `HOMEBREW_NO_AUTO_UPDATE=1 brew install cmake`
- Clone `llama.cpp` into `external/llama.cpp`:
  - `git clone --depth 1 https://github.com/ggerganov/llama.cpp external/llama.cpp`
- Configure the build:
  - `cmake -S . -B build`
- Build the server binary (single job to avoid OOM):
  - `cmake --build build --target llama-server -j1`
- Verify the binary exists:
  - `ls -l external/llama.cpp/build/bin/llama-server`
- (Optional) Show help output:
  - `external/llama.cpp/build/bin/llama-server --help`

## Exit Criteria
- `external/llama.cpp/build/bin/llama-server` exists and runs.
