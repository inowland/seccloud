#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE="${SECCLOUD_WORKSPACE:-.seccloud}"
RUNTIME_ROOT="${SECCLOUD_RUNTIME_ROOT:-.}"

cd "$ROOT_DIR"

echo "=== Quickwit Event Smoke Test ==="
echo "workspace: $WORKSPACE"
echo "runtime:   $RUNTIME_ROOT"
echo ""

uv run seccloud ensure-quickwit --runtime-root "$RUNTIME_ROOT" >/dev/null

if uv run seccloud smoke-quickwit-events --workspace "$WORKSPACE" --runtime-root "$RUNTIME_ROOT" "$@"; then
  echo ""
  echo "=== QUICKWIT SMOKE PASSED ==="
  exit 0
fi

LOG_PATH="$ROOT_DIR/.seccloud/quickwit/quickwit.log"
if [ "$RUNTIME_ROOT" != "." ]; then
  LOG_PATH="$ROOT_DIR/$RUNTIME_ROOT/.seccloud/quickwit/quickwit.log"
fi

echo ""
echo "=== QUICKWIT SMOKE FAILED ==="
if [ -f "$LOG_PATH" ]; then
  echo "--- Quickwit log tail ---"
  tail -n 60 "$LOG_PATH"
fi
exit 1
