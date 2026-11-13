#!/usr/bin/env bash
# Flask API + dashboard in the foreground.  ./scripts/start_dashboard.sh [--demo]
set -euo pipefail
source "$(dirname "$0")/env.sh"
exec "$VENV/bin/python" "$ROOT/api/app.py" --port "$API_PORT" "$@"
