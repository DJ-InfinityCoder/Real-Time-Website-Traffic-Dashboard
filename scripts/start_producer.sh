#!/usr/bin/env bash
# Log producer in the foreground.  ./scripts/start_producer.sh [--rate 120] [--error-every 60] ...
set -euo pipefail
source "$(dirname "$0")/env.sh"
exec "$VENV/bin/python" "$ROOT/producer/log_producer.py" "$@"
