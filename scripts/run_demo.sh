#!/usr/bin/env bash
# UI preview WITHOUT Kafka/Spark/HBase: only needs Python + Flask.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -x .venv/bin/python ]; then python3 -m venv .venv && .venv/bin/pip install -q Flask; fi
.venv/bin/python -c "import flask" 2>/dev/null || .venv/bin/pip install -q Flask
exec .venv/bin/python api/app.py --demo --port "${API_PORT:-8050}"
