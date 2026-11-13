#!/usr/bin/env bash
# Runs the Spark Structured Streaming job in the foreground.   ./scripts/start_spark.sh [--reset]
set -euo pipefail
source "$(dirname "$0")/env.sh"
CP="${CHECKPOINT_DIR:-$DATA/checkpoints/web-logs}"; export CHECKPOINT_DIR="$CP"
if [ "${1:-}" = "--reset" ]; then rm -rf "$CP"; echo "checkpoint cleared"; fi
"$VENV/bin/python" "$ROOT/scripts/package_python.py"
exec "$SPARK_HOME/bin/spark-submit" \
  --master "${SPARK_MASTER:-local[2]}" \
  --py-files "$ROOT/data/traffic-python.zip" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:$SPARK_VERSION" \
  --conf spark.driver.memory=1g \
  --conf spark.ui.showConsoleProgress=false \
  "$ROOT/spark_job/streaming_job.py"
