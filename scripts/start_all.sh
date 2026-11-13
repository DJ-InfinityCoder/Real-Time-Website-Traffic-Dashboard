#!/usr/bin/env bash
# Starts the complete pipeline in the background:  Kafka + HBase  ->  Spark job  ->  API/dashboard  <-  producer
set -euo pipefail
source "$(dirname "$0")/env.sh"
"$ROOT/scripts/start_infra.sh"

launch() {  # launch name command...
  local name="$1"; shift
  if [ -f "$RUN/$name.pid" ] && kill -0 "$(cat "$RUN/$name.pid")" 2>/dev/null; then echo "   $name already running"; return; fi
  nohup "$@" > "$LOGS/$name.log" 2>&1 &
  echo $! > "$RUN/$name.pid"
  echo "   started $name (pid $!, log: logs/$name.log)"
}

echo "== application"
launch spark     "$ROOT/scripts/start_spark.sh"
launch dashboard "$ROOT/scripts/start_dashboard.sh"
echo "   waiting for Spark to subscribe to Kafka ..."
ready=false
for i in $(seq 120); do
  if grep -q "streaming from" "$LOGS/spark.log" 2>/dev/null; then ready=true; break; fi
  if ! kill -0 "$(cat "$RUN/spark.pid")" 2>/dev/null; then
    echo "Spark exited. See $LOGS/spark.log" >&2; exit 1
  fi
  sleep 1
done
$ready || { echo "Spark startup timed out. See $LOGS/spark.log" >&2; exit 1; }
wait_port localhost "$API_PORT" Dashboard 30
"$VENV/bin/python" -c "import urllib.request; urllib.request.urlopen('http://localhost:$API_PORT/api/health', timeout=10)"
launch producer  "$ROOT/scripts/start_producer.sh" --rate "${RATE:-60}"

echo
echo "Everything is running."
echo "  Dashboard : http://localhost:$API_PORT"
echo "  Spark UI  : http://localhost:4040"
echo "  HBase UI  : http://localhost:16010"
echo "  Logs      : tail -f logs/spark.log   (one line per micro-batch)"
echo "  Stop      : ./scripts/stop_all.sh"
