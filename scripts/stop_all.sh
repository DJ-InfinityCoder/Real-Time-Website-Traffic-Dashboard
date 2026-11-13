#!/usr/bin/env bash
# Stops everything started by start_all.sh. Add --wipe to also delete all stored data.
source "$(dirname "$0")/env.sh"
for name in producer dashboard spark thrift; do
  if [ -f "$RUN/$name.pid" ]; then
    pid=$(cat "$RUN/$name.pid")
    if kill -0 "$pid" 2>/dev/null; then pkill -P "$pid" 2>/dev/null || true; kill "$pid" 2>/dev/null || true; echo "stopped $name"; fi
    rm -f "$RUN/$name.pid"
  fi
done
[ -x "$HBASE_HOME/bin/stop-hbase.sh" ] && "$HBASE_HOME/bin/stop-hbase.sh" 2>/dev/null | tail -1 || true
[ -x "$KAFKA_HOME/bin/kafka-server-stop.sh" ] && "$KAFKA_HOME/bin/kafka-server-stop.sh" 2>/dev/null || true
if [ "${1:-}" = "--wipe" ]; then sleep 3; rm -rf "$DATA" "$LOGS" "$RUN"; echo "data wiped"; fi
echo "done"
