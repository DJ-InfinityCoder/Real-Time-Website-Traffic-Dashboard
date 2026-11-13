#!/usr/bin/env bash
# Starts Kafka (KRaft), HBase (standalone) and the HBase Thrift gateway, then creates topic + tables.
set -euo pipefail
source "$(dirname "$0")/env.sh"
[ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ] || { echo "Run ./scripts/setup.sh first"; exit 1; }

echo "== Kafka"
if port_open localhost 9092; then echo "   already running"; else
  if [ ! -f "$DATA/kafka/meta.properties" ]; then
    "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" -c "$ROOT/conf/kafka-server.properties" >/dev/null
  fi
  "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$ROOT/conf/kafka-server.properties"
  wait_port localhost 9092 Kafka 60
fi
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic "$KAFKA_TOPIC" --partitions 3 --replication-factor 1 >/dev/null
echo "   topic '$KAFKA_TOPIC' ready"

echo "== HBase (standalone)"
if port_open localhost 16000 || port_open localhost 16010; then echo "   already running"; else
  "$HBASE_HOME/bin/start-hbase.sh" >/dev/null
  wait_port localhost 16010 "HBase master" 120
fi

echo "== HBase Thrift gateway (port $HBASE_PORT)"
if port_open "$HBASE_HOST" "$HBASE_PORT"; then echo "   already running"; else
  nohup "$HBASE_HOME/bin/hbase" thrift start -p "$HBASE_PORT" > "$LOGS/hbase-thrift.log" 2>&1 &
  echo $! > "$RUN/thrift.pid"
  wait_port "$HBASE_HOST" "$HBASE_PORT" "HBase Thrift" 90
fi

echo "== HBase tables"
tables_ready=false
for i in $(seq 12); do
  if "$VENV/bin/python" "$ROOT/scripts/create_tables.py"; then tables_ready=true; break; fi
  echo "   HBase is still initializing; retrying table creation ..."
  sleep 3
done
$tables_ready || { echo "HBase tables unavailable; see $LOGS/hbase" >&2; exit 1; }
echo "Infrastructure is up."
