#!/usr/bin/env bash
source "$(dirname "$0")/env.sh"
chk() { if port_open "$2" "$3"; then printf "  %-22s UP   (%s:%s)\n" "$1" "$2" "$3"; else printf "  %-22s DOWN (%s:%s)\n" "$1" "$2" "$3"; fi; }
chk "Kafka broker" localhost 9092
chk "HBase master UI" localhost 16010
chk "HBase Thrift" "$HBASE_HOST" "$HBASE_PORT"
chk "Spark UI" localhost 4040
chk "Dashboard/API" localhost "$API_PORT"
echo; echo "Last Spark micro-batches:"; tail -n 3 "$LOGS/spark.log" 2>/dev/null | grep "batch" || echo "  (none yet)"
