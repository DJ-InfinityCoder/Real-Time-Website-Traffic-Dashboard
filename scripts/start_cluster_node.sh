#!/usr/bin/env bash
# Start the Spark role on this PC; run once on the master and on each worker.
set -euo pipefail
source "$(dirname "$0")/env.sh"
role="${1:-}"
: "${MASTER_HOST:?Set MASTER_HOST to the master PC LAN IP or resolvable hostname}"
export SPARK_LOG_DIR="$LOGS/spark-cluster"
export SPARK_PID_DIR="$RUN/spark-cluster"
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
case "$role" in
  master)
    export SPARK_MASTER_HOST="$MASTER_HOST"
    exec "$SPARK_HOME/sbin/start-master.sh"
    ;;
  worker)
    export SPARK_LOCAL_IP="${WORKER_HOST:?Set WORKER_HOST to this worker PC LAN IP}"
    export SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-2}"
    export SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-2g}"
    exec "$SPARK_HOME/sbin/start-worker.sh" "spark://$MASTER_HOST:7077"
    ;;
  *) echo "Usage: MASTER_HOST=192.168.1.10 [WORKER_HOST=...] $0 master|worker" >&2; exit 2 ;;
esac
