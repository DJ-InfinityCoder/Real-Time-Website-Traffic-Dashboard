# Sourced by every script. Defines paths and the environment for Kafka / Spark / HBase.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENDOR="$ROOT/vendor"; DATA="$ROOT/data"; LOGS="$ROOT/logs"; RUN="$ROOT/run"; VENV="$ROOT/.venv"
mkdir -p "$VENDOR" "$DATA" "$LOGS" "$RUN"

KAFKA_VERSION="${KAFKA_VERSION:-3.7.0}"
SPARK_VERSION="${SPARK_VERSION:-3.5.1}"
HBASE_VERSION="${HBASE_VERSION:-2.5.8}"

export KAFKA_HOME="$VENDOR/kafka"
export SPARK_HOME="$VENDOR/spark"
export HBASE_HOME="$VENDOR/hbase"

if [ -x "$VENDOR/java/bin/java" ] && [ -z "${JAVA_HOME:-}" ]; then
  export JAVA_HOME="$VENDOR/java"
  export PATH="$JAVA_HOME/bin:$PATH"
fi

# Java: use JAVA_HOME if set, otherwise derive it from `java` on the PATH
if [ -z "${JAVA_HOME:-}" ] && command -v java >/dev/null 2>&1; then
  export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
fi

export LOG_DIR="$LOGS/kafka"                 # Kafka daemon logs
export HBASE_LOG_DIR="$LOGS/hbase"
export HBASE_PID_DIR="$RUN/hbase"
export HBASE_HEAPSIZE="${HBASE_HEAPSIZE:-1G}"
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xmx512m -Xms512m}"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export PYSPARK_PYTHON="$VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VENV/bin/python"
export PATH="$VENV/bin:$PATH"

export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-web-logs}"
export HBASE_HOST="${HBASE_HOST:-localhost}"
export HBASE_PORT="${HBASE_PORT:-9090}"
export API_PORT="${API_PORT:-8050}"

port_open() { (echo > "/dev/tcp/$1/$2") >/dev/null 2>&1; }

wait_port() {  # wait_port host port name [seconds]
  local i
  for i in $(seq "${4:-90}"); do
    port_open "$1" "$2" && return 0
    sleep 1
  done
  echo "!! timed out waiting for $3 on $1:$2 - see $LOGS" >&2
  return 1
}
