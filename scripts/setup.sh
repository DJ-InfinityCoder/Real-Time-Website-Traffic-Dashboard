#!/usr/bin/env bash
# One-time setup: Python venv + local copies of Kafka, Spark and HBase (no Docker, no root needed).
set -euo pipefail
source "$(dirname "$0")/env.sh"

echo "== checking prerequisites"
command -v java    >/dev/null || { echo "Java not found. Install JDK 11 or 17 (e.g. sudo apt install openjdk-17-jdk)"; exit 1; }
command -v python3 >/dev/null || { echo "python3 not found"; exit 1; }
command -v curl    >/dev/null || { echo "curl not found"; exit 1; }
JV=$(java -version 2>&1 | head -1 | sed -E 's/.*"([0-9]+)(\.[0-9]+)*.*/\1/'); [ "$JV" = "1" ] && JV=8
echo "   java major version: $JV, JAVA_HOME=$JAVA_HOME"
case "$JV" in 8|11|17) ;; *) echo "   WARNING: Java $JV detected. Spark 3.5 + HBase 2.5 are tested on Java 8/11/17 - Java 11 or 17 is recommended." ;; esac

echo "== python virtualenv"
if [ ! -x "$VENV/bin/python" ]; then python3 -m venv "$VENV" || { echo "venv failed. On Ubuntu: sudo apt install python3-venv"; exit 1; }; fi
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -r "$ROOT/requirements.txt"

fetch() {  # fetch name url target_dir
  local name="$1" url="$2" dir="$3" tgz="$VENDOR/$1.tgz"
  if [ -x "$dir/bin/$4" ]; then echo "== $name already installed"; return; fi
  echo "== downloading $name  ($url)"
  curl -fL --retry 3 --progress-bar -o "$tgz" "$url"
  rm -rf "$dir"; mkdir -p "$dir"
  tar -xzf "$tgz" -C "$dir" --strip-components=1
  rm -f "$tgz"
}
APACHE="${APACHE_MIRROR:-https://archive.apache.org/dist}"
fetch kafka "${KAFKA_URL:-$APACHE/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz}" "$KAFKA_HOME" kafka-server-start.sh
fetch spark "${SPARK_URL:-$APACHE/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz}" "$SPARK_HOME" spark-submit
fetch hbase "${HBASE_URL:-$APACHE/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz}" "$HBASE_HOME" hbase

echo "== writing configuration"
sed "s|__DATA__|$DATA|g" "$ROOT/conf/kafka-server.properties.template" > "$ROOT/conf/kafka-server.properties"
sed "s|__DATA__|$DATA|g" "$ROOT/conf/hbase-site.xml.template" > "$HBASE_HOME/conf/hbase-site.xml"
grep -q "# set by traffic-dashboard" "$HBASE_HOME/conf/hbase-env.sh" || \
  echo "export JAVA_HOME=\"$JAVA_HOME\"   # set by traffic-dashboard" >> "$HBASE_HOME/conf/hbase-env.sh"

echo
echo "Setup complete. Next:  ./scripts/start_all.sh"
