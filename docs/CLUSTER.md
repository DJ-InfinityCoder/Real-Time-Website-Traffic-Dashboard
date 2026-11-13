# Project 11: three-PC demonstration

Use three physical PCs on one trusted LAN. The master runs Kafka, HBase, the Spark
master and streaming driver, producer, and dashboard. Two worker PCs run Spark
executors, which perform parsing and distributed aggregations. HBase is standalone
on the master in this topology; this is not a replicated HBase/HDFS cluster.

## Prepare all three PCs

Copy the project to the **same absolute path** on each PC. Install Java 11 and
Python 3.10 with venv, then run `./scripts/setup.sh` on each machine. Keep the same
Spark/Python versions on all nodes. Alternatively a JDK extracted into
`vendor/java` is detected automatically. Avoid sharing `.venv` across PCs.

Example addresses below are placeholders: replace with your actual LAN addresses.
Master: `192.168.1.10`; worker 1: `192.168.1.11`; worker 2: `192.168.1.12`.
Allow communication between these PCs for Kafka 9092, Spark master 7077, Spark
worker/driver/executor ports (Spark uses dynamic ports), and the dashboard 8050.
Do not expose these unauthenticated lab services to the public internet.

## Master terminal

After setup, before starting Kafka, edit `conf/kafka-server.properties`:

```properties
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://192.168.1.10:9092
```

Then:

```bash
MASTER_HOST=192.168.1.10 ./scripts/start_cluster_node.sh master
```

## Each worker terminal

On worker 1:

```bash
MASTER_HOST=192.168.1.10 WORKER_HOST=192.168.1.11 ./scripts/start_cluster_node.sh worker
```

On worker 2, use `WORKER_HOST=192.168.1.12`. Open the master's Spark UI at
`http://192.168.1.10:8080` and confirm **two alive workers** before proceeding.

## Run the pipeline on the master

```bash
export SPARK_MASTER=spark://192.168.1.10:7077
export SPARK_LOCAL_IP=192.168.1.10
export KAFKA_BOOTSTRAP=192.168.1.10:9092
./scripts/start_all.sh
```

Open `http://192.168.1.10:8050` from any PC. The job submits in client mode, so
its HBase sink runs on the master; workers do not need access to HBase Thrift.
`start_spark.sh` packages shared modules with `--py-files`. Each worker must have
the project venv at the same absolute path (`PYSPARK_PYTHON`).

## Evaluation evidence

1. Record PC names/IPs and roles, and a screenshot of two alive workers.
2. Capture the Spark application UI showing executors on both workers.
3. Run `./scripts/verify_pipeline.py` on the master and save its JSON output.
4. Show Kafka topic: `vendor/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic web-logs`.
5. Show HBase: `echo "scan 'traffic_metrics', {LIMIT => 3}" | vendor/hbase/bin/hbase shell -n`.
6. Demonstrate changing dashboard windows and producer stop/restart.
7. Submit the GitHub repository URL and your own measurements/screenshots.

Stop application services with `./scripts/stop_all.sh` on the master.
Stop each worker with `vendor/spark/sbin/stop-worker.sh` and the master with
`vendor/spark/sbin/stop-master.sh`, after sourcing `scripts/env.sh` and setting
`SPARK_PID_DIR="$RUN/spark-cluster"` on the corresponding PC.

Three physical PCs are not available in the development workspace. These steps
provide the deployment path; the distributed demonstration must be run on your PCs.
