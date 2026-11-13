"""Spark Structured Streaming job:  Kafka (web-logs)  ->  clean + aggregate  ->  HBase.

Run with scripts/start_spark.sh (which sets the Kafka package, PYTHONPATH, etc.).

Pipeline per micro-batch (every SPARK_TRIGGER_SECONDS):
  1. Parse the JSON value with an explicit schema; rows that fail to parse are counted as `malformed`
     and dropped (data cleaning).
  2. Derive event-time columns: second bucket.
  3. Aggregate with Spark SQL:
        - per-second totals (requests, bytes, response-time sum, 2xx/3xx/4xx/5xx counts)
        - per-second top pages / countries / client IPs
        - distinct visitors per second
        - the latest 5xx error events (for the live error log)
  4. The aggregates are tiny, so they are collected to the driver and written to HBase with atomic
     counter increments (common/hbase_writer.py). Because counters are additive, a second whose events
     arrive in several micro-batches is still summed correctly.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import happybase  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import LongType, StringType, StructField, StructType  # noqa: E402

from common import config  # noqa: E402
from common.hbase_writer import write_aggregates  # noqa: E402

MAX_ERRORS = 25

EVENT_SCHEMA = StructType([
    StructField("ts", LongType()),          # epoch milliseconds
    StructField("ip", StringType()),
    StructField("method", StringType()),
    StructField("url", StringType()),
    StructField("status", LongType()),
    StructField("bytes", LongType()),
    StructField("rt_ms", LongType()),
    StructField("country", StringType()),
    StructField("ua", StringType()),
])


def status_count(klass: int):
    lo, hi = klass * 100, klass * 100 + 99
    return F.sum(F.when((F.col("status") >= lo) & (F.col("status") <= hi), 1).otherwise(0))


def process_batch(batch_df, batch_id):
    started = time.time()
    batch_df = batch_df.cache()
    total = batch_df.count()
    if total == 0:
        batch_df.unpersist()
        return

    valid_mask = (
        (F.col("e.ts") > 0)
        & (F.col("e.ts") <= int(time.time() * 1000) + 5000)
        & (F.length(F.trim(F.col("e.ip"))) > 0)
        & (F.length(F.trim(F.col("e.url"))) > 0)
        & F.col("e.status").between(200, 599)
        & (F.coalesce(F.col("e.bytes"), F.lit(0)) >= 0)
        & (F.coalesce(F.col("e.rt_ms"), F.lit(0)) >= 0)
    )
    good = (
        batch_df.filter(valid_mask)
        .select("e.*")
        .withColumn("bytes", F.coalesce(F.col("bytes"), F.lit(0)))
        .withColumn("rt_ms", F.coalesce(F.col("rt_ms"), F.lit(0)))
        .withColumn("country", F.coalesce(F.col("country"), F.lit("??")))
        .withColumn("sec", (F.col("ts") / 1000).cast("long"))
        .cache()
    )
    good_count = good.count()
    malformed = total - good_count

    per_sec = good.groupBy("sec").agg(
        F.count("*").alias("req"),
        F.sum("bytes").alias("bytes"),
        F.sum("rt_ms").alias("rt_sum"),
        status_count(2).alias("s2xx"),
        status_count(3).alias("s3xx"),
        status_count(4).alias("s4xx"),
        status_count(5).alias("s5xx"),
    ).collect()

    def dim(column):
        return (
            good.groupBy("sec", F.col(column).alias("value"))
            .agg(F.count("*").alias("hits"), F.sum(F.when(F.col("status") >= 500, 1).otherwise(0)).alias("errors"))
            .collect()
        )

    agg = {
        "now": int(time.time()),
        "malformed": malformed,
        "per_sec": [r.asDict() for r in per_sec],
        "dims": {
            "page": [r.asDict() for r in dim("url")],
            "country": [r.asDict() for r in dim("country")],
            "ip": [r.asDict() for r in dim("ip")],
        },
        "visitors": [r.asDict() for r in good.select("sec", "ip").distinct().collect()],
        "errors": [r.asDict() for r in good.filter(F.col("status") >= 500).orderBy(F.desc("ts")).limit(MAX_ERRORS).collect()],
    }
    good.unpersist()
    batch_df.unpersist()

    conn = happybase.Connection(config.HBASE_HOST, config.HBASE_PORT, timeout=10000)
    try:
        ops = write_aggregates(conn, agg)
    finally:
        conn.close()
    print(f"[batch {batch_id}] events={total} malformed={malformed} hbase_ops={ops} took={time.time() - started:.2f}s", flush=True)


def main():
    spark = (
        SparkSession.builder.appName("WebTrafficDashboard")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("maxOffsetsPerTrigger", int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000")))
        .option("failOnDataLoss", "true")
        .load()
    )
    parsed = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))

    query = (
        parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", config.CHECKPOINT_DIR)
        .trigger(processingTime=f"{config.SPARK_TRIGGER_SECONDS} seconds")
        .start()
    )
    print(f"[spark] streaming from {config.KAFKA_BOOTSTRAP}/{config.KAFKA_TOPIC} -> HBase {config.HBASE_HOST}:{config.HBASE_PORT}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
