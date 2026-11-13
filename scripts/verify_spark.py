"""Run with vendor/spark/bin/spark-submit scripts/verify_spark.py.

Tests real Spark parsing/aggregation and HBase writes against the in-memory fake.
No Kafka or HBase is required. This complements verify_pipeline.py.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession, functions as F
from spark_job import streaming_job as job
from tests.fake_happybase import FakeConnection
from api.stores import HBaseStore

spark = SparkSession.builder.master('local[2]').appName('TrafficAggregationVerification').config('spark.sql.shuffle.partitions', '2').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
conn = FakeConnection()
job.happybase.Connection = lambda *a, **k: conn
now = int(time.time())
events = [dict(ts=(now-3)*1000, ip=f'10.0.0.{i}', method='GET', url=f'/page/{i}',
               status=503 if i % 10 == 0 else 200, bytes=100, rt_ms=20, country='IN')
          for i in range(80)]
bad = [dict(events[0], status=700), dict(events[0], bytes=-1),
       dict(events[0], ip=''), dict(events[0], rt_ms=-1), dict(events[0], ts=-1)]
raw = spark.createDataFrame([(json.dumps(e),) for e in events + bad] + [('bad json',)], ['value'])
parsed = raw.select(F.from_json('value', job.EVENT_SCHEMA).alias('e'))
try:
    job.process_batch(parsed, 0)
    store = HBaseStore()
    store._conn = lambda: conn
    snap = store.snapshot(60, int(time.time()))
    assert snap['summary']['requests'] == 80, snap['summary']
    assert snap['summary']['unique_visitors'] == 80, snap['summary']
    assert snap['summary']['malformed'] == 6, snap['summary']
    assert len(store.fetch_dim('page', now, 60)) == 80
    assert len(store.fetch_dim('ip', now, 60)) == 80
    assert snap['status']['s5xx'] == 8
    print('PASS: real Spark aggregates all 80 pages/IPs, counts visitors and rejects 6 invalid events')
finally:
    spark.stop()
