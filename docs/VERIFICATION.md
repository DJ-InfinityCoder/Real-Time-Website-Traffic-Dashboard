# Verified local run — 13 November 2026

Environment: Python 3.10.12, project-local Temurin JDK 11, Kafka 3.7.0,
Spark 3.5.1, HBase 2.5.8. Apache runtimes were installed using
`APACHE_MIRROR=https://repo.huaweicloud.com/apache ./scripts/setup.sh` because
the archive mirror was slow. Runtime downloads and `.venv` are ignored by Git.

## Results

- `.venv/bin/python -m unittest discover -s tests -v`: **15 passed**.
- `source scripts/env.sh && vendor/spark/bin/spark-submit scripts/verify_spark.py`:
  **PASS** with real Spark. All 80 distinct pages and IPs retained, 80 unique
  visitors counted, 6 invalid events rejected, 8 server errors counted. HBase
  writes in this isolated check use the fake connection.
- `./scripts/start_all.sh`: real Kafka, HBase/Thrift, Spark, producer and API
  started successfully.
- `.venv/bin/python scripts/verify_pipeline.py`: **PASS**, real Kafka → Spark →
  HBase → HTTP API. The unique probe page had 1 HBase hit; the API was in
  `hbase` mode, returned 982 requests, and reported data age 2 seconds. These
  are one-run observations, not benchmark guarantees.
- Observed steady batches in that run took approximately 1.3–1.7 seconds.
- Browser preview rendered metrics/charts and the one-minute window control.
- Real HBase dashboard also rendered live metrics and charts.
- All three API windows (60/300/900 seconds) returned consistent status totals;
  observed response times were about 0.14–0.17 seconds on the short local run.

## Reproduce

```bash
./scripts/start_all.sh
.venv/bin/python scripts/verify_pipeline.py
.venv/bin/python -m unittest discover -s tests -v
./scripts/status.sh
```

Open <http://localhost:8050>. Stop with `./scripts/stop_all.sh`.
The probe deliberately inserts one HTTP 503 event under `/verify/<unique-id>`.

## Remaining limits

The two physical workers were not available, so a three-PC execution has not
been verified. Follow CLUSTER.md on your LAN and capture executor/worker evidence.
The HBase counter sink is at-least-once: crash replay can double counts.
The producer supplies simulated access logs; it does not collect a website's
actual Nginx/Apache logs. Driver-side collection and counter RPCs limit scale.
There is no replicated HBase/HDFS deployment or production authentication.
This workspace has no usable Git metadata; create/push your GitHub submission
from a normal repository checkout, excluding ignored runtime and data folders.
