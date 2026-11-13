# Real-Time Website Traffic Dashboard

Big Data Analytics course project. Web-server logs are streamed through **Kafka**, processed by
**Spark Structured Streaming**, stored in **HBase**, and shown on a live dashboard that refreshes every 2 seconds.
**No Docker**: everything runs as ordinary local processes.

```
 log producer         Kafka               Spark Structured Streaming            HBase               Flask API        Browser
 (Python)  ──JSON──▶  topic web-logs ──▶  parse · clean · aggregate  ──────▶  4 tables  ◀──scan──  /api/snapshot ◀── dashboard
 ~60 req/s            3 partitions        micro-batch every 2 s               (counters)           (happybase)       (Chart.js)
```

## What the dashboard shows
Requests/sec (live, average, peak) · total requests · unique visitors · 5xx error rate with an alert banner ·
average response time · bandwidth · traffic time-series (total, 4xx, 5xx) · response-time chart ·
HTTP status distribution · top pages · traffic by country · busiest IPs with a "bot?" flag · live log of the latest 5xx errors ·
1 / 5 / 15 minute windows.

The producer deliberately creates **traffic spikes**, **5xx error bursts** and **bot traffic**, so every panel has something to show.

---

## 1. Requirements
| Need | Notes |
|---|---|
| Linux, macOS, or Windows with **WSL2** (Ubuntu) | Run everything inside the Linux shell |
| **Java 11 or 17** | `sudo apt install openjdk-17-jdk` · `java -version` to check |
| **Python 3.9 – 3.11** with `venv` | `sudo apt install python3-venv` |
| `curl`, ~2 GB free disk, 4 GB+ RAM | Kafka + Spark + HBase together use roughly 2.5 GB |
| Internet (first run only) | downloads Kafka, Spark, HBase, and Spark's Kafka connector (from Maven) |

## 2. Run the full pipeline
```bash
unzip traffic-dashboard.zip && cd traffic-dashboard

./scripts/setup.sh        # once: venv + downloads Kafka/Spark/HBase into ./vendor (~800 MB)
./scripts/start_all.sh    # starts Kafka, HBase, Spark job, API, producer
```
Open **http://localhost:8050**. Data appears 5–10 seconds after the first Spark batch.

Useful commands
```bash
./scripts/status.sh       # which services are up + last Spark batches
tail -f logs/spark.log    # one line per micro-batch:  events=…  malformed=…  hbase_ops=…
RATE=300 ./scripts/start_all.sh        # heavier traffic (requests/sec)
./scripts/stop_all.sh                  # stop everything     (--wipe also deletes stored data)
```
Web UIs: Spark `http://localhost:4040` · HBase master `http://localhost:16010`.

### Run each component yourself (to see what each does)
Use five terminals, from the project folder:
```bash
./scripts/start_infra.sh                 # 1  Kafka + HBase + Thrift, creates topic and tables
./scripts/start_spark.sh                 # 2  Spark streaming job
./scripts/start_dashboard.sh             # 3  Flask API + dashboard
./scripts/start_producer.sh --rate 100   # 4  log generator
python3 scripts/create_tables.py --reset # (optional) empty the HBase tables
```
Producer options: `--rate N`, `--spike-every SEC`, `--error-every SEC`, `--duration SEC`, `--stdout` (print instead of Kafka).

### Quick UI preview (no Kafka / Spark / HBase)
```bash
./scripts/run_demo.sh      # needs only Python; serves simulated data at http://localhost:8050
```
Demo mode is only a preview of the interface. It is labelled "demo data" in the dashboard.

## 3. Verify it works

See [docs/VERIFICATION.md](docs/VERIFICATION.md) for the actual local verification results.
```bash
python3 -m unittest discover -s tests -v     # no services needed
```
The tests check the log generator, the HBase row-key design (newest-first, exact scan ranges), the complete HBase
write → read path against a fake HBase (including counters that receive data from several micro-batches), and the REST API.

Manual end-to-end checks while running:
* `curl localhost:8050/api/snapshot?window=60` returns live JSON.
* Kafka: `vendor/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic web-logs --max-messages 3`
* HBase shell: `vendor/hbase/bin/hbase shell` then `scan 'traffic_metrics', {LIMIT => 3}`
* Stop the producer: after ~15 s the dashboard shows "No new data". Restart it: traffic resumes.
* Wait for an error burst (every 150 s) or run `./scripts/start_producer.sh --error-every 30`: the red banner appears.

## 4. How it works

**Producer** (`producer/`) – generates JSON events `{ts, ip, method, url, status, bytes, rt_ms, country, ua}` with a
weighted page mix, a pool of regular visitors, spikes, error bursts, bot IPs, and a few deliberately malformed messages.
Messages are keyed by client IP.

**Spark job** (`spark_job/streaming_job.py`) – reads the topic with `readStream`, parses JSON against an explicit schema,
drops/counts malformed rows, and in each 2-second micro-batch computes with DataFrame aggregations: per-second totals and status
classes, per-second top pages / countries / IPs, distinct visitors, and the latest 5xx events.

**HBase** (`common/schema.py`) – row keys start with a *reversed timestamp* so the newest data sorts first and the
dashboard reads a time window with one short range scan:

| Table | Row key | Columns |
|---|---|---|
| `traffic_metrics` | `rev(second)` | `m:req bytes rt_sum s2xx s3xx s4xx s5xx bad` |
| `dim_counts` | `page\|country\|ip # rev(second) # value` | `c:hits c:errors` |
| `visitors` | `rev(second) # ip` | `v:1` (exact unique counting) |
| `recent_errors` | `rev(ms) # nonce` | `e:j` (JSON, TTL 1 h) |

Numbers are written with HBase atomic **counter increments**, so one second of traffic that is split across two micro-batches is
still summed correctly. Tables have TTLs so storage stays bounded.

**API** (`api/`) – Flask + happybase. `GET /api/snapshot?window=300` returns everything the page needs in one call;
also `/api/timeseries`, `/api/top/<page|country|ip>`, `/api/health`.

**Dashboard** (`dashboard/index.html`) – plain HTML/JS + bundled Chart.js (charts work offline).

## 5. Design decisions (good viva answers)
* **Structured Streaming, not DStreams** – the current Spark API; DataFrame/SQL aggregations with exactly-once-capable checkpointing.
* **`foreachBatch` + HBase counters** – HBase has no official Structured Streaming sink; `foreachBatch` gives full control, and because the
  per-batch aggregates are tiny (hundreds of rows) collecting them to the driver is cheap.
* **Reversed-timestamp row keys** – "latest N seconds" becomes a bounded scan instead of a table scan.
* **Separate `visitors` table** – distinct counts are not additive across batches, so unique visitors are stored as rows and counted exactly at read time.
* **Kafka in KRaft mode** – no ZooKeeper to run; HBase brings its own embedded ZooKeeper in standalone mode.
* **Delivery semantics** – Kafka offsets are checkpointed, so the pipeline is *at-least-once*: a batch replayed after a crash could add
  its counters twice. Making it exactly-once would require idempotent writes keyed by `batch_id` (listed under future work).
* **Latency** – about 2 s trigger + ≤2 s dashboard refresh. The newest second is skipped when computing "requests/sec now", because it is still filling.

## 6. Troubleshooting
| Symptom | Fix |
|---|---|
| `setup.sh` download fails | Re-run (it resumes per component). Set `APACHE_MIRROR=https://dlcdn.apache.org` or any Apache mirror, or `KAFKA_URL=…` |
| Dashboard says "backend unreachable" | `./scripts/status.sh`; HBase Thrift must be UP on 9090. See `logs/hbase-thrift.log` and `logs/hbase/` |
| "No new data" banner | Producer or Spark not running: `tail logs/spark.log logs/producer.log` |
| Spark: `Failed to find data source: kafka` | The `--packages` download failed (no internet during first run). Re-run `./scripts/start_spark.sh` |
| Spark errors on `Unsupported class file major version` | Wrong Java version; use Java 11 or 17 |
| Kafka `Address already in use` | Another Kafka is running on 9092: stop it or `./scripts/stop_all.sh` |
| HBase won't start | Check `logs/hbase/*.log`; make sure `JAVA_HOME` points to Java 11/17; `./scripts/stop_all.sh --wipe` for a clean start |
| Port 8050 busy | `API_PORT=8060 ./scripts/start_all.sh` |
| Want a clean slate | `./scripts/stop_all.sh --wipe` then `./scripts/start_all.sh` |
| Spark fails after you wiped Kafka data only | `./scripts/start_spark.sh --reset` (clears its checkpoint) |

## 7. Project layout
```
producer/     generator.py, log_producer.py       simulated logs -> Kafka
spark_job/    streaming_job.py                    Structured Streaming job
common/       config.py, schema.py, hbase_writer.py   shared settings, HBase design, write path
api/          app.py, stores.py                   Flask REST API (HBase + demo back-ends)
dashboard/    index.html                          live UI
scripts/      setup / start_* / stop_all / status install & run without Docker
conf/         Kafka and HBase config templates
tests/        unit tests + fake HBase
docs/         REPORT.md                           report outline for submission
```

## Three-PC course requirement

The default setup runs on one PC for development. For the required one-master,
two-worker demonstration, follow [docs/CLUSTER.md](docs/CLUSTER.md). It runs Spark
executors on both workers and Kafka/HBase/API on the master. This is not a
replicated HBase cluster.

## Verification and accuracy

Run `.venv/bin/python scripts/verify_pipeline.py` while the full pipeline is up.
It publishes a uniquely identified event to Kafka and requires that exact page
and client to appear through Spark, HBase and the HTTP API. Demo mode is rejected.

Dimension and visitor keys now use second resolution, matching request windows.
If upgrading an existing installation, archive/reset the old tables and Spark
checkpoint before use; minute-resolution rows cannot be migrated automatically.
Spark no longer discards non-top dimensions within each batch.
The sink remains at-least-once: replay after a failure can double counters.
For this lab, aggregates are collected on the driver; this limits scale.
