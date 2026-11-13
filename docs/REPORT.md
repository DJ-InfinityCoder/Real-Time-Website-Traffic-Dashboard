# Project Report: Real-Time Website Traffic Dashboard
Course: Big Data Analytics    |    Technologies: Apache Kafka, Apache Spark (Structured Streaming), Apache HBase

## 1. Problem statement and objective
Websites produce a continuous stream of access-log events. Batch analysis of these logs reveals problems hours late. The
objective is to build a pipeline that ingests web logs as they are produced, computes traffic metrics within seconds, stores them
in a scalable store, and displays them on a live dashboard so that traffic spikes, error bursts and suspicious clients are visible immediately.

## 2. Why Big Data technologies
| Requirement | Technology | Reason |
|---|---|---|
| High-throughput, durable ingestion, decoupling producers from consumers | Kafka | Partitioned, replayable log; the producer never waits for the processor |
| Continuous distributed computation with windows and aggregations | Spark Structured Streaming | DataFrame/SQL API, micro-batches, checkpointed offsets |
| Low-latency reads of the latest time ranges, huge write volume | HBase | Wide-column store with sorted row keys, atomic counters, TTLs |

## 3. Architecture
Producer -> Kafka topic `web-logs` (3 partitions) -> Spark Structured Streaming (2 s micro-batches) -> HBase (4 tables) -> Flask REST API -> browser dashboard (Chart.js, 2 s polling).
(Insert the diagram from README.)

## 4. Data
Each log event is JSON: timestamp (epoch ms), client IP, HTTP method, URL, status code, bytes sent, response time (ms), country, user agent.
A generator models a realistic page mix, 900 regular visitors, a day-style rate wobble, periodic traffic spikes (x3), 5xx error bursts on
`/checkout` and `/api/*`, three bot IPs attacking `/login`, and 0.2 % malformed messages to test data cleaning.

## 5. Implementation
### 5.1 Ingestion
`kafka-python` producer, messages keyed by client IP (same client -> same partition), batching with `linger_ms=20`.
### 5.2 Stream processing
1. `readStream` from Kafka; `from_json` with an explicit schema.
2. Cleaning: rows with a missing timestamp, IP, URL or status are counted as malformed and dropped; missing numeric fields default to 0.
3. Derived columns: second bucket from event time.
4. Aggregations per micro-batch: totals and status classes per second; hits and 5xx per page / country / IP per second; distinct (second, IP); latest 5xx events.
5. Output through `foreachBatch`: the small aggregates are collected and written to HBase with atomic increments.
### 5.3 HBase schema
See README section 4. Key idea: **reversed timestamp row keys** so the newest rows are scanned first, bounded range scans for any window,
**counters** so overlapping micro-batches add up, **TTL** to bound storage, a dedicated **visitors** table for exact unique counts within second-aligned windows.
### 5.4 Serving layer
`/api/snapshot` converts raw counters into dashboard metrics: requests/s, average response time (sum / count), error rates, bucketed time-series,
top-N lists, bot heuristic (>= 40 hits and >= 8x average hits per IP), 5xx alert (>= 5 % over the last 30 s).
### 5.5 Dashboard
Single page with KPI tiles, a traffic chart, a status doughnut, latency chart, top-N bars, error log; window selector 1/5/15 min; connection and data-freshness indicators.

## 6. Testing and results
* Automated tests: generator behaviour, row-key ordering and scan ranges, complete HBase write/read path (including counters accumulated over several batches, window exclusion, empty store, bot detection) and API endpoints.
* End-to-end run: record the figures from your own machine here, for example sustained throughput (events/s per batch from `logs/spark.log`), end-to-end latency (time from event to dashboard), screenshots of normal traffic, a spike, and an error burst.
* Alert timing depends on micro-batch and polling duration. The "no new data" banner appears when the newest data is older than 15 seconds. See VERIFICATION.md for actual local results.

## 7. Limitations and future work
* At-least-once delivery: a replayed batch may double count; fix with idempotent writes keyed by batch id.
* Default local deployment; three-PC Spark deployment supported (see CLUSTER.md). Kafka replication factor 1; HBase standalone.
* Row keys are not salted; at very high write rates, hot-spotting on the newest region would need a salt prefix.
* Polling instead of push (WebSocket / SSE).
* Possible extensions: sliding-window anomaly detection in Spark, geo-IP lookup, session analysis, raw-log archive in HDFS.

## 8. Conclusion
The system demonstrates a complete streaming analytics pipeline: ingest (Kafka), compute (Spark), store (HBase), and serve (REST + dashboard), with data-quality handling, a query-aware storage design, and live alerting.
