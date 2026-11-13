"""Run:  python -m unittest discover -s tests -v   (from the project root)

Covers everything that does not need a running Kafka/Spark/HBase:
  - the log generator
  - HBase row-key design (newest-first, correct scan ranges)
  - the HBase write path (hbase_writer) + read path (HBaseStore) against a fake HBase
  - the in-memory demo store and the Flask API
"""
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api.app import create_app  # noqa: E402
from api.stores import HBaseStore, MemoryStore  # noqa: E402
from common import schema as S  # noqa: E402
from common.hbase_writer import write_aggregates  # noqa: E402
from fake_happybase import FakeConnection  # noqa: E402
from producer.generator import TrafficGenerator  # noqa: E402


def ev(ts, url="/", status=200, ip="1.1.1.1", country="US", rt=100, size=1000):
    return {"ts": ts * 1000, "ip": ip, "method": "GET", "url": url, "status": status,
            "bytes": size, "rt_ms": rt, "country": country, "ua": "x"}


def spark_style_aggregate(events, now):
    """Pure-Python equivalent of what streaming_job.process_batch computes."""
    per_sec, dims, visitors, errors = {}, {d: {} for d in S.DIMENSIONS}, set(), []
    for e in events:
        sec = e["ts"] // 1000
        minute = sec
        r = per_sec.setdefault(sec, {"sec": sec, "req": 0, "bytes": 0, "rt_sum": 0, "s2xx": 0, "s3xx": 0, "s4xx": 0, "s5xx": 0})
        r["req"] += 1
        r["bytes"] += e["bytes"]
        r["rt_sum"] += e["rt_ms"]
        r[f"s{e['status'] // 100}xx"] += 1
        for dim, v in (("page", e["url"]), ("country", e["country"]), ("ip", e["ip"])):
            c = dims[dim].setdefault((minute, v), {"sec": minute, "value": v, "hits": 0, "errors": 0})
            c["hits"] += 1
            c["errors"] += int(e["status"] >= 500)
        visitors.add((minute, e["ip"]))
        if e["status"] >= 500:
            errors.append(e)
    return {
        "now": now, "malformed": 0, "per_sec": list(per_sec.values()),
        "dims": {d: list(v.values()) for d, v in dims.items()},
        "visitors": [{"sec": m, "ip": ip} for m, ip in visitors], "errors": errors,
    }


class GeneratorTests(unittest.TestCase):
    def test_events_are_valid(self):
        gen = TrafficGenerator(base_rate=100, seed=1)
        events = gen.events(1.0)
        self.assertGreater(len(events), 30)
        for e in events:
            self.assertEqual(set(e), {"ts", "ip", "method", "url", "status", "bytes", "rt_ms", "country", "ua"})
            self.assertIn(e["status"] // 100, (2, 3, 4, 5))
            self.assertTrue(e["url"].startswith("/"))

    def test_error_burst_raises_5xx(self):
        gen = TrafficGenerator(base_rate=200, error_every=100, spike_every=0, seed=2)
        calm = gen._event  # noqa
        t_burst = 105  # (105 % 100) < 20 and t > 40
        self.assertTrue(gen.in_error_burst(t_burst))
        burst = [gen._event(0, t_burst) for _ in range(3000)]
        quiet = [gen._event(0, 60) for _ in range(3000)]
        rate = lambda xs: sum(e["status"] >= 500 for e in xs) / len(xs)
        self.assertGreater(rate(burst), rate(quiet) * 4)


class SchemaTests(unittest.TestCase):
    def test_newest_sorts_first(self):
        self.assertLess(S.sec_row(2000), S.sec_row(1000))
        self.assertLess(S.error_row(2_000_000, "a"), S.error_row(1_000_000, "a"))

    def test_sec_scan_window(self):
        now, n = 1_700_000_100, 10
        start, stop = S.sec_scan(now, n)
        inside = [s for s in range(now - 20, now + 5) if start <= S.sec_row(s) < stop]
        self.assertEqual(min(inside), now - n + 1)
        self.assertEqual(max(inside), now)

    def test_dim_scan_includes_current_and_oldest_minute_only(self):
        now, window = 1_700_000_130, 120
        start, stop = S.dim_scan("page", now, window)
        keep = [m for m in range(S.minute_of(now) - 600, S.minute_of(now) + 300, 60)
                if start <= S.dim_row("page", m, "/x") < stop]
        self.assertGreaterEqual(min(keep), now - window + 1)
        self.assertLessEqual(max(keep), now)
        self.assertFalse(start <= S.dim_row("country", S.minute_of(now), "US") < stop)  # other dim excluded


class HBasePathTests(unittest.TestCase):
    def setUp(self):
        self.now = int(time.time())
        self.conn = FakeConnection()
        store = HBaseStore()
        store._conn = lambda: self.conn
        self.store = store

    def feed(self, events):
        write_aggregates(self.conn, spark_style_aggregate(events, self.now))

    def test_roundtrip_totals_and_counters_across_batches(self):
        n = self.now - 5
        batch1 = [ev(n, "/", 200, "1.1.1.1"), ev(n, "/cart", 500, "2.2.2.2", "IN", rt=300)]
        batch2 = [ev(n, "/", 200, "1.1.1.1"), ev(n - 1, "/", 404, "3.3.3.3", "GB")]   # same second, later micro-batch
        self.feed(batch1)
        self.feed(batch2)
        snap = self.store.snapshot(60, self.now)
        s = snap["summary"]
        self.assertEqual(s["requests"], 4)
        self.assertEqual(s["unique_visitors"], 3)
        self.assertEqual(snap["status"], {"s2xx": 2, "s3xx": 0, "s4xx": 1, "s5xx": 1})
        self.assertAlmostEqual(s["err5_rate"], 0.25)
        self.assertEqual(s["avg_rt"], round((100 + 300 + 100 + 100) / 4, 1))
        self.assertEqual(snap["top_pages"][0]["value"], "/")
        self.assertEqual(snap["top_pages"][0]["hits"], 3)
        self.assertEqual({c["value"] for c in snap["top_countries"]}, {"US", "IN", "GB"})
        self.assertEqual(len(snap["errors"]), 1)
        self.assertEqual(snap["errors"][0]["status"], 500)
        self.assertEqual(sum(p["rps"] for p in snap["series"]) * snap["bucket"], 4)

    def test_events_outside_window_are_excluded(self):
        self.feed([ev(self.now - 400, "/old"), ev(self.now - 3, "/new")])
        snap = self.store.snapshot(60, self.now)
        self.assertEqual(snap["summary"]["requests"], 1)
        self.assertEqual([p["value"] for p in snap["top_pages"]], ["/new"])

    def test_empty_store(self):
        snap = self.store.snapshot(60, self.now)
        self.assertEqual(snap["summary"]["requests"], 0)
        self.assertIsNone(snap["data_age"])
        self.assertEqual(snap["top_pages"], [])

    def test_bot_flagged_suspicious(self):
        n = self.now - 5
        events = [ev(n, "/login", 401, "9.9.9.9") for _ in range(120)] + [ev(n, "/", 200, f"10.0.0.{i}") for i in range(80)]
        self.feed(events)
        ips = self.store.snapshot(60, self.now)["top_ips"]
        self.assertEqual(ips[0]["value"], "9.9.9.9")
        self.assertTrue(ips[0]["suspicious"])
        self.assertFalse(ips[1]["suspicious"])


class DemoApiTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        gen = TrafficGenerator(base_rate=80, seed=3)
        now = time.time()
        for i in range(60):
            self.store.ingest(gen.events(1.0, now - 60 + i))
        self.client = create_app(self.store).test_client()

    def test_endpoints(self):
        self.assertEqual(self.client.get("/api/health").get_json()["mode"], "demo")
        snap = self.client.get("/api/snapshot?window=60").get_json()
        self.assertGreater(snap["summary"]["requests"], 1000)
        self.assertEqual(len(snap["top_pages"]), 10)
        self.assertEqual(sum(snap["status"].values()), snap["summary"]["requests"])
        self.assertEqual(self.client.get("/api/top/country?n=3").get_json()["rows"].__len__(), 3)
        self.assertEqual(self.client.get("/api/top/nope").status_code, 400)
        with self.client.get("/") as response:
            self.assertIn(b"<html", response.data.lower())

    def test_window_is_clamped(self):
        self.assertEqual(self.client.get("/api/snapshot?window=999999").get_json()["window"], 1800)
        self.assertEqual(self.client.get("/api/snapshot?window=abc").get_json()["window"], 300)


class WindowRegressionTests(unittest.TestCase):
    def test_boundary_excludes_old_same_minute_and_future(self):
        now = 1_700_000_130
        events = [ev(now - 60, '/old', ip='old'), ev(now - 59, '/in', ip='in'),
                  ev(now, '/in', ip='in'), ev(now + 1, '/future', ip='future')]
        memory = MemoryStore()
        memory.ingest(events)
        conn = FakeConnection()
        write_aggregates(conn, spark_style_aggregate(events, now))
        hbase = HBaseStore()
        hbase._conn = lambda: conn
        for store in (memory, hbase):
            snap = store.snapshot(60, now)
            self.assertEqual(snap['summary']['requests'], 2)
            self.assertEqual(snap['summary']['unique_visitors'], 1)
            self.assertEqual([(r['value'], r['hits']) for r in snap['top_pages']], [('/in', 2)])

    def test_chart_does_not_include_data_before_window(self):
        store = MemoryStore()
        now = 1_700_000_131
        store.ingest([ev(now - 301), ev(now - 300), ev(now)])
        points, bucket = store.timeseries(301, now)
        self.assertEqual(points[0]['t'], now - 300)
        self.assertAlmostEqual(sum(p['rps'] * min(bucket, now - p['t'] + 1) for p in points), 2)

    def test_backend_failure_is_503(self):
        class Broken(MemoryStore):
            def ping(self):
                raise ConnectionError('unavailable')
        client = create_app(Broken()).test_client()
        self.assertEqual(client.get('/api/health').status_code, 503)

    def test_chart_library_served_locally(self):
        with create_app(MemoryStore()).test_client().get('/vendor/chart.umd.min.js') as response:
            self.assertEqual(response.status_code, 200)
            self.assertIn(b'Chart.js', response.data)


if __name__ == "__main__":
    unittest.main()
