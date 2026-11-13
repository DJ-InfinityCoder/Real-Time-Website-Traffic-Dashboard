"""Data access for the dashboard API.

BaseStore turns raw per-second / per-second counters into dashboard-ready numbers.
Two concrete back-ends only have to implement the `fetch_*` primitives:

  HBaseStore   - the real pipeline (reads what the Spark job wrote)
  MemoryStore  - demo mode, fed by the built-in generator, so the dashboard can be previewed
                 without Kafka / Spark / HBase
"""
import json
import threading
import time
from collections import defaultdict, deque

from common import config
from common import schema as S

LAG = 2  # seconds: the newest second is still being filled by Spark


class BaseStore:
    mode = "base"

    # ---- primitives (implemented by back-ends) -----------------------------
    def fetch_seconds(self, now, seconds):      # -> {sec: {col: int}}
        raise NotImplementedError

    def fetch_dim(self, dim, now, window):      # -> {value: (hits, errors)}
        raise NotImplementedError

    def fetch_visitors(self, now, window):      # -> set of ips
        raise NotImplementedError

    def fetch_errors(self, limit):              # -> [event dict]
        raise NotImplementedError

    def ping(self):
        return True

    # ---- derived metrics ---------------------------------------------------
    @staticmethod
    def _sum(rows):
        t = {c: 0 for c in S.METRIC_COLS}
        for r in rows:
            for c in S.METRIC_COLS:
                t[c] += r.get(c, 0)
        return t

    def timeseries(self, window, now=None, secs=None):
        now = int(time.time() if now is None else now)
        secs = secs if secs is not None else self.fetch_seconds(now, window)
        bucket = max(1, window // 150)
        start = now - window + 1
        points = []
        for b in range(start, now + 1, bucket):
            rows = [secs[s] for s in range(b, min(b + bucket, now + 1)) if s in secs]
            tot = self._sum(rows)
            width = min(bucket, now - b + 1)
            req = tot["req"]
            points.append({
                "t": b,
                "rps": round(req / width, 2),
                "e4": round(tot["s4xx"] / width, 2),
                "e5": round(tot["s5xx"] / width, 2),
                "avg_rt": round(tot["rt_sum"] / req, 1) if req else None,
            })
        return points, bucket

    def top(self, dim, window, n=10, now=None, total=None):
        now = int(time.time() if now is None else now)
        rows = self.fetch_dim(dim, now, window)
        ranked = sorted(rows.items(), key=lambda kv: kv[1][0], reverse=True)[:n]
        total = total or sum(h for h, _ in rows.values()) or 1
        mean_hits = (sum(h for h, _ in rows.values()) / len(rows)) if rows else 0
        out = []
        for value, (hits, errors) in ranked:
            item = {"value": value, "hits": hits, "errors": errors, "share": round(min(hits / total, 1.0), 4)}
            if dim == "ip":
                # bot heuristic: at least 40 hits AND 8x the average hits per client IP
                item["suspicious"] = hits >= max(40, 8 * mean_hits)
            out.append(item)
        return out

    def snapshot(self, window=300, now=None):
        now = int(time.time() if now is None else now)
        window = max(30, min(int(window), 1800))
        secs = self.fetch_seconds(now, window)
        totals = self._sum(secs.values())
        req = totals["req"]

        series, bucket = self.timeseries(window, now, secs)

        def span(first, last):  # inclusive range of seconds
            return self._sum(secs[s] for s in range(first, last + 1) if s in secs)

        last10 = span(now - LAG - 9, now - LAG)
        last30 = span(now - LAG - 29, now - LAG)
        seen = [s for s, r in secs.items() if r.get("req")]
        age = (now - max(seen)) if seen else None

        return {
            "now": now,
            "window": window,
            "bucket": bucket,
            "mode": self.mode,
            "data_age": age,
            "summary": {
                "requests": req,
                "rps_now": round(last10["req"] / 10, 1),
                "rps_avg": round(req / window, 1),
                "rps_peak": max((r.get("req", 0) for r in secs.values()), default=0),
                "unique_visitors": len(self.fetch_visitors(now, window)),
                "avg_rt": round(totals["rt_sum"] / req, 1) if req else 0,
                "bytes": totals["bytes"],
                "bytes_per_sec": round(totals["bytes"] / window),
                "err5_rate": round(totals["s5xx"] / req, 4) if req else 0,
                "err4_rate": round(totals["s4xx"] / req, 4) if req else 0,
                "malformed": totals["bad"],
            },
            "recent": {  # last 30 s, used for alerting
                "requests": last30["req"],
                "err5_rate": round(last30["s5xx"] / last30["req"], 4) if last30["req"] else 0,
            },
            "status": {k: totals[k] for k in ("s2xx", "s3xx", "s4xx", "s5xx")},
            "series": series,
            "top_pages": self.top("page", window, 10, now, req),
            "top_countries": self.top("country", window, 8, now, req),
            "top_ips": self.top("ip", window, 8, now, req),
            "errors": self.fetch_errors(12),
        }


class HBaseStore(BaseStore):
    mode = "hbase"

    def __init__(self, host=None, port=None):
        self.host = host or config.HBASE_HOST
        self.port = port or config.HBASE_PORT
        self._local = threading.local()

    def _conn(self):
        import happybase
        if getattr(self._local, "conn", None) is None:
            self._local.conn = happybase.Connection(self.host, self.port, timeout=5000)
        return self._local.conn

    def _run(self, fn):
        try:
            return fn(self._conn())
        except Exception:
            try:
                self._local.conn.close()
            except Exception:
                pass
            self._local.conn = None
            return fn(self._conn())  # one retry on a fresh connection

    def ping(self):
        self._run(lambda c: c.tables())
        return True

    def fetch_seconds(self, now, seconds):
        start, stop = S.sec_scan(now, seconds)

        def go(c):
            out = {}
            for key, data in c.table(S.T_METRICS).scan(row_start=start, row_stop=stop, limit=seconds + 5):
                sec = S.MAX_TS - int(key)
                out[sec] = {col: S.decode_counter(data.get(f"m:{col}".encode())) for col in S.METRIC_COLS}
            return out
        return self._run(go)

    def fetch_dim(self, dim, now, window):
        start, stop = S.dim_scan(dim, now, window)

        def go(c):
            out = {}
            for key, data in c.table(S.T_DIMS).scan(row_start=start, row_stop=stop):
                value = key.decode(errors="replace").split("#", 2)[2]
                h, e = out.get(value, (0, 0))
                out[value] = (h + S.decode_counter(data.get(b"c:hits")), e + S.decode_counter(data.get(b"c:errors")))
            return out
        return self._run(go)

    def fetch_visitors(self, now, window):
        start, stop = S.visitor_scan(now, window)

        def go(c):
            return {k.decode(errors="replace").split("#", 1)[1]
                    for k, _ in c.table(S.T_VISITORS).scan(row_start=start, row_stop=stop, columns=[b"v:1"])}
        return self._run(go)

    def fetch_errors(self, limit):
        def go(c):
            return [json.loads(d[b"e:j"]) for _, d in c.table(S.T_ERRORS).scan(limit=limit) if b"e:j" in d]
        return self._run(go)


class MemoryStore(BaseStore):
    """Demo back-end: same aggregation arithmetic as the Spark job, but in-process."""
    mode = "demo"

    def __init__(self):
        self.lock = threading.Lock()
        self.secs = defaultdict(lambda: {c: 0 for c in S.METRIC_COLS})
        self.dims = {d: defaultdict(lambda: [0, 0]) for d in S.DIMENSIONS}   # dim -> (sec, value) -> [hits, errors]
        self.visitors = defaultdict(set)                                       # second -> ips
        self.errors = deque(maxlen=100)
        self._last_prune = time.time()

    def ingest(self, events):
        with self.lock:
            for e in events:
                sec = e["ts"] // 1000
                r = self.secs[sec]
                r["req"] += 1
                r["bytes"] += e["bytes"]
                r["rt_sum"] += e["rt_ms"]
                r[f"s{e['status'] // 100}xx"] += 1
                is_err = e["status"] >= 500
                for dim, value in (("page", e["url"]), ("country", e["country"]), ("ip", e["ip"])):
                    cell = self.dims[dim][(sec, value)]
                    cell[0] += 1
                    cell[1] += int(is_err)
                self.visitors[sec].add(e["ip"])
                if is_err:
                    self.errors.appendleft({k: e[k] for k in ("ts", "ip", "method", "url", "status", "rt_ms")})
            if time.time() - self._last_prune > 60:
                self._prune()

    def _prune(self):
        cutoff = int(time.time()) - 3600
        for s in [s for s in self.secs if s < cutoff]:
            del self.secs[s]
        for d in self.dims.values():
            for k in [k for k in d if k[0] < cutoff]:
                del d[k]
        for m in [m for m in self.visitors if m < cutoff]:
            del self.visitors[m]
        self._last_prune = time.time()

    def fetch_seconds(self, now, seconds):
        with self.lock:
            return {s: dict(self.secs[s]) for s in range(now - seconds + 1, now + 1) if s in self.secs}

    def fetch_dim(self, dim, now, window):
        lo, hi = now - window + 1, now
        out = {}
        with self.lock:
            for (sec, value), (h, e) in self.dims[dim].items():
                if lo <= sec <= hi:
                    ph, pe = out.get(value, (0, 0))
                    out[value] = (ph + h, pe + e)
        return out

    def fetch_visitors(self, now, window):
        lo, hi = now - window + 1, now
        with self.lock:
            return set().union(*[ips for m, ips in self.visitors.items() if lo <= m <= hi]) if self.visitors else set()

    def fetch_errors(self, limit):
        with self.lock:
            return sorted(self.errors, key=lambda e: e["ts"], reverse=True)[:limit]
