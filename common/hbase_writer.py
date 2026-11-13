"""Writes the aggregates produced by one Spark micro-batch into HBase.

Kept free of any Spark code so it can be unit-tested with a fake HBase.
`agg` layout (plain Python types, produced by spark_job/streaming_job.py):

    {
      "now": 1700000000,
      "malformed": 0,
      "per_sec":  [{"sec": .., "req": .., "bytes": .., "rt_sum": .., "s2xx": .., "s3xx": .., "s4xx": .., "s5xx": ..}],
      "dims":     {"page": [{"sec": .., "value": .., "hits": .., "errors": ..}], "country": [...], "ip": [...]},
      "visitors": [{"sec": .., "ip": ..}],
      "errors":   [{"ts": ms, "ip": .., "method": .., "url": .., "status": .., "rt_ms": ..}]
    }
"""
import json
import uuid

from common import schema as S


def write_aggregates(conn, agg: dict) -> int:
    """Returns the number of HBase operations issued."""
    ops = 0

    metrics = conn.table(S.T_METRICS)
    for r in agg.get("per_sec", []):
        row = S.sec_row(r["sec"])
        for col in S.METRIC_COLS:
            val = int(r.get(col) or 0)
            if val:
                metrics.counter_inc(row, f"m:{col}".encode(), val)
                ops += 1
    if agg.get("malformed"):
        metrics.counter_inc(S.sec_row(agg["now"]), b"m:bad", int(agg["malformed"]))
        ops += 1

    dims = conn.table(S.T_DIMS)
    for dim, rows in agg.get("dims", {}).items():
        for r in rows:
            row = S.dim_row(dim, r["sec"], r["value"])
            dims.counter_inc(row, b"c:hits", int(r["hits"]))
            ops += 1
            if r.get("errors"):
                dims.counter_inc(row, b"c:errors", int(r["errors"]))
                ops += 1

    visitors = conn.table(S.T_VISITORS)
    with visitors.batch() as b:
        for r in agg.get("visitors", []):
            b.put(S.visitor_row(r["sec"], r["ip"]), {b"v:1": b"1"})
            ops += 1

    errors = conn.table(S.T_ERRORS)
    with errors.batch() as b:
        for e in agg.get("errors", []):
            payload = json.dumps(
                {k: e.get(k) for k in ("ts", "ip", "method", "url", "status", "rt_ms")}
            ).encode()
            b.put(S.error_row(e["ts"], uuid.uuid4().hex[:6]), {b"e:j": payload})
            ops += 1
    return ops
