"""HBase schema and row-key design (shared by the Spark job and the API).

Design goal: the dashboard always asks "what happened most recently?", so every
time-based row key starts with a REVERSED timestamp. HBase sorts rows
lexicographically, therefore the newest data comes first and a short scan
(row_start .. row_stop) reads exactly the time window we need.

    reverse(ts) = 9_999_999_999 - ts       (zero padded to 10 digits)

Tables
------
traffic_metrics   row: rev(second)                      cf m : req bytes rt_sum s2xx s3xx s4xx s5xx bad
dim_counts        row: <dim>#rev(second)#<value>        cf c : hits errors      (dim = page | country | ip)
visitors          row: rev(second)#<ip>                 cf v : 1                (exact unique-visitor counting)
recent_errors     row: rev_ms#<random>                  cf e : j                (JSON of a 5xx event, TTL 1h)

All numeric cells in traffic_metrics and dim_counts are HBase *counters*
(atomic increments), so several Spark micro-batches can safely contribute to
the same second.
"""
import struct

MAX_TS = 9_999_999_999
MAX_MS = 9_999_999_999_999

T_METRICS = "traffic_metrics"
T_DIMS = "dim_counts"
T_VISITORS = "visitors"
T_ERRORS = "recent_errors"

METRIC_COLS = ["req", "bytes", "rt_sum", "s2xx", "s3xx", "s4xx", "s5xx", "bad"]
DIMENSIONS = ["page", "country", "ip"]

TABLES = {
    T_METRICS: {"m": {"max_versions": 1, "time_to_live": 86400}},
    T_DIMS: {"c": {"max_versions": 1, "time_to_live": 86400}},
    T_VISITORS: {"v": {"max_versions": 1, "time_to_live": 7200}},
    T_ERRORS: {"e": {"max_versions": 1, "time_to_live": 3600}},
}


def rev(ts: int) -> str:
    return f"{MAX_TS - int(ts):010d}"


def minute_of(ts: int) -> int:
    return int(ts) // 60 * 60


def clean_value(v, limit: int = 120) -> str:
    """Make a log value safe to embed in a row key."""
    return str(v).replace("\n", " ").replace("\r", " ")[:limit]


def sec_row(sec: int) -> bytes:
    return rev(sec).encode()


def sec_scan(now: int, seconds: int):
    """Rows for the `seconds` most recent seconds ending at `now` (stop is exclusive)."""
    return rev(now).encode(), rev(now - seconds).encode()


def dim_row(dim: str, second: int, value: str) -> bytes:
    return f"{dim}#{rev(second)}#{clean_value(value)}".encode()


def dim_scan(dim: str, now: int, window: int):
    newest = now
    oldest = now - window + 1
    return f"{dim}#{rev(newest)}#".encode(), f"{dim}#{rev(oldest - 1)}#".encode()


def visitor_row(second: int, ip: str) -> bytes:
    return f"{rev(second)}#{clean_value(ip, 64)}".encode()


def visitor_scan(now: int, window: int):
    newest = now
    oldest = now - window + 1
    return f"{rev(newest)}#".encode(), f"{rev(oldest - 1)}#".encode()


def error_row(ts_ms: int, nonce: str) -> bytes:
    return f"{MAX_MS - int(ts_ms):013d}#{nonce}".encode()


def decode_counter(raw: bytes) -> int:
    """HBase counters are stored as 8-byte big-endian signed integers."""
    if raw is None:
        return 0
    if len(raw) == 8:
        return struct.unpack(">q", raw)[0]
    try:
        return int(raw)
    except ValueError:
        return 0
