"""Minimal in-memory imitation of the happybase API (sorted rows, counters, scan ranges)."""
import struct


class FakeTable:
    def __init__(self):
        self.rows = {}

    def counter_inc(self, row, column, value=1):
        cell = self.rows.setdefault(bytes(row), {})
        cur = struct.unpack(">q", cell[column])[0] if column in cell else 0
        cell[column] = struct.pack(">q", cur + value)
        return cur + value

    def put(self, row, data):
        self.rows.setdefault(bytes(row), {}).update(data)

    def batch(self):
        table = self

        class B:
            def __enter__(s): return s
            def __exit__(s, *a): return False
            def put(s, row, data): table.put(row, data)
        return B()

    def scan(self, row_start=None, row_stop=None, columns=None, limit=None, **kw):
        n = 0
        for key in sorted(self.rows):
            if row_start is not None and key < row_start:
                continue
            if row_stop is not None and key >= row_stop:
                continue
            yield key, dict(self.rows[key])
            n += 1
            if limit and n >= limit:
                return


class FakeConnection:
    def __init__(self, *a, **k):
        self._tables = {}

    def table(self, name):
        return self._tables.setdefault(name, FakeTable())

    def tables(self):
        return [n.encode() for n in self._tables]

    def close(self):
        pass
