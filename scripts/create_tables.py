"""Creates the HBase tables (idempotent).   python scripts/create_tables.py [--reset]"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import happybase  # noqa: E402

from common import config  # noqa: E402
from common.schema import TABLES  # noqa: E402


def main():
    reset = "--reset" in sys.argv
    conn = happybase.Connection(config.HBASE_HOST, config.HBASE_PORT, timeout=15000)
    existing = {t.decode() for t in conn.tables()}
    for name, families in TABLES.items():
        if name in existing and reset:
            conn.disable_table(name)
            conn.delete_table(name)
            existing.discard(name)
            print(f"dropped {name}")
        if name not in existing:
            conn.create_table(name, families)
            print(f"created {name}  families={list(families)}")
        else:
            print(f"exists  {name}")
    conn.close()


if __name__ == "__main__":
    main()
