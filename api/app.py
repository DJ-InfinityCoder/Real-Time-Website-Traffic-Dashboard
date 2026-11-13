"""Flask REST API + static host for the dashboard.

    python api/app.py            # reads live data from HBase
    python api/app.py --demo     # built-in simulator, no Kafka/Spark/HBase needed (UI preview)
"""
import argparse
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, jsonify, request, send_from_directory  # noqa: E402

from api.stores import BaseStore, HBaseStore, MemoryStore  # noqa: E402
from common import config  # noqa: E402

DASHBOARD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard")


def _int_arg(name, default, lo, hi):
    try:
        v = int(request.args.get(name, default))
    except ValueError:
        v = default
    return max(lo, min(v, hi))


def create_app(store: BaseStore) -> Flask:
    app = Flask(__name__, static_folder=None)

    @app.route("/")
    def index():
        return send_from_directory(DASHBOARD_DIR, "index.html")

    @app.route("/vendor/<path:name>")
    def vendor(name):
        return send_from_directory(os.path.join(DASHBOARD_DIR, "vendor"), name)

    @app.route("/api/health")
    def health():
        try:
            store.ping()
            return jsonify(status="ok", mode=store.mode, time=int(time.time()))
        except Exception as exc:
            return jsonify(status="error", mode=store.mode, detail=str(exc)), 503

    @app.route("/api/snapshot")
    def snapshot():
        try:
            return jsonify(store.snapshot(_int_arg("window", 300, 30, 1800)))
        except Exception as exc:
            return jsonify(error="backend unavailable", detail=str(exc)), 503

    @app.route("/api/timeseries")
    def timeseries():
        window = _int_arg("window", 300, 30, 1800)
        try:
            points, bucket = store.timeseries(window)
            return jsonify(window=window, bucket=bucket, points=points)
        except Exception as exc:
            return jsonify(error="backend unavailable", detail=str(exc)), 503

    @app.route("/api/top/<dim>")
    def top(dim):
        if dim not in ("page", "country", "ip"):
            return jsonify(error="dim must be page, country or ip"), 400
        try:
            return jsonify(dim=dim, rows=store.top(dim, _int_arg("window", 300, 30, 1800), _int_arg("n", 10, 1, 50)))
        except Exception as exc:
            return jsonify(error="backend unavailable", detail=str(exc)), 503

    @app.after_request
    def no_cache(resp):
        resp.headers["Cache-Control"] = "no-store"
        return resp

    return app


def start_demo_feed(store: MemoryStore):
    from producer.generator import TrafficGenerator
    gen = TrafficGenerator(base_rate=60, spike_every=75, error_every=120)
    gen.t0 = time.time() - 205  # generator clock starts 205 s in the past -> history contains spikes/bursts

    def loop():
        last = time.time()
        while True:
            time.sleep(0.2)
            now = time.time()
            store.ingest(gen.events(now - last, now))
            last = now

    # pre-fill ~3 minutes of history so the charts are not empty on first load
    t = time.time() - 180
    while t < time.time():
        store.ingest(gen.events(0.5, t))
        t += 0.5
    threading.Thread(target=loop, daemon=True).start()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true", help="serve simulated data from memory (no HBase needed)")
    ap.add_argument("--host", default=config.API_HOST)
    ap.add_argument("--port", type=int, default=config.API_PORT)
    args = ap.parse_args()

    if args.demo:
        store = MemoryStore()
        start_demo_feed(store)
        print("[api] DEMO mode: simulated data, HBase is not used")
    else:
        store = HBaseStore()
        print(f"[api] reading from HBase Thrift at {store.host}:{store.port}")
    app = create_app(store)
    print(f"[api] dashboard on http://localhost:{args.port}")
    app.run(host=args.host, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
