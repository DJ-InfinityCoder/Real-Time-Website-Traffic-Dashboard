"""Streams simulated web-server logs into a Kafka topic.

    python producer/log_producer.py --rate 80
    python producer/log_producer.py --stdout --duration 5      # no Kafka needed, just prints
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import config  # noqa: E402
from producer.generator import TrafficGenerator  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="Simulated web log producer")
    ap.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    ap.add_argument("--topic", default=config.KAFKA_TOPIC)
    ap.add_argument("--rate", type=float, default=60, help="average requests per second")
    ap.add_argument("--spike-every", type=int, default=90, help="seconds between traffic spikes (0 = never)")
    ap.add_argument("--error-every", type=int, default=150, help="seconds between 5xx error bursts (0 = never)")
    ap.add_argument("--duration", type=int, default=0, help="stop after N seconds (0 = run forever)")
    ap.add_argument("--malformed", type=float, default=0.002, help="fraction of deliberately broken messages")
    ap.add_argument("--stdout", action="store_true", help="print events instead of sending to Kafka")
    args = ap.parse_args()

    if args.rate <= 0 or args.duration < 0 or not 0 <= args.malformed <= 1:
        ap.error("rate must be positive, duration nonnegative, and malformed between 0 and 1")

    gen = TrafficGenerator(base_rate=args.rate, spike_every=args.spike_every, error_every=args.error_every)

    producer = None
    if not args.stdout:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            linger_ms=20,
            acks="all",
            retries=5,
            key_serializer=lambda k: k.encode(),
            value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode(),
        )
        print(f"[producer] sending to {args.bootstrap} topic '{args.topic}' at ~{args.rate:.0f} req/s (Ctrl+C to stop)")

    delivery_errors = []
    tick, sent, start, last_report = 0.1, 0, time.time(), time.time()
    try:
        while True:
            if delivery_errors:
                raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")
            loop_start = time.time()
            for ev in gen.events(tick, loop_start):
                payload = ev
                if gen.rng.random() < args.malformed:   # exercise the cleaning step in Spark
                    payload = b'{"ts": "garbage", "url": '
                if producer:
                    producer.send(args.topic, key=ev["ip"], value=payload).add_errback(delivery_errors.append)
                else:
                    print(json.dumps(ev))
                sent += 1
            if producer and time.time() - last_report >= 5:
                t = loop_start - gen.t0
                flag = " SPIKE" if gen.in_spike(t) else ""
                flag += " ERROR-BURST" if gen.in_error_burst(t) else ""
                print(f"[producer] {sent} events sent, current rate ~{gen.rate(t):.0f}/s{flag}")
                last_report = time.time()
            if args.duration and time.time() - start >= args.duration:
                break
            time.sleep(max(0, tick - (time.time() - loop_start)))
    except KeyboardInterrupt:
        pass
    finally:
        if producer:
            producer.flush()
            producer.close()
        print(f"[producer] stopped after {sent} events", file=sys.stderr)
        if delivery_errors:
            raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")


if __name__ == "__main__":
    main()
