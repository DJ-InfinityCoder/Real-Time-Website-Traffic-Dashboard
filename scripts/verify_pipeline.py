#!/usr/bin/env python3
"""Real end-to-end smoke test: unique Kafka event -> Spark -> HBase -> API."""
import argparse
import json
import os
import sys
import time
import uuid
from urllib.request import urlopen

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default=f'http://localhost:{config.API_PORT}')
    parser.add_argument('--timeout', type=int, default=120)
    args = parser.parse_args()
    with urlopen(args.url + '/api/health', timeout=10) as response:
        health = json.load(response)
    if health.get('mode') != 'hbase':
        raise RuntimeError('This test requires the real HBase backend; demo mode is not accepted')
    from kafka import KafkaProducer
    token = uuid.uuid4().hex
    page = '/verify/' + token
    client = 'verify-' + token
    event = dict(ts=int(time.time()*1000), ip=client, method='GET', url=page,
                 status=503, bytes=321, rt_ms=123, country='TEST', ua='pipeline-check')
    producer = KafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP, acks='all',
                             value_serializer=lambda v: json.dumps(v).encode())
    try:
        record = producer.send(config.KAFKA_TOPIC, key=client.encode(), value=event).get(timeout=20)
        producer.flush(timeout=20)
    finally:
        producer.close(timeout=10)
    deadline = time.monotonic() + args.timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            # Error log retains only recent rows. Direct page scan avoids relying on top-N visibility.
            from api.stores import HBaseStore
            rows = HBaseStore().fetch_dim('page', int(time.time()), 300)
            with urlopen(args.url + '/api/snapshot?window=300', timeout=15) as response:
                snap = json.load(response)
            if rows.get(page, (0, 0))[0] >= 1 and any(e.get('url') == page for e in snap['errors']):
                print(json.dumps(dict(result='PASS', mode=snap['mode'], page=page,
                      kafka_partition=record.partition, kafka_offset=record.offset,
                      hbase_hits=rows[page][0], api_requests=snap['summary']['requests'],
                      data_age=snap['data_age']), indent=2))
                return
        except Exception as exc:
            last_error = str(exc)
        time.sleep(2)
    raise RuntimeError(f'Event did not complete Kafka -> Spark -> HBase -> API within {args.timeout}s. Last error: {last_error}')


if __name__ == '__main__':
    main()
