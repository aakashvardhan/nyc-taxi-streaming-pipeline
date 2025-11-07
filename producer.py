import re, io, csv, json, gzip, sys
from typing import Iterable, Dict
import requests
from kafka import KafkaProducer

BROKER = "broker:9092"
TOPIC = "nyctaxi.raw_trips"

URLS = [
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
]


def rows_from_http_parquet(url: str) -> Iterable[Dict]:
    import pyarrow.parquet as pq
    import tempfile, shutil

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tf:
            shutil.copyfileobj(r.raw, tf)
            tf.flush()
            pf = pq.ParquetFile(tf.name)
            for batch in pf.iter_batches(batch_size=10000):
                for rec in batch.to_pylist():
                    yield rec

def stream_urls_to_kafka(urls):
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            linger_ms=50, acks=1
    )
    sent = 0
    for url in urls:
        lower = url.lower()
        if lower.endswith(".parquet"):
            it = rows_from_http_parquet(url)
        else:
            continue
        for row in it:
            producer.send(TOPIC, value=row)
            sent += 1
            if sent % 5000 == 0:
                producer.flush()
    producer.flush()
    print(f"Done. Sent {sent} records.")

if __name__ == '__main__':
    if not URLS:
        print("Populate URLS with monthly files from the TLC page.")

        sys.exit(2)
    stream_urls_to_kafka(URLS)

