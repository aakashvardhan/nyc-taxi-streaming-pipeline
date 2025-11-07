import json, signal, sys, time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from dateutil import parser as dtp
from kafka import KafkaConsumer, KafkaProducer


BROKER = "broker:9092"

RAW_TOPIC = "nyctaxi.raw_trips"
CLEAN_TOPIC = "nyctaxi.clean_trips"
METRICS_TOPIC = "nyctaxi.metrics_5m"
DLQ_TOPIC = "nyctaxi.dlq"


FLUSH_INTERVAL_SEC = 60 # emitting snapshot once per minute
WINDOW_SIZE_SEC = 5 * 60 # tumbling 5-minute windows

running = True

def ts_floor_5m(dt: datetime) -> int:
    epoch = int(dt.replace(tzinfo=timezone.utc).timestamp())
    return epoch - (epoch % WINDOW_SIZE_SEC)

def parse_dt(s: str) -> datetime:
    return dtp.parse(s)

def validate_and_enrich(msg: dict):
    try:
        pick = parse_dt(msg["tpep_pickup_datetime"])
        drop = parse_dt(msg["tpep_dropoff_datetime"])
        trip_seconds = max((drop - pick).total_seconds(), 0.0)
        distance = float(msg.get("trip_distance") or 0.0)
        total_amount = float(msg.get("total_amount") or 0.0)

        if trip_seconds <= 0 or distance <= 0:
            return None, {"error":"invalid_trip", "payload": msg}

        speed_mph = distance / (trip_seconds / 3600.0) if trip_seconds > 0 else 0.0
        fare_per_mile = (total_amount / distance) if distance > 0 else 0.0
        #sanity
        if speed_mph <= 1 or speed_mph > 80 or fare_per_mile <= 0 or fare_per_mile > 50:
            return None, {"error": "sanity_check_failed", "payload": msg}

        clean = dict(msg)
        clean.update({
                     "trip_seconds": trip_seconds,
                     "speed_mph": speed_mph,
                     "fare_per_mile": fare_per_mile
        })
        return clean, None

    except Exception as e:
        return None, {"error":"exception", "detail": str(e), "payload": msg}

def emit_metrics_snapshot(producer, table, now_ts=None):
    now_iso = datetime.utcnow().isoformat()
    for (pu, wstart), bucket in list(table.items()):
        trips = bucket.get("trips", 0)
        if trips == 0:
            continue
        out = {
                "PULocationID": pu,
                "window": "5m",
                "window_start": datetime.fromtimestamp(wstart, tz=timezone.utc).isoformat(),
                "window_end": datetime.fromtimestamp(wstart + WINDOW_SIZE_SEC, tz=timezone.utc).isoformat(),
                "trips": trips,
                "avg_speed_mph": bucket["sum_speed"] / trips,
                "avg_fare_per_mile": bucket["sum_fare_per_mile"] / trips,
                "total_fare": bucket["sum_total_fare"],
                "ts": now_iso,
        }
        producer.send(METRICS_TOPIC, value=out)
    producer.flush()

def gc_old_windows(table, watermark_epoch):
    # drop windows that ended > 5 mins before watermark to avoid unbounded growth
    threshold = watermark_epoch - (5 * 60)
    to_delete = [(k_pu, k_ws) for (k_pu, k_ws) in table.keys() if (k_ws + WINDOW_SIZE_SEC) < threshold]
    for key in to_delete:
        del table[key]

def main():
    global running

    consumer = KafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=BROKER,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            group_id="nyctaxi-manual-proc",
            consumer_timeout_ms=0
    )

    producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer= lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=25,
            acks=1
    )

    # metrics table keyed by (PULocationID, window_start_epoch)
    metrics_table = defaultdict(lambda: {"trips":0, "sum_speed":0.0, "sum_fare_per_mile":0.0, "sum_total_fare":0.0})

    next_flush = time.time() + FLUSH_INTERVAL_SEC

    def _shutdown(sig, frame):
        #nonlocal running
        running = False
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while running:
        msg_batch = consumer.poll(timeout_ms=500)
        batch_empty = True

        for tp, records in msg_batch.items():
            batch_empty = False
            for rec in records:
                raw = rec.value
                clean, err = validate_and_enrich(raw)
                if err:
                    producer.send(DLQ_TOPIC, value=err)
                    continue

                #emit clean trip
                producer.send(CLEAN_TOPIC, value=clean)
                
                # event time streaming based on pickup time
                pu_time = parse_dt(clean["tpep_pickup_datetime"])
                window_start = ts_floor_5m(pu_time)
                pu = int(clean.get("PULocationID") or -1)

                b = metrics_table[(pu, window_start)]
                b["trips"] += 1
                b["sum_speed"] += clean["speed_mph"]
                b["sum_fare_per_mile"] += clean["fare_per_mile"]
                b["sum_total_fare"] += float(clean.get("total_amount"))
        now = time.time()
        if now >= next_flush:
            emit_metrics_snapshot(producer, metrics_table)
            # simple garbage collection based on processing-time watermark
            gc_old_windows(metrics_table, int(now))
            next_flush = now + FLUSH_INTERVAL_SEC

        if batch_empty:
            time.sleep(0.05)

    #final flush on _shutdown
    emit_metrics_snapshot(producer, metrics_table)
    producer.flush()
    consumer.close()

if __name__ == "__main__":
    main()

        
