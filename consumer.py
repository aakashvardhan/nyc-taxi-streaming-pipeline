import json
from kafka import KafkaConsumer

BROKER = "broker:9092" # use broker port

consumer = KafkaConsumer(
    "nyctaxi.clean_trips",
        bootstrap_servers=BROKER,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="nyctaxi-inspector"
)

print("Listening for messages... (Ctrl+C to exit)")
for message in consumer:
    data = message.value
    print(json.dumps(data, indent=2))
