import json

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders-dlq",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("DLQ Monitor Started")

for message in consumer:
    print("DLQ EVENT:", message.value)
