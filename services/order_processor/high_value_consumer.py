import json

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "high-value-orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="high-value-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("High Value Consumer Started")

for message in consumer:

    print(
        "ALERT:",
        message.value
    )
