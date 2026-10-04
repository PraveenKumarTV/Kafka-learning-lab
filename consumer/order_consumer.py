import json
from kafka import KafkaConsumer

CONSUMER_NAME = "Consumer-1"


def safe_json_deserializer(data):
    if data is None:
        return None

    return json.loads(data.decode("utf-8"))


consumer = KafkaConsumer(
    "orders_v2",
    bootstrap_servers="localhost:9092",
    group_id="order-processors",
    value_deserializer=safe_json_deserializer,
)

print(f"{CONSUMER_NAME} started")

try:
    for message in consumer:

        print(
            f"{CONSUMER_NAME} | "
            f"Partition={message.partition} | "
            f"Offset={message.offset} | "
            f"Value={message.value}"
        )

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()
