import json
from kafka import KafkaConsumer


def safe_json_deserializer(data):
    if data is None:
        return None

    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        return {"error": str(e)}


consumer = KafkaConsumer(
    "orders_v2",
    bootstrap_servers="localhost:9092",
    group_id="order-processors-v2",
    auto_offset_reset="earliest",
    value_deserializer=safe_json_deserializer,
)

print("Waiting for orders... (Press Ctrl+C to stop)")

try:
    for message in consumer:
        print(
            f"Partition={message.partition} | "
            f"Offset={message.offset} | "
            f"Value={message.value}"
        )

except KeyboardInterrupt:
    print("\nShutting down consumer gracefully...")

finally:
    consumer.close()
    print("Consumer closed.")
