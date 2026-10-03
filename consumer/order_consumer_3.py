import json
from kafka import KafkaConsumer

# Define a safe deserializer function
def safe_json_deserializer(data):
    if data is None:
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        # Return the raw string or marker instead of crashing
        return f"[Invalid JSON Data]: {data}"

consumer = KafkaConsumer(
    "orders_v2",
    bootstrap_servers="localhost:9092",
    group_id="order-processors-v2",
    value_deserializer=safe_json_deserializer
)

print("Waiting for orders...")

for message in consumer:
    # Safely skip or print out bad data
    if isinstance(message.value, str) and message.value.startswith("[Invalid JSON Data]"):
        print(message.value)
    else:
        print("Received:", message.value)
