from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    #auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Inventory Service Started")

for message in consumer:
    order = message.value

    print(
        f"Inventory Updated "
        f"for Order {order['order_id']}"
    )
