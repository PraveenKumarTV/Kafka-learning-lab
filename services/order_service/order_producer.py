from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

order = {
    "order_id": 1001,
    "item": "Pizza",
    "amount": 501
}

producer.send("orders", order)
producer.flush()

print("Order created:", order)
