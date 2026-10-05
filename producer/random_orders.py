from kafka import KafkaProducer
import json
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(20):

    order = {
        "order_id": i,
        "amount": random.randint(100, 1000)
    }

    producer.send("orders", order)

    print(order)

producer.flush()
