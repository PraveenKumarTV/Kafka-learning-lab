from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

orders = [
    {
        "order_id": 1,
        "item": "Pizza",
        "restaurant":"Pizza Hut",
        "amount": 499
    },
    {
        "order_id": 2,
        "item": "Burger",
        "amount": 199
    },
    {
        "order_id": 3,
        "item": "Biryani",
        "amount": 299
    }
]

for order in orders:
    producer.send("orders", order)
    print(f"Sent: {order}")
    time.sleep(2)

producer.flush()
