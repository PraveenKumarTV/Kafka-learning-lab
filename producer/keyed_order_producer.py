from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

users = [101, 102, 103]

for i in range(20):

    user_id = random.choice(users)

    order = {
        "order_id": i,
        "user_id": user_id,
        "amount": random.randint(100, 500)
    }

    producer.send(
        "orders_v2",
        key=str(user_id).encode("utf-8"),
        value=order
    )

    print(f"Sent {order}")

    time.sleep(1)

producer.flush()
