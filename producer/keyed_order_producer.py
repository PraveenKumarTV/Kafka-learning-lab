from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

users =list(range(100,150))
for i in range(20):

    user_id = random.choice(users)

    order = {
        "order_id": i,
        "user_id": user_id,
        "amount": random.randint(100, 500)
    }

    # producer.send(
    #     "orders_v2",
    #     key=str(user_id).encode("utf-8"),
    #     value=order
    # )

    future = producer.send(
        "orders_v2",
        key=str(user_id).encode("utf-8"),
        value=order
    )
    
    metadata = future.get(timeout=10)
    
    print(
        f"user={user_id} -> partition={metadata.partition}"
    )

    #print(f"Sent {order}")

    time.sleep(1)

producer.flush()
