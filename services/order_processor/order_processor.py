import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="order-processor",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Order Processor Started")

for message in consumer:

    order = message.value

    amount = order.get("amount", 0)

    if amount >= 500:

        print(
            f"HIGH VALUE ORDER: "
            f"{order['order_id']}"
        )

        producer.send(
            "high-value-orders",
            order
        )

        producer.flush()
