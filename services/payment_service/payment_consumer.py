import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
   # auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

dlq_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Payment Service Started")

for message in consumer:

    order = message.value

    try:

        order_id = order["order_id"]

        print(
            f"Payment Processed "
            f"for Order {order_id}"
        )

    except Exception as e:

        print(
            f"Invalid Event Sent To DLQ: {order}"
        )

        dlq_producer.send(
            "orders-dlq",
            {
                "original_event": order,
                "error": str(e)
            }
        )

        dlq_producer.flush()
