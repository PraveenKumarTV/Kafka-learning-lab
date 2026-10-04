import json

from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    # auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

dlq_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Payment Service Started")
retries = 3

for message in consumer:
    order = message.value
    success = False
    last_error = None

    for attempt in range(retries):
        try:
            order_id = order["order_id"]
            print(f"Payment Processed for Order {order_id}")
            success = True
            break  # Exit retry loop on success

        except Exception as e:
            last_error = e
            print(f"Retry {attempt + 1} failed: {e}")

    # If all retries failed, send to Dead Letter Queue (DLQ)
    if not success:
        print(f"Invalid Event Sent To DLQ: {order}")
        dlq_producer.send(
            "orders-dlq",
            {"original_event": order, "error": str(last_error)},
        )
        dlq_producer.flush()
