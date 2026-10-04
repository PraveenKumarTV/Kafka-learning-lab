from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

bad_order = {
    "amount": 999
}

producer.send("orders", bad_order)

producer.flush()

print("Bad order sent")
