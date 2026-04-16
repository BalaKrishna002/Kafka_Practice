from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "rr-topic"

for i in range(100):
    data = {
        "event_id": i,
        "message": f"event-{i}"
    }

    # ❗ No key → round robin across partitions
    producer.send(topic, value=data)

    print(f"Sent: {data}")
    time.sleep(0.5)

producer.flush()