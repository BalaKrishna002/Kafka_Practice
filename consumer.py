from kafka import KafkaConsumer
import json
import sys

consumer_name = sys.argv[1]  # pass name while running

consumer = KafkaConsumer(
    'rr-topic',
    bootstrap_servers='localhost:9092',
    group_id='cg-1',   
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"{consumer_name} started...")

for message in consumer:
    print(f"[{consumer_name}] Partition: {message.partition} Offset:{message.offset}, Value: {message.value}")