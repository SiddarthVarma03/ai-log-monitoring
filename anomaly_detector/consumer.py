import json

from kafka import KafkaConsumer


TOPIC_NAME = "application-logs"


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="anomaly-detector-group",
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    )
)


print("Anomaly detector consumer started...")
print("Waiting for logs from Kafka...")


for message in consumer:

    log = message.value

    print("\nNew Kafka log received:")
    print("Date:", log.get("date"))
    print("Time:", log.get("time"))
    print("Level:", log.get("level"))
    print("Component:", log.get("component"))
    print("Block ID:", log.get("block_id"))
    print("Message:", log.get("message"))