import os
import json
import joblib
from collections import defaultdict, Counter

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer


# --------------------------------------------------
# Kafka configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)


# --------------------------------------------------
# Load trained ML model
# --------------------------------------------------

MODEL_PATH = (
    "anomaly_detector/model/"
    "logistic_regression_model.pkl"
)

FEATURES_PATH = (
    "anomaly_detector/model/"
    "event_features.pkl"
)


model = joblib.load(MODEL_PATH)
event_features = joblib.load(FEATURES_PATH)


print("✅ ML model loaded")
print(
    f"✅ Loaded {len(event_features)} event features"
)


# --------------------------------------------------
# Kafka consumer
# --------------------------------------------------

consumer = KafkaConsumer(
    "application-logs",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="live-anomaly-detector-group-v3",
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    )
)


# --------------------------------------------------
# Kafka producer for detected incidents
# --------------------------------------------------

incident_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(
        value
    ).encode("utf-8")
)


# --------------------------------------------------
# Store events seen for each block
# --------------------------------------------------

block_events = defaultdict(Counter)


# --------------------------------------------------
# Fallback Event ID mapping
# --------------------------------------------------

def map_event_id(message):

    if not message:
        return None

    message_lower = message.lower()

    if "receiving block" in message_lower:
        return "E5"

    if "received block" in message_lower:
        return "E6"

    if "verification succeeded" in message_lower:
        return "E2"

    if "served block" in message_lower:
        return "E3"

    if "writeblock" in message_lower:
        return "E7"

    if (
        "packetresponder" in message_lower
        and "interrupted" in message_lower
    ):
        return "E8"

    if (
        "packetresponder" in message_lower
        and "exception" in message_lower
    ):
        return "E10"

    if (
        "packetresponder" in message_lower
        and "terminating" in message_lower
    ):
        return "E11"

    if "receiving empty packet" in message_lower:
        return "E13"

    if "exception in receiveblock" in message_lower:
        return "E14"

    if "changing block file offset" in message_lower:
        return "E15"

    if "transmitted block" in message_lower:
        return "E16"

    if "starting thread to transfer block" in message_lower:
        return "E18"

    if "deleting block" in message_lower:
        return "E21"

    if "namesystem.allocateblock" in message_lower:
        return "E22"

    if "to replicate" in message_lower:
        return "E25"

    if (
        "blockmap updated" in message_lower
        and "is added to" in message_lower
    ):
        return "E26"

    if (
        "redundant addstoredblock request"
        in message_lower
    ):
        return "E27"

    return None


# --------------------------------------------------
# Start detector
# --------------------------------------------------

print("🚨 Live Anomaly Detector started...")
print(
    f"Kafka server: {KAFKA_BOOTSTRAP_SERVERS}"
)
print("Waiting for application logs...")


# --------------------------------------------------
# Process incoming Kafka logs
# --------------------------------------------------

for message in consumer:

    log = message.value

    block_id = log.get("block_id")
    log_message = log.get("message", "")

    if not block_id:
        continue


    event_id = log.get("event_id")

    if (
        not event_id
        or event_id == "None"
        or event_id == "nan"
    ):
        event_id = map_event_id(log_message)


    if event_id not in event_features:

        print(
            f"Block: {block_id} | "
            f"Event: {event_id} | "
            f"Event not used by model"
        )

        continue


    block_events[block_id][event_id] += 1

    events_seen = sum(
        block_events[block_id].values()
    )


    print("\n" + "=" * 60)

    print(f"Block ID: {block_id}")
    print(f"Event ID: {event_id}")
    print(f"Events Seen: {events_seen}")


    if events_seen < 3:

        print(
            "Prediction: waiting for more events..."
        )

        continue


    feature_row = {
        feature: block_events[block_id].get(
            feature,
            0
        )
        for feature in event_features
    }


    feature_df = pd.DataFrame(
        [feature_row],
        columns=event_features
    )


    prediction = int(
        model.predict(feature_df)[0]
    )

    probabilities = model.predict_proba(
        feature_df
    )[0]

    anomaly_probability = probabilities[1]


    if prediction == 1:

        print("Prediction: 🚨 ANOMALY")

    else:

        print("Prediction: ✅ NORMAL")


    print(
        "Anomaly Probability: "
        f"{anomaly_probability:.4f}"
    )


    if prediction == 1:

        incident = {
            "block_id": block_id,
            "event_id": event_id,
            "level": log.get("level"),
            "component": log.get("component"),
            "message": log_message,
            "events_seen": events_seen,
            "anomaly_probability": round(
                float(anomaly_probability),
                4
            )
        }


        future = incident_producer.send(
            "detected-incidents",
            value=incident
        )

        future.get(timeout=10)


        print(
            "📤 Incident sent to "
            "detected-incidents topic"
        )