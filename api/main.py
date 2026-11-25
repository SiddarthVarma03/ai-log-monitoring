import json
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer


app = FastAPI(
    title="AI Log Monitoring API",
    description="Receives system logs and sends them to Kafka",
    version="1.0"
)


class LogMessage(BaseModel):
    date: str
    time: str
    pid: int
    level: str
    component: str
    block_id: str | None
    message: str
    event_id: str | None = None


# Connect FastAPI to Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


@app.get("/")
def home():
    return {
        "message": "AI Log Monitoring API is running"
    }


@app.get("/health")
def health():
    return {
        "status": "healthy"
    }


@app.post("/logs")
def receive_log(log: LogMessage):

    try:
        log_data = log.model_dump()

        # Send the log to Kafka
        future = kafka_producer.send(
            "application-logs",
            value=log_data
        )

        # Wait until Kafka confirms it received the message
        future.get(timeout=10)

        print("\nLog sent to Kafka:")
        print(log_data)

        return {
            "status": "success",
            "message": "Log received and sent to Kafka",
            "block_id": log.block_id
        }

    except Exception as error:
        print("Kafka error:", error)

        raise HTTPException(
            status_code=500,
            detail="Could not send log to Kafka"
        )