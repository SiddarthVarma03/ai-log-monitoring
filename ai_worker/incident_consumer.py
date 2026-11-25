import os
import json
import time
import re
import redis
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
from google import genai


# --------------------------------------------------
# Environment variables
# --------------------------------------------------

load_dotenv(".env")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

# --------------------------------------------------
# Gemini setup
# --------------------------------------------------

client = genai.Client(
    api_key=os.getenv("GEMINI_API_KEY")
)

MODEL_NAME = "gemini-3.5-flash-lite"

MIN_REQUEST_INTERVAL = 5
last_request_time = 0


# --------------------------------------------------
# Kafka setup
# --------------------------------------------------

TOPIC_NAME = "detected-incidents"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="ai-worker-group",
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    )
)


# --------------------------------------------------
# Redis setup
# --------------------------------------------------

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

try:
    redis_client.ping()
    print("✅ Redis connected")
except Exception as error:
    print(f"❌ Redis connection failed: {error}")


# --------------------------------------------------
# Extract IP addresses
# --------------------------------------------------

def extract_ips(message):

    source_ip = "Not available"
    destination_ip = "Not available"

    if not message:
        return source_ip, destination_ip

    source_match = re.search(
        r"src:\s*/?([\d.]+)",
        message
    )

    destination_match = re.search(
        r"dest:\s*/?([\d.]+)",
        message
    )

    if source_match:
        source_ip = source_match.group(1)

    if destination_match:
        destination_ip = destination_match.group(1)

    return source_ip, destination_ip


# --------------------------------------------------
# Gemini rate limiter
# --------------------------------------------------

def wait_for_rate_limit():

    global last_request_time

    elapsed = time.time() - last_request_time

    if elapsed < MIN_REQUEST_INTERVAL:

        wait_time = MIN_REQUEST_INTERVAL - elapsed

        print(
            f"⏳ Waiting {wait_time:.1f}s "
            f"to stay within Gemini free-tier limits..."
        )

        time.sleep(wait_time)

    last_request_time = time.time()


# --------------------------------------------------
# Gemini analysis
# --------------------------------------------------

def generate_ai_analysis(incident):

    message = incident.get("message", "")

    source_ip, destination_ip = extract_ips(message)

    block_id = incident.get("block_id", "Not available")
    event_id = incident.get("event_id", "Not available")
    component = incident.get("component", "Not available")
    probability = incident.get(
        "anomaly_probability",
        "Not available"
    )

    prompt = f"""
You are a cloud incident analysis assistant.

Analyze the HDFS anomaly below.

Use only the information provided.
Do not invent missing information.

Return EXACTLY the following format:

Summary:
<one short sentence explaining the event>

Severity:
<Low, Medium, High, or Critical>

Component:
{component}

Event ID:
{event_id}

Block ID:
{block_id}

Source IP:
{source_ip}

Destination IP:
{destination_ip}

Anomaly Probability:
{probability}

Likely Cause:
<short explanation based only on the provided data>

Recommended Action:
<short practical action>

Important rules:

1. Do not remove any field.
2. Keep Source IP and Destination IP exactly as provided.
3. If an IP says "Not available", keep it as "Not available".
4. Do not invent IP addresses.
5. Do not invent system failures.
6. An anomaly probability alone does not prove that a failure occurred.
7. Keep the response concise.

Original incident:
{json.dumps(incident, indent=2)}
"""

    max_attempts = 3

    for attempt in range(max_attempts):

        try:

            wait_for_rate_limit()

            print(
                f"🤖 Gemini request attempt "
                f"{attempt + 1}/{max_attempts}"
            )

            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt
            )

            return response.text

        except Exception as error:

            error_text = str(error)

            if (
                "429" in error_text
                or "RESOURCE_EXHAUSTED" in error_text
            ):

                print(
                    "⚠️ Gemini free-tier rate limit reached."
                )

                if attempt < max_attempts - 1:

                    print(
                        "⏳ Waiting 60 seconds before retrying..."
                    )

                    time.sleep(60)

            elif (
                "503" in error_text
                or "UNAVAILABLE" in error_text
            ):

                print(
                    "⚠️ Gemini is temporarily busy."
                )

                if attempt < max_attempts - 1:

                    wait_time = 5 * (attempt + 1)

                    print(
                        f"⏳ Retrying in {wait_time} seconds..."
                    )

                    time.sleep(wait_time)

            else:

                return f"Gemini analysis failed: {error}"

    return "Gemini analysis failed after 3 attempts."


# --------------------------------------------------
# Extract Gemini fields
# --------------------------------------------------

def parse_ai_analysis(analysis):

    summary = None
    severity = None
    likely_cause = None
    recommended_action = None

    try:

        summary_match = re.search(
            r"Summary:\s*(.*?)\s*Severity:",
            analysis,
            re.DOTALL
        )

        severity_match = re.search(
            r"Severity:\s*(.*?)\s*Component:",
            analysis,
            re.DOTALL
        )

        cause_match = re.search(
            r"Likely Cause:\s*(.*?)\s*Recommended Action:",
            analysis,
            re.DOTALL
        )

        action_match = re.search(
            r"Recommended Action:\s*(.*)",
            analysis,
            re.DOTALL
        )

        if summary_match:
            summary = summary_match.group(1).strip()

        if severity_match:
            severity = severity_match.group(1).strip()

        if cause_match:
            likely_cause = cause_match.group(1).strip()

        if action_match:
            recommended_action = action_match.group(1).strip()

    except Exception as error:

        print(
            f"⚠️ Could not parse AI response: {error}"
        )

    return (
        summary,
        severity,
        likely_cause,
        recommended_action
    )


# --------------------------------------------------
# Save incident to PostgreSQL
# --------------------------------------------------

def save_incident_to_database(
    incident,
    analysis
):

    message = incident.get("message", "")

    source_ip, destination_ip = extract_ips(
        message
    )

    (
        summary,
        severity,
        likely_cause,
        recommended_action
    ) = parse_ai_analysis(analysis)

    try:

        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )

        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO incidents (
                block_id,
                event_id,
                log_level,
                component,
                events_seen,
                anomaly_probability,
                source_ip,
                destination_ip,
                summary,
                severity,
                likely_cause,
                recommended_action,
                raw_message
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                incident.get("block_id"),
                incident.get("event_id"),
                incident.get("level"),
                incident.get("component"),
                incident.get("events_seen"),
                incident.get("anomaly_probability"),
                source_ip,
                destination_ip,
                summary,
                severity,
                likely_cause,
                recommended_action,
                message
            )
        )

        connection.commit()

        cursor.close()
        connection.close()

        print(
            "💾 Incident saved to PostgreSQL"
        )

    except Exception as error:

        print(
            f"❌ PostgreSQL save failed: {error}"
        )


# --------------------------------------------------
# Start AI worker
# --------------------------------------------------

print("🤖 AI Worker started...")
print("Waiting for detected incidents...")


for message in consumer:

    incident = message.value

    block_id = incident.get("block_id")

    redis_key = f"processed_block:{block_id}"

    if redis_client.exists(redis_key):

        print(
        f"Skipping duplicate incident for block: "
        f"{block_id}"
        )

        continue


    print("\n" + "=" * 60)
    print("🚨 NEW INCIDENT RECEIVED")
    print("=" * 60)

    print(f"Block ID: {block_id}")
    print(f"Event ID: {incident.get('event_id')}")
    print(f"Level: {incident.get('level')}")
    print(f"Component: {incident.get('component')}")
    print(f"Events Seen: {incident.get('events_seen')}")

    print(
        f"Anomaly Probability: "
        f"{incident.get('anomaly_probability')}"
    )

    print(
        f"Message: {incident.get('message')}"
    )


    print(
        "\n🤖 Generating AI incident analysis..."
    )

    analysis = generate_ai_analysis(
        incident
    )


    print("\n🤖 AI INCIDENT ANALYSIS")
    print("-" * 60)

    print(analysis)

    print("=" * 60)


    # Save completed incident to PostgreSQL
    save_incident_to_database(
        incident,
        analysis
    )
    redis_client.set(
    redis_key,
    "processed"
    )

    print(
        f"✅ Block stored in Redis: {block_id}"
    )