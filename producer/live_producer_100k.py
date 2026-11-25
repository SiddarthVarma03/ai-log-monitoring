import re
import time
import pandas as pd
import requests


CSV_FILE = "data/training/HDFS_100k.log_structured.csv"

API_URL = "http://127.0.0.1:8000/logs"


print("Loading HDFS 100k structured logs...")

df = pd.read_csv(
    CSV_FILE,
    dtype={
        "Date": "string",
        "Time": "string",
        "EventId": "string"
    }
)


def extract_block_id(content):

    match = re.search(
        r"blk_-?\d+",
        str(content)
    )

    if match:
        return match.group()

    return None


print("Starting 100k live log simulation...")
print("Total logs available:", len(df))


for index, row in df.iterrows():

    block_id = extract_block_id(
        row["Content"]
    )

    if block_id is None:
        continue


    log_data = {
        "date": str(row["Date"]),
        "time": str(row["Time"]),
        "pid": int(row["Pid"]),
        "level": str(row["Level"]),
        "component": str(row["Component"]),
        "block_id": block_id,
        "message": str(row["Content"]),

        # IMPORTANT:
        # Send the real Event ID
        "event_id": str(row["EventId"])
    }


    try:

        response = requests.post(
            API_URL,
            json=log_data
        )

        print(
            f"Sent log {index + 1} "
            f"- Event: {row['EventId']} "
            f"- Block: {block_id} "
            f"- Status: {response.status_code}"
        )


    except requests.exceptions.RequestException as error:

        print("Error sending log:", error)


    # Faster than the old 0.5 second producer
    time.sleep(0.05)