import time
import pandas as pd
import requests


API_URL = "http://127.0.0.1:8000/logs"
CSV_FILE = "data/HDFS_2k_structured.csv"


df = pd.read_csv(
    CSV_FILE,
    dtype={
        "date": "string",
        "time": "string",
        "block_id": "string"
    }
)


print("Starting live log simulation...")
print("Total logs to send:", len(df))


for index, row in df.iterrows():

    log_data = {
        "date": str(row["date"]),
        "time": str(row["time"]),
        "pid": int(row["pid"]),
        "level": str(row["level"]),
        "component": str(row["component"]),
        "block_id": None if pd.isna(row["block_id"]) else str(row["block_id"]),
        "message": str(row["message"])
    }

    try:
        response = requests.post(API_URL, json=log_data)

        print(
            f"Sent log {index + 1}/{len(df)} "
            f"- Status: {response.status_code}"
        )

    except requests.exceptions.RequestException as error:
        print("Error sending log:", error)

    time.sleep(0.5)