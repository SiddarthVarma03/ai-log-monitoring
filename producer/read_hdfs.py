import re
import pandas as pd


LOG_FILE = "data/HDFS_2k.log"
OUTPUT_FILE = "data/HDFS_2k_structured.csv"


logs = []


with open(LOG_FILE, "r") as file:
    for line in file:

        parts = line.strip().split(" ", 5)

        if len(parts) < 6:
            continue

        date = parts[0]
        time = parts[1]
        pid = parts[2]
        level = parts[3]

        component = parts[4].replace(":", "")

        message = parts[5]

        block_match = re.search(r"blk_-?\d+", line)

        if block_match:
            block_id = block_match.group()
        else:
            block_id = None

        logs.append({
            "date": date,
            "time": time,
            "pid": pid,
            "level": level,
            "component": component,
            "block_id": block_id,
            "message": message
        })


df = pd.DataFrame(logs)

df.to_csv(OUTPUT_FILE, index=False)


print("Finished parsing logs!")
print("Total logs:", len(df))

print("\nFirst 5 structured logs:")
print(df.head())