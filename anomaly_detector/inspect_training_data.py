import re
import pandas as pd


LOG_FILE = "data/training/HDFS_100k.log_structured.csv"
LABEL_FILE = "data/training/anomaly_label.csv"


print("Loading files...")

logs = pd.read_csv(LOG_FILE)
labels = pd.read_csv(LABEL_FILE)


def extract_block_id(content):
    match = re.search(r"blk_-?\d+", str(content))

    if match:
        return match.group()

    return None


logs["BlockId"] = logs["Content"].apply(extract_block_id)


logs = logs.dropna(subset=["BlockId"])


merged = logs.merge(
    labels,
    on="BlockId",
    how="left"
)


print("\nTotal structured log events:", len(logs))
print("Unique block IDs in 100k sample:", logs["BlockId"].nunique())

print("\nLabel distribution:")
print(merged["Label"].value_counts(dropna=False))

print("\nSample joined data:")
print(
    merged[
        ["BlockId", "EventId", "Level", "Component", "Label"]
    ].head(10)
)