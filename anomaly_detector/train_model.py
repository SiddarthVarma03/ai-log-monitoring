import re
import os
import joblib
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    accuracy_score
)


LOG_FILE = "data/training/HDFS_100k.log_structured.csv"
LABEL_FILE = "data/training/anomaly_label.csv"

MODEL_DIR = "anomaly_detector/model"
MODEL_FILE = os.path.join(MODEL_DIR, "logistic_regression_model.pkl")
FEATURE_FILE = os.path.join(MODEL_DIR, "event_features.pkl")


print("Loading training data...")

logs = pd.read_csv(LOG_FILE)
labels = pd.read_csv(LABEL_FILE)


# --------------------------------------------------
# Extract HDFS block ID from each log message
# --------------------------------------------------

def extract_block_id(content):
    match = re.search(r"blk_-?\d+", str(content))

    if match:
        return match.group()

    return None


logs["BlockId"] = logs["Content"].apply(extract_block_id)

logs = logs.dropna(subset=["BlockId"])


print("Structured logs:", len(logs))
print("Unique blocks:", logs["BlockId"].nunique())


# --------------------------------------------------
# Create event-count features for each block
# --------------------------------------------------

print("\nCreating event features...")

features = pd.crosstab(
    logs["BlockId"],
    logs["EventId"]
)

features = features.reset_index()


print("Feature table shape:", features.shape)


# --------------------------------------------------
# Add Normal / Anomaly labels
# --------------------------------------------------

dataset = features.merge(
    labels,
    on="BlockId",
    how="inner"
)


print("\nBlock-level label distribution:")
print(dataset["Label"].value_counts())


# Convert labels:
# Normal  -> 0
# Anomaly -> 1

dataset["Target"] = dataset["Label"].map({
    "Normal": 0,
    "Anomaly": 1
})


# --------------------------------------------------
# Prepare X and y
# --------------------------------------------------

X = dataset.drop(
    columns=["BlockId", "Label", "Target"]
)

y = dataset["Target"]


print("\nNumber of ML features:", X.shape[1])


# --------------------------------------------------
# Split training and testing data
# --------------------------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.20,
    random_state=42,
    stratify=y
)


print("Training blocks:", len(X_train))
print("Testing blocks:", len(X_test))


# --------------------------------------------------
# Train Logistic Regression
# --------------------------------------------------

print("\nTraining Logistic Regression model...")

model = LogisticRegression(
    max_iter=1000,
    class_weight="balanced"
)

model.fit(X_train, y_train)


# --------------------------------------------------
# Test model
# --------------------------------------------------

predictions = model.predict(X_test)


print("\nAccuracy:")
print(accuracy_score(y_test, predictions))


print("\nClassification Report:")
print(
    classification_report(
        y_test,
        predictions,
        target_names=["Normal", "Anomaly"]
    )
)


print("\nConfusion Matrix:")
print(confusion_matrix(y_test, predictions))


# --------------------------------------------------
# Save model
# --------------------------------------------------

os.makedirs(MODEL_DIR, exist_ok=True)

joblib.dump(
    model,
    MODEL_FILE
)

joblib.dump(
    list(X.columns),
    FEATURE_FILE
)


print("\nModel saved to:")
print(MODEL_FILE)

print("\nFeature list saved to:")
print(FEATURE_FILE)

print("\nTraining complete!")