import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["roi_tracker"]

# Load and insert lead attribution data
def store_leads():
    df = pd.read_csv("sample_data/lead_attribution.csv")
    records = df.to_dict(orient="records")
    db.lead_attribution.delete_many({})  # Optional: clean before insert
    db.lead_attribution.insert_many(records)
    print("✅ lead_attribution data saved")

# Load and insert ROI metrics
def store_roi():
    df = pd.read_csv("sample_data/roi_metrics.csv")
    records = df.to_dict(orient="records")
    db.roi_metrics.delete_many({})
    db.roi_metrics.insert_many(records)
    print("✅ roi_metrics data saved")

if __name__ == "__main__":
    store_leads()
    store_roi()
