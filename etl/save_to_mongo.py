import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# ---------------------------
# Connect to MongoDB
# ---------------------------
client = MongoClient(MONGO_URI)
db = client["roi_tracker"]

# ---------------------------
# Upload lead attribution data
# ---------------------------
def store_leads():
    try:
        df = pd.read_csv("sample_data/lead_attribution.csv")
        records = df.to_dict(orient="records")

        db.lead_attribution.delete_many({})  # Optional: clear existing
        db.lead_attribution.insert_many(records)

        print("✅ lead_attribution data saved to MongoDB")
    except Exception as e:
        print(f"❌ Error saving lead attribution: {e}")

# ---------------------------
# Upload ROI metrics
# ---------------------------
def store_roi():
    try:
        df = pd.read_csv("sample_data/roi_metrics.csv")
        records = df.to_dict(orient="records")

        db.roi_metrics.delete_many({})
        db.roi_metrics.insert_many(records)

        print("✅ roi_metrics data saved to MongoDB")
    except Exception as e:
        print(f"❌ Error saving roi_metrics: {e}")

# ---------------------------
# Execute when script is run
# ---------------------------
if __name__ == "__main__":
    store_leads()
    store_roi()
