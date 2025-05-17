import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["roi_tracker"]

# Load merged data
df = pd.read_csv("sample_data/lead_attribution.csv")
df["spend"] = df["spend"].fillna(0)

# Push raw attribution data
db.lead_attribution.delete_many({})
db.lead_attribution.insert_many(df.to_dict(orient="records"))
print("✅ lead_attribution data saved to MongoDB")

# Aggregate ROI metrics per tenant + campaign
roi_df = df.groupby(
    ["tenant_id", "ad_platform", "utm_source", "utm_medium", "utm_campaign"]
).agg(
    total_calls=("call_id", "count"),
    completed_calls=("call_status", lambda x: (x == "completed").sum()),
    missed_calls=("call_status", lambda x: (x == "missed").sum()),
    total_spend=("spend", "sum")
).reset_index()

roi_df["cost_per_call"] = round(roi_df["total_spend"] / roi_df["total_calls"].replace(0, 1), 2)

# Push ROI summary
db.roi_metrics.delete_many({})
db.roi_metrics.insert_many(roi_df.to_dict(orient="records"))
print("✅ roi_metrics saved to MongoDB")
