import pandas as pd

# Load merged attribution data
df = pd.read_csv("sample_data/lead_attribution.csv")

# Fill missing ad spend as 0
df["spend"] = df["spend"].fillna(0)

# Add cost per call (prevent division by 0)
df["cost_per_call"] = df.apply(
    lambda row: round(row["spend"] / 1, 2) if row["call_id"] else 0,
    axis=1
)

# Aggregated metrics per campaign
campaign_summary = df.groupby(
    ["utm_source", "utm_medium", "utm_campaign", "ad_platform"]
).agg(
    total_calls=("call_id", "count"),
    completed_calls=("call_status", lambda x: (x == "completed").sum()),
    missed_calls=("call_status", lambda x: (x == "missed").sum()),
    total_spend=("spend", "sum")
).reset_index()

# Add CPC
campaign_summary["cost_per_call"] = round(
    campaign_summary["total_spend"] / campaign_summary["total_calls"], 2
)

# Save to CSV
campaign_summary.to_csv("sample_data/roi_metrics.csv", index=False)
print("✅ ROI metrics saved to sample_data/roi_metrics.csv")
