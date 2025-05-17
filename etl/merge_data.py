import pandas as pd

# Load ads data (combined)
google_df = pd.read_csv("sample_data/google_ads.csv")
meta_df = pd.read_csv("sample_data/meta_ads.csv")
calls_df = pd.read_csv("sample_data/calls.csv")

# Add platform columns
google_df["ad_platform"] = "Google"
meta_df["ad_platform"] = "Meta"

# Combine ads
ads_df = pd.concat([google_df, meta_df], ignore_index=True)

# Normalize UTM fields for matching
ads_df["utm_source"] = ads_df["utm_source"].str.lower().fillna("")
ads_df["utm_medium"] = ads_df["utm_medium"].str.lower().fillna("")
ads_df["utm_campaign"] = ads_df["utm_campaign"].str.lower().fillna("")

calls_df["utm_source"] = calls_df["utm_source"].str.lower().fillna("")
calls_df["utm_medium"] = calls_df["utm_medium"].str.lower().fillna("")
calls_df["utm_campaign"] = calls_df["utm_campaign"].str.lower().fillna("")

# Merge on UTM fields
merged_df = pd.merge(
    calls_df,
    ads_df,
    on=["utm_source", "utm_medium", "utm_campaign"],
    how="left",
    suffixes=("_call", "_ad")
)

# Save merged attribution
merged_df.to_csv("sample_data/lead_attribution.csv", index=False)
print("✅ Merged lead attribution saved to sample_data/lead_attribution.csv")
