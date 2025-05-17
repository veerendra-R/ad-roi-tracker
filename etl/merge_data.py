import pandas as pd

# Load all three datasets
google_ads_df = pd.read_csv("sample_data/google_ads.csv")
meta_ads_df = pd.read_csv("sample_data/meta_ads.csv")
calls_df = pd.read_csv("sample_data/calls.csv")

# Add source column to identify origin
google_ads_df["ad_platform"] = "Google"
meta_ads_df["ad_platform"] = "Meta"

# Combine both ad data into one DataFrame
ads_df = pd.concat([google_ads_df, meta_ads_df], ignore_index=True)

# Lowercase UTM values for consistency
ads_df["utm_source"] = ads_df["utm_source"].str.lower()
ads_df["utm_medium"] = ads_df["utm_medium"].str.lower()
ads_df["utm_campaign"] = ads_df["utm_campaign"].str.lower()

calls_df["utm_source"] = calls_df["utm_source"].str.lower()
calls_df["utm_medium"] = calls_df["utm_medium"].str.lower()
calls_df["utm_campaign"] = calls_df["utm_campaign"].str.lower()

# Merge ads and calls on utm fields
merged_df = pd.merge(
    calls_df,
    ads_df,
    on=["utm_source", "utm_medium", "utm_campaign"],
    how="left",  # left join to keep all calls
    suffixes=("_call", "_ad")
)

# Save output
merged_df.to_csv("sample_data/lead_attribution.csv", index=False)
print("✅ Merged attribution table saved to sample_data/lead_attribution.csv")
