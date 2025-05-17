import os
import pandas as pd
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

load_dotenv()

ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")  # Example: 'act_1234567890'
APP_ID = os.getenv("META_APP_ID")
APP_SECRET = os.getenv("META_APP_SECRET")

# Initialize SDK
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# Define fields and params
FIELDS = [
    "ad_id", "ad_name", "campaign_id", "campaign_name", "adset_id", "adset_name",
    "impressions", "clicks", "spend", "account_name",
    "utm_source", "utm_medium", "utm_campaign"
]

PARAMS = {
    "level": "ad",
    "date_preset": "last_7d",
    "time_increment": 1,
    "limit": 25,
}

def fetch_meta_ads():
    ad_account = AdAccount(AD_ACCOUNT_ID)
    ads = ad_account.get_insights(fields=FIELDS, params=PARAMS)

    # Extract relevant fields into dataframe
    rows = []
    for ad in ads:
        row = {field: ad.get(field, "") for field in FIELDS}
        rows.append(row)

    df = pd.DataFrame(rows)

    # Save to CSV
    df.to_csv("sample_data/meta_ads.csv", index=False)
    print("✅ Meta Ads data saved to sample_data/meta_ads.csv")

if __name__ == "__main__":
    fetch_meta_ads()
