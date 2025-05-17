import os
import pandas as pd
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from pymongo import MongoClient

load_dotenv()

# Shared credentials
APP_ID = os.getenv("META_APP_ID")
APP_SECRET = os.getenv("META_APP_SECRET")

# MongoDB tenants
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["roi_tracker"]
tenants = db.tenants.find({})

# Meta fields
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

def fetch_meta_ads_for_tenant(tenant):
    access_token = tenant.get("meta_access_token")
    ad_account_id = tenant.get("meta_ad_account_id")
    tenant_id = tenant.get("_id")

    FacebookAdsApi.init(APP_ID, APP_SECRET, access_token)

    ad_account = AdAccount(ad_account_id)
    ads = ad_account.get_insights(fields=FIELDS, params=PARAMS)

    rows = []
    for ad in ads:
        row = {field: ad.get(field, "") for field in FIELDS}
        row["tenant_id"] = tenant_id
        rows.append(row)

    return pd.DataFrame(rows)

if __name__ == "__main__":
    all_data = []
    for tenant in tenants:
        if tenant.get("meta_ad_account_id") and tenant.get("meta_access_token"):
            try:
                tenant_df = fetch_meta_ads_for_tenant(tenant)
                all_data.append(tenant_df)
            except Exception as e:
                print(f"❌ Error fetching Meta ads for tenant {tenant['_id']}: {e}")

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv("sample_data/meta_ads.csv", index=False)
        print("✅ All Meta Ads data saved to sample_data/meta_ads.csv")
