import os
import pandas as pd
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from pymongo import MongoClient

load_dotenv()

# Global credentials (shared)
GOOGLE_CLIENT_ID = os.getenv("CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GOOGLE_DEVELOPER_TOKEN = os.getenv("DEVELOPER_TOKEN")

# MongoDB setup to load tenants
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["roi_tracker"]
tenants = db.tenants.find({})

def fetch_campaigns_for_tenant(tenant):
    credentials = {
        "developer_token": GOOGLE_DEVELOPER_TOKEN,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": tenant.get("google_ads_refresh_token"),
        "login_customer_id": None,
        "use_proto_plus": True,
    }

    client = GoogleAdsClient.load_from_dict(credentials)
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type,
          campaign.start_date,
          campaign.end_date
        FROM campaign
        LIMIT 20
    """

    customer_id = tenant.get("google_ads_customer_id")
    tenant_id = tenant.get("_id")

    response = ga_service.search_stream(customer_id=customer_id, query=query)

    data = []
    for batch in response:
        for row in batch.results:
            data.append({
                "tenant_id": tenant_id,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "status": row.campaign.status.name,
                "channel": row.campaign.advertising_channel_type.name,
                "start_date": row.campaign.start_date,
                "end_date": row.campaign.end_date,
            })

    df = pd.DataFrame(data)
    return df

if __name__ == "__main__":
    all_data = []
    for tenant in tenants:
        if tenant.get("google_ads_customer_id") and tenant.get("google_ads_refresh_token"):
            try:
                tenant_df = fetch_campaigns_for_tenant(tenant)
                all_data.append(tenant_df)
            except Exception as e:
                print(f"❌ Error fetching for tenant {tenant['_id']}: {e}")

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv("sample_data/google_ads.csv", index=False)
        print("✅ All Google Ads campaign data saved to sample_data/google_ads.csv")
