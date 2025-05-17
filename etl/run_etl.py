import os
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from google.ads.googleads.client import GoogleAdsClient
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from utils.tenant_config_loader import get_all_tenant_configs

# Load environment
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
GOOGLE_CLIENT_ID = os.getenv("CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GOOGLE_DEV_TOKEN = os.getenv("DEVELOPER_TOKEN")
META_APP_ID = os.getenv("META_APP_ID")
META_APP_SECRET = os.getenv("META_APP_SECRET")

# Setup MongoDB
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["roi_tracker"]

# Step 1 - Fetch Google Ads per tenant
def fetch_google_ads():
    all_data = []
    tenants = get_all_tenant_configs()
    for tenant in tenants:
        google_config = tenant.get("platforms", {}).get("google_ads", {})
        refresh_token = google_config.get("refresh_token")
        customer_id = google_config.get("customer_id")
        tenant_id = tenant.get("_id")

        if not refresh_token or not customer_id:
            continue

        credentials = {
            "developer_token": GOOGLE_DEV_TOKEN,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "login_customer_id": None,
            "use_proto_plus": True,
        }

        try:
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
                        "utm_source": "google",
                        "utm_medium": "cpc",
                        "utm_campaign": row.campaign.name.lower().replace(" ", "_")
                    })

            if data:
                all_data.append(pd.DataFrame(data))
        except Exception as e:
            print(f"\u274c Google fetch failed for tenant {tenant_id}: {e}")

    if all_data:
        pd.concat(all_data, ignore_index=True).to_csv("sample_data/google_ads.csv", index=False)
        print("\u2705 Google Ads data saved")

# Step 2 - Fetch Meta Ads per tenant
def fetch_meta_ads():
    tenants = get_all_tenant_configs()
    all_data = []

    for tenant in tenants:
        meta_config = tenant.get("platforms", {}).get("meta_ads", {})
        access_token = meta_config.get("access_token")
        ad_account_id = meta_config.get("ad_account_id")
        tenant_id = tenant.get("_id")

        if not access_token or not ad_account_id:
            continue

        try:
            FacebookAdsApi.init(META_APP_ID, META_APP_SECRET, access_token)
            ad_account = AdAccount(ad_account_id)
            ads = ad_account.get_insights(
                fields=[
                    "ad_id", "ad_name", "campaign_id", "campaign_name", "adset_id", "adset_name",
                    "impressions", "clicks", "spend", "account_name",
                    "utm_source", "utm_medium", "utm_campaign"
                ],
                params={
                    "level": "ad",
                    "date_preset": "last_7d",
                    "time_increment": 1,
                    "limit": 25
                }
            )

            rows = []
            for ad in ads:
                row = {k: ad.get(k, "") for k in ad.keys()}
                row["tenant_id"] = tenant_id
                rows.append(row)

            if rows:
                all_data.append(pd.DataFrame(rows))
        except Exception as e:
            print(f"\u274c Meta fetch failed for tenant {tenant_id}: {e}")

    if all_data:
        pd.concat(all_data, ignore_index=True).to_csv("sample_data/meta_ads.csv", index=False)
        print("\u2705 Meta Ads data saved")

# Step 3 - Merge Ads + Calls
def merge_ads_and_calls():
    g_df = pd.read_csv("sample_data/google_ads.csv")
    m_df = pd.read_csv("sample_data/meta_ads.csv")
    c_df = pd.read_csv("sample_data/calls.csv")

    g_df["ad_platform"] = "Google"
    m_df["ad_platform"] = "Meta"

    ads_df = pd.concat([g_df, m_df], ignore_index=True)
    for col in ["utm_source", "utm_medium", "utm_campaign"]:
        ads_df[col] = ads_df[col].fillna("").str.lower()
        c_df[col] = c_df[col].fillna("").str.lower()

    merged_df = pd.merge(
        c_df,
        ads_df,
        on=["utm_source", "utm_medium", "utm_campaign"],
        how="left",
        suffixes=("_call", "_ad")
    )

    merged_df.to_csv("sample_data/lead_attribution.csv", index=False)
    print("\u2705 Merged data saved to lead_attribution.csv")

# Step 4 - Aggregate ROI and Store to MongoDB
def compute_and_store_metrics():
    df = pd.read_csv("sample_data/lead_attribution.csv")
    df["spend"] = df["spend"].fillna(0)

    db.lead_attribution.delete_many({})
    db.lead_attribution.insert_many(df.to_dict(orient="records"))
    print("\u2705 lead_attribution inserted")

    roi_df = df.groupby(
        ["tenant_id", "ad_platform", "utm_source", "utm_medium", "utm_campaign"]
    ).agg(
        total_calls=("call_id", "count"),
        completed_calls=("call_status", lambda x: (x == "completed").sum()),
        missed_calls=("call_status", lambda x: (x == "missed").sum()),
        total_spend=("spend", "sum")
    ).reset_index()

    roi_df["cost_per_call"] = round(roi_df["total_spend"] / roi_df["total_calls"].replace(0, 1), 2)

    db.roi_metrics.delete_many({})
    db.roi_metrics.insert_many(roi_df.to_dict(orient="records"))
    print("\u2705 roi_metrics inserted")

# ---------------------------
# Run All
# ---------------------------
if __name__ == "__main__":
    # fetch_google_ads()
    # fetch_meta_ads()
    merge_ads_and_calls()
    compute_and_store_metrics()
    print("\ud83c\udf1f ETL run completed successfully.")
