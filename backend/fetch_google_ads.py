import os
from google.ads.googleads.client import GoogleAdsClient
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Load credentials
credentials = {
    "developer_token": os.getenv("DEVELOPER_TOKEN"),
    "client_id": os.getenv("CLIENT_ID"),
    "client_secret": os.getenv("CLIENT_SECRET"),
    "refresh_token": os.getenv("REFRESH_TOKEN"),
    "login_customer_id": None,
    "use_proto_plus": True,
}

client = GoogleAdsClient.load_from_dict(credentials)

def fetch_campaigns(customer_id):
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
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "status": row.campaign.status.name,
                "channel": row.campaign.advertising_channel_type.name,
                "start_date": row.campaign.start_date,
                "end_date": row.campaign.end_date,
            })

    df = pd.DataFrame(data)
    df.to_csv("sample_data/google_ads.csv", index=False)
    print("✅ Google Ads campaign data saved to sample_data/google_ads.csv")

if __name__ == "__main__":
    fetch_campaigns(os.getenv("CUSTOMER_ID"))
