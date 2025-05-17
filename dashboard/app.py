import streamlit as st
import pandas as pd
import streamlit_authenticator as stauth
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# ---------------------------
# Page Config (must be first)
# ---------------------------
st.set_page_config(page_title="Ad ROI Tracker", layout="wide")

# ---------------------------
# MongoDB Setup
# ---------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["roi_tracker"]

# ---------------------------
# Auth Setup
# ---------------------------
hashed_passwords = stauth.Hasher(['demo123']).generate()

authenticator = stauth.Authenticate(
    {
        "usernames": {
            "demo@tracker.com": {
                "name": "Demo User",
                "password": hashed_passwords[0]
            }
        }
    },
    "roi_dashboard",
    "auth_cookie",
    cookie_expiry_days=1
)

name, auth_status, username = authenticator.login("Login", "sidebar")

if auth_status is False:
    st.error("Invalid credentials")
elif auth_status is None:
    st.warning("Please enter your credentials")
elif auth_status:
    st.title(f"📊 Welcome, {name}!")

    # ---------------------------
    # Load ROI Data from MongoDB
    # ---------------------------
    roi_data = list(db.roi_metrics.find({}))
    if not roi_data:
        st.warning("No ROI data found in MongoDB.")
        st.stop()

    df = pd.DataFrame(roi_data)

    # ---------------------------
    # Sidebar Filters
    # ---------------------------
    with st.sidebar:
        st.subheader("🔎 Filter Campaigns")

        # Tenant Filter (new)
        tenant_ids = df["tenant_id"].dropna().unique().tolist()
        tenant_filter = st.multiselect("Tenant", tenant_ids, default=tenant_ids)

        # Existing filters
        platform_filter = st.multiselect("Platform", options=df["ad_platform"].unique(), default=df["ad_platform"].unique())
        source_filter = st.multiselect("UTM Source", options=df["utm_source"].unique(), default=df["utm_source"].unique())

    # ---------------------------
    # Apply Filters
    # ---------------------------
    filtered_df = df[
        (df["tenant_id"].isin(tenant_filter)) &
        (df["ad_platform"].isin(platform_filter)) &
        (df["utm_source"].isin(source_filter))
    ]

    # ---------------------------
    # Show ROI Table & Charts
    # ---------------------------
    st.subheader("📋 ROI Summary Table")
    st.dataframe(filtered_df)

    st.subheader("📈 Calls vs Spend by Campaign")
    chart_df = filtered_df.copy()
    chart_df["label"] = chart_df["utm_campaign"] + " (" + chart_df["ad_platform"] + ")"
    st.bar_chart(chart_df.set_index("label")[["total_calls", "total_spend"]])

    st.subheader("💸 Cost Per Call (CPC)")
    st.bar_chart(chart_df.set_index("label")["cost_per_call"])

    # ---------------------------
    # Logout Button
    # ---------------------------
    authenticator.logout("Logout", "sidebar")
