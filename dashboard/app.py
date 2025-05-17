import streamlit as st
import pandas as pd
import streamlit_authenticator as stauth
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import subprocess
import time

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
    # Fetch User Record and Role
    # ---------------------------
    user_record = db.tenants.find_one({"name": name})
    user_role = user_record.get("role", "") if user_record else ""
    is_admin = user_role in ["admin", "superadmin"]
    is_superadmin = user_role == "superadmin"

    # ---------------------------
    # Show Last ETL Run
    # ---------------------------
    status = db.etl_status.find_one({"_id": "last_run"})
    if status:
        st.caption(f"🕒 Last ETL run: {pd.to_datetime(status['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")

    # ---------------------------
    # Admin/Superadmin: ETL Button
    # ---------------------------
    if is_admin:
        with st.sidebar:
            if st.button("🔄 Refresh All Tenant Data"):
                with st.spinner("Running full ETL pipeline..."):
                    result = subprocess.run(["python3", "etl/run_etl.py"], capture_output=True, text=True)
                    time.sleep(2)
                    if result.returncode == 0:
                        db.etl_status.update_one(
                            {"_id": "last_run"},
                            {"$set": {"timestamp": pd.Timestamp.now().isoformat()}},
                            upsert=True
                        )
                        st.success("✅ ETL pipeline completed successfully!")
                    else:
                        st.error("❌ ETL pipeline failed.")
                        st.text(result.stderr)

    # ---------------------------
    # Load ROI Data from MongoDB
    # ---------------------------
    roi_data = list(db.roi_metrics.find({}))
    if not roi_data:
        st.warning("No ROI data found in MongoDB.")
        st.stop()
    df = pd.DataFrame(roi_data)

    # ---------------------------
    # Join with Tenants Collection
    # ---------------------------
    tenants = list(db.tenants.find({}))
    tenant_df = pd.DataFrame(tenants)[["_id", "name"]].rename(columns={"_id": "tenant_id", "name": "tenant_name"})
    df = df.merge(tenant_df, on="tenant_id", how="left")

    # ---------------------------
    # Regular User Sees Only Their Data
    # ---------------------------
    if not is_admin:
        df = df[df["tenant_id"] == user_record["_id"]]

    # ---------------------------
    # Sidebar Filters
    # ---------------------------
    with st.sidebar:
        st.subheader("🔎 Filter Campaigns")

        tenant_names = df["tenant_name"].dropna().unique().tolist()
        selected_tenants = st.multiselect("Tenant", tenant_names, default=tenant_names)

        platform_filter = st.multiselect("Platform", options=df["ad_platform"].unique(), default=df["ad_platform"].unique())
        source_filter = st.multiselect("UTM Source", options=df["utm_source"].unique(), default=df["utm_source"].unique())

    # ---------------------------
    # Apply Filters
    # ---------------------------
    filtered_df = df[
        (df["tenant_name"].isin(selected_tenants)) &
        (df["ad_platform"].isin(platform_filter)) &
        (df["utm_source"].isin(source_filter))
    ]

    # ---------------------------
    # Show ROI Table & Charts
    # ---------------------------
    st.subheader("📋 ROI Summary Table")
    st.dataframe(filtered_df)

    st.download_button(
        label="📥 Download CSV",
        data=filtered_df.to_csv(index=False),
        file_name="filtered_roi_data.csv",
        mime="text/csv"
    )

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
