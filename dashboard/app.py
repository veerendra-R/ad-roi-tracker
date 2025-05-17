import streamlit as st
import pandas as pd

# Page setup
st.set_page_config(page_title="Ad ROI Tracker", layout="wide")

# Title
st.title("📊 Local Business Ad ROI Dashboard")

# Load ROI metrics
@st.cache_data
def load_data():
    return pd.read_csv("sample_data/roi_metrics.csv")

df = load_data()

# Sidebar filters
with st.sidebar:
    st.header("🔎 Filter Campaigns")
    platform_filter = st.multiselect("Platform", options=df["ad_platform"].unique(), default=df["ad_platform"].unique())
    source_filter = st.multiselect("UTM Source", options=df["utm_source"].unique(), default=df["utm_source"].unique())

# Apply filters
filtered_df = df[
    (df["ad_platform"].isin(platform_filter)) &
    (df["utm_source"].isin(source_filter))
]

# Show filtered table
st.subheader("📋 ROI Summary Table")
st.dataframe(filtered_df)

# Charts
st.subheader("📈 Calls vs Spend by Campaign")
chart_df = filtered_df.copy()
chart_df["label"] = chart_df["utm_campaign"] + " (" + chart_df["ad_platform"] + ")"

st.bar_chart(chart_df.set_index("label")[["total_calls", "total_spend"]])

st.subheader("💸 Cost Per Call (CPC)")
st.bar_chart(chart_df.set_index("label")["cost_per_call"])
