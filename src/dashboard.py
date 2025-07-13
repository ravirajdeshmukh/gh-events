import streamlit as st
import requests

# Base URL for the FastAPI backend
API_BASE = "http://localhost:9000/kpi"

# Set Streamlit app title
st.title("📊 GitHub Events Metrics")

# Define two tabs for different metrics
tab1, tab2 = st.tabs(["Event Counts", "PR Time"])

# ----------------------------
# 📌 Tab 1: Event Count by Offset
# ----------------------------
with tab1:
    st.header("Event Counts by Offset")
    
    # Let user select a time window (in minutes)
    offset = st.slider("Minutes to Look Back", 5, 100, 50)

    # Button to fetch event counts
    if st.button("Fetch Counts"):
        # Call API endpoint with selected offset
        r = requests.get(f"{API_BASE}/event_count_offset?offset={offset}")
        
        if r.ok:
            data = r.json()["data"]
            # Visualize event counts as bar chart
            st.bar_chart({d['event_type']: d['count'] for d in data})
        else:
            st.error("Failed to fetch data")

# ----------------------------
# 📌 Tab 2: Average PR Time by Repo
# ----------------------------
with tab2:
    st.header("Average Time Between PRs")

    # Button to fetch PR time averages
    if st.button("Fetch Average"):
        r = requests.get(f"{API_BASE}/avg_pr_time")
        
        if r.ok:
            data = r.json()["data"]
            # Show table of repo name and average time
            st.subheader("Results (repo + avg_minutes)")
            st.table(data)
        else:
            st.error("Error fetching average PR time")
