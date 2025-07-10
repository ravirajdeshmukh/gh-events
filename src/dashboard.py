import streamlit as st
import requests

API_BASE = "http://localhost:9000/kpi"

st.title("📊 GitHub Events Metrics")

tab1, tab2 = st.tabs(["Event Counts", "PR Time"])

with tab1:
    st.header("Event Counts by Offset")
    offset = st.slider("Minutes to Look Back", 5, 10000, 500)

    if st.button("Fetch Counts"):
        r = requests.get(f"{API_BASE}/event_count_offset?offset={offset}")
        if r.ok:
            data = r.json()["data"]
            st.bar_chart({d['event_type']: d['count'] for d in data})
        else:
            st.error("Failed to fetch data")

with tab2:
    st.header("Average Time Between PRs")
    if st.button("Fetch Average"):
        r = requests.get(f"{API_BASE}/avg_pr_time")
        if r.ok:
            data=r.json()["data"]
            st.subheader("Results (repo + avg_minutes)")
            st.table(data)
        else:
            st.error("Error fetching average PR time")