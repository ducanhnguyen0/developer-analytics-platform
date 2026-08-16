import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import streamlit as st

from streamlit_autorefresh import st_autorefresh as _autorefresh
from backend.config import settings
from backend.service.queries import (
    get_event_counts_over_time,
    get_top_event_types,
    get_top_active_repos,
    get_trending_repos,
    get_new_repos,
    get_top_contributors,
    get_activity_by_category,
    get_total_event_count,
)

st.set_page_config(page_title="Developer Analytics Platform", layout="wide")

st_autorefresh = _autorefresh

st.title("Developer Analytics Platform")
st.caption("Live GitHub activity")

st_autorefresh(interval=settings.DASHBOARD_REFRESH_SECONDS * 1000, key="dashboard_refresh")

lookback = st.sidebar.slider(
    "Lookback window (minutes)", min_value=5, max_value=120,
    value=settings.DASHBOARD_LOOKBACK_MINUTES, step=5,
)


# Top-line metric
total_events = get_total_event_count(lookback_minutes=lookback)
st.metric(label=f"Total events (last {lookback} min)", value=f"{total_events:,}")


# Events over time, by type
st.subheader("Event volume over time")
time_series = get_event_counts_over_time(lookback_minutes=lookback)
if time_series:
    df = pd.DataFrame(time_series)
    pivoted = df.pivot_table(
        index="window_start", columns="event_type", values="event_count", fill_value=0,
    )
    st.line_chart(pivoted)
else:
    st.info("No data yet — make sure the producer and streaming job are running.")


# Activity category breakdown
st.subheader("What kind of activity is happening")
categories = get_activity_by_category(lookback_minutes=lookback)
if categories:
    cat_df = pd.DataFrame(categories).set_index("category")
    st.bar_chart(cat_df)
else:
    st.info("No data yet.")


# Trending vs top repos
col1, col2 = st.columns(2)

with col1:
    st.subheader("Trending repos")
    st.caption("Rising activity: recent half of the window vs the earlier half")
    trending = get_trending_repos(lookback_minutes=lookback)
    if trending:
        st.dataframe(pd.DataFrame(trending), use_container_width=True, hide_index=True)
    else:
        st.info("No data yet.")

with col2:
    st.subheader("Most active repos (overall)")
    st.caption("Repos with the highest activity over the selected window")
    top_repos = get_top_active_repos(lookback_minutes=lookback)
    if top_repos:
        st.dataframe(pd.DataFrame(top_repos), use_container_width=True, hide_index=True)
    else:
        st.info("No data yet.")


# New repos + top contributors
col3, col4 = st.columns(2)

with col3:
    st.subheader("New repos")
    st.caption("First seen within this lookback window")
    new_repos = get_new_repos(lookback_minutes=lookback)
    if new_repos:
        st.dataframe(pd.DataFrame(new_repos), use_container_width=True, hide_index=True)
    else:
        st.info("No newly-seen repos in this window.")

with col4:
    st.subheader("Top contributors")
    st.caption("Contributors with the highest activity over the selected window")
    top_contributors = get_top_contributors(lookback_minutes=lookback)
    if top_contributors:
        st.dataframe(pd.DataFrame(top_contributors), use_container_width=True, hide_index=True)
    else:
        st.info("No data yet.")


# Raw event-type table
st.subheader("Top event types")
top_types = get_top_event_types(lookback_minutes=lookback)
if top_types:
    st.dataframe(pd.DataFrame(top_types), use_container_width=True, hide_index=True)
else:
    st.info("No data yet.")
