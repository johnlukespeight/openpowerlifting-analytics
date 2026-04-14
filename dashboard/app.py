"""
Powerlifting Analytics Dashboard
Streamlit app backed by BigQuery mart tables.
"""

import os
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

st.set_page_config(
    page_title="Powerlifting Analytics",
    page_icon="🏋️",
    layout="wide",
)

# ── Credentials: Streamlit Cloud secrets take priority over local .env ────────
def _get_credentials_and_project():
    # Running on Streamlit Cloud — secrets stored via the UI
    if "gcp_service_account" in st.secrets:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        project = (
            st.secrets.get("GCP_PROJECT_ID")
            or st.secrets.get("BQ_PROJECT")
            or st.secrets["gcp_service_account"]["project_id"]
        )
        return creds, project

    # Running locally — read from .env / environment
    key_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    project   = os.environ.get("BQ_PROJECT") or os.environ.get("GCP_PROJECT_ID")
    if not key_file or not project:
        st.error(
            "Missing credentials. Set GOOGLE_APPLICATION_CREDENTIALS and "
            "GCP_PROJECT_ID in your .env, or add secrets in Streamlit Cloud."
        )
        st.stop()
    creds = service_account.Credentials.from_service_account_file(key_file)
    return creds, project

DATASET = (
    st.secrets.get("BQ_DATASET")
    if "gcp_service_account" in st.secrets
    else os.environ.get("BQ_DATASET", "powerlifting")
)

# ── BigQuery client ───────────────────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    creds, project = _get_credentials_and_project()
    return bigquery.Client(project=project, credentials=creds)

PROJECT = (
    st.secrets.get("GCP_PROJECT_ID")
    or st.secrets.get("BQ_PROJECT")
    if "gcp_service_account" in st.secrets
    else os.environ.get("BQ_PROJECT") or os.environ.get("GCP_PROJECT_ID")
)

@st.cache_data(ttl=3600)
def query(_client, sql: str) -> pd.DataFrame:
    return _client.query(sql).to_dataframe()

client = get_bq_client()

# ── Load data ─────────────────────────────────────────────────────────────────
fed_stats = query(client, f"""
    SELECT * FROM `{PROJECT}.{DATASET}.mart_federation_stats`
    ORDER BY year, federation
""")

athletes = query(client, f"""
    SELECT * FROM `{PROJECT}.{DATASET}.mart_athlete_records`
    ORDER BY best_total_kg DESC
""")

wc_trends = query(client, f"""
    SELECT * FROM `{PROJECT}.{DATASET}.mart_weight_class_trends`
    WHERE weight_class_kg NOT IN ('+', '')
      AND SAFE_CAST(weight_class_kg AS FLOAT64) IS NOT NULL
    ORDER BY year, weight_class_kg
""")

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.title("Filters")

year_min = int(fed_stats["year"].min())
year_max = int(fed_stats["year"].max())
year_range = st.sidebar.slider("Year range", year_min, year_max, (2000, year_max))

sex_options = sorted(athletes["sex"].dropna().unique().tolist())
sex_filter = st.sidebar.multiselect("Sex", sex_options, default=sex_options)

equip_options = sorted(athletes["equipment"].dropna().unique().tolist())
equip_filter = st.sidebar.multiselect("Equipment", equip_options, default=equip_options)

# Apply filters
fed_filtered = fed_stats[
    (fed_stats["year"] >= year_range[0]) &
    (fed_stats["year"] <= year_range[1])
]

ath_filtered = athletes[
    athletes["sex"].isin(sex_filter) &
    athletes["equipment"].isin(equip_filter)
]

wc_filtered = wc_trends[
    (wc_trends["year"] >= year_range[0]) &
    (wc_trends["year"] <= year_range[1]) &
    (wc_trends["sex"].isin(sex_filter)) &
    (wc_trends["equipment"].isin(equip_filter))
]

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏋️ Powerlifting Analytics")
st.caption(f"Data: OpenPowerlifting · Warehouse: BigQuery · Transformed by dbt")

# ── KPI row ───────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total athletes",     f"{ath_filtered['athlete_name'].nunique():,}")
k2.metric("Total competitions", f"{int(fed_filtered['meet_count'].sum()):,}")
k3.metric("Federations",        f"{fed_filtered['federation'].nunique():,}")
k4.metric("Avg Dots (filtered)",f"{fed_filtered['avg_dots_score'].mean():.1f}")

st.divider()

# ── Row 1 ─────────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Average Dots Score Over Time")
    # Top 8 federations by total athlete count to keep chart readable
    top_feds = (
        fed_filtered.groupby("federation")["athlete_count"]
        .sum()
        .nlargest(8)
        .index.tolist()
    )
    dots_data = fed_filtered[fed_filtered["federation"].isin(top_feds)]
    fig = px.line(
        dots_data,
        x="year", y="avg_dots_score", color="federation",
        labels={"year": "Year", "avg_dots_score": "Avg Dots", "federation": "Federation"},
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top 10 Federations by Athlete Count")
    top10 = (
        fed_filtered.groupby("federation")["athlete_count"]
        .sum()
        .nlargest(10)
        .reset_index()
        .sort_values("athlete_count")
    )
    fig = px.bar(
        top10, x="athlete_count", y="federation", orientation="h",
        labels={"athlete_count": "Athletes", "federation": "Federation"},
        color="athlete_count", color_continuous_scale="Blues",
    )
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

# ── Row 2 ─────────────────────────────────────────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("Equipment Breakdown")
    equip_counts = (
        ath_filtered.groupby("equipment")["athlete_name"]
        .count()
        .reset_index()
        .rename(columns={"athlete_name": "count"})
    )
    fig = px.pie(
        equip_counts, names="equipment", values="count",
        hole=0.4,
    )
    st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("Avg Total (kg) by Weight Class Over Time")
    wc_agg = (
        wc_filtered.groupby(["year", "weight_class_kg"])["avg_total_kg"]
        .mean()
        .reset_index()
    )
    # Show a selection of common weight classes
    common_wc = ["59", "66", "74", "83", "93", "105", "120"]
    wc_plot = wc_agg[wc_agg["weight_class_kg"].isin(common_wc)]
    fig = px.line(
        wc_plot, x="year", y="avg_total_kg", color="weight_class_kg",
        labels={"year": "Year", "avg_total_kg": "Avg Total (kg)", "weight_class_kg": "Weight Class"},
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Leaderboard ───────────────────────────────────────────────────────────────
st.subheader("🏆 All-Time Leaderboard")
top_n = st.slider("Show top N athletes", 10, 100, 25)
leaderboard = (
    ath_filtered[["athlete_name", "sex", "weight_class_kg", "equipment",
                  "federation", "country", "best_total_kg", "best_dots", "latest_competition"]]
    .dropna(subset=["best_total_kg"])
    .nlargest(top_n, "best_total_kg")
    .reset_index(drop=True)
)
leaderboard.index += 1
st.dataframe(leaderboard, use_container_width=True)
