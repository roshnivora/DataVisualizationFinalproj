import streamlit as st
import pandas as pd
import geopandas as gpd
import altair as alt
import json
import os

# -------------------------
# Base directory for file paths
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# -------------------------
# Load data
# -------------------------
df = pd.read_csv(os.path.join(BASE_DIR, 'streamlit_data.csv'), dtype={"COUNTYFP": str})

# Ensure COUNTYFP is 3-digit
df["COUNTYFP"] = df["COUNTYFP"].str.zfill(3)

# Load county shapefile
counties = gpd.read_file(os.path.join(BASE_DIR, 'IL_County_Boundaries'))
counties = counties[["COUNTYFP", "geometry"]]

# Ensure CRS is WGS84
counties_wgs84 = counties.to_crs(epsg=4326)

# -------------------------
# Get global min/max for shared color scale
# -------------------------
all_corr = pd.to_numeric(df["temp_aqi_corr"], errors='coerce').fillna(0)
corr_min = all_corr.min()
corr_max = all_corr.max()

# -------------------------
# Helper function to build a map chart for a given season
# -------------------------
def build_map(season):
    filtered = df[df["Season"] == season].copy()
    filtered["temp_aqi_corr"] = pd.to_numeric(filtered["temp_aqi_corr"], errors='coerce').fillna(0)

    # Merge with geometry
    filtered_gdf = counties_wgs84.merge(filtered, on="COUNTYFP", how="right")

    # Background counties (no AQI data)
    no_aqi = counties_wgs84[~counties_wgs84["COUNTYFP"].isin(filtered_gdf["COUNTYFP"])]

    # Convert to GeoJSON
    no_aqi_json = json.loads(no_aqi.to_json())
    filtered_json = json.loads(filtered_gdf.to_json())

    # Background layer (gray)
    background = alt.Chart(
        alt.Data(values=no_aqi_json["features"])
    ).mark_geoshape(
        fill='lightgray',
        stroke='white',
        strokeWidth=0.5,
        opacity=1
    )

    # Foreground layer (colored) — uses shared color scale
    foreground = alt.Chart(
        alt.Data(values=filtered_json["features"])
    ).mark_geoshape(
        stroke='white',
        strokeWidth=0.5,
        opacity=1
    ).encode(
        color=alt.Color(
            'properties.temp_aqi_corr:Q',
            scale=alt.Scale(
                domain=[corr_min, corr_max],
                range=['#2166ac', '#f7f7f7', '#b2182b']  # blue → white → red
            ),
            title='Temp-AQI Correlation'
        ),
        tooltip=[
            alt.Tooltip('properties.COUNTYFP:N', title='County'),
            alt.Tooltip('properties.temp_aqi_corr:Q', title='Correlation', format=".2f")
        ]
    )

    chart = (background + foreground).properties(
        width=340,
        height=500,
        title=season
    ).project(
        type='mercator',
        center=[-89.3, 40.0],
        scale=2500
    )

    return chart

# -------------------------
# Streamlit UI
# -------------------------
st.title("Seasonality of AQI and Temperature Correlation")

seasons = df["Season"].unique().tolist()

col1, col2 = st.columns(2)

with col1:
    season_left = st.selectbox("Select first season", options=seasons, index=0, key="left")

with col2:
    season_right = st.selectbox("Select second season", options=seasons, index=min(1, len(seasons) - 1), key="right")

# Build and display maps side by side
map_left = build_map(season_left)
map_right = build_map(season_right)

combined = alt.hconcat(map_left, map_right).resolve_scale(
    color='shared'
)

st.altair_chart(combined, use_container_width=True)