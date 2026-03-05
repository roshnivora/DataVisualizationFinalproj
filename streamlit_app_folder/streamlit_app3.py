import streamlit as st
import pandas as pd
import geopandas as gpd
import altair as alt
import json
import os

# -------------------------
# Base directory
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# -------------------------
# Load data
# -------------------------
df = pd.read_csv(os.path.join(BASE_DIR, 'streamlit_data.csv'), dtype={"COUNTYFP": str})

df["COUNTYFP"] = df["COUNTYFP"].str.zfill(3)
df["temp_aqi_corr"] = pd.to_numeric(df["temp_aqi_corr"], errors="coerce")

# GLOBAL color scale limits
global_min = df["temp_aqi_corr"].min()
global_max = df["temp_aqi_corr"].max()

# -------------------------
# Load county shapes
# -------------------------
counties = gpd.read_file(os.path.join(BASE_DIR, 'IL_County_Boundaries'))
counties = counties[["COUNTYFP", "geometry"]]
counties_wgs84 = counties.to_crs(epsg=4326)

# -------------------------
# Title
# -------------------------
st.title("Seasonality of AQI and Temperature Correlation")

# -------------------------
# Season selectors
# -------------------------
seasons = sorted(df["Season"].unique())

col1, col2 = st.columns(2)

with col1:
    season_left = st.selectbox("Left Map Season", seasons, index=0)

with col2:
    season_right = st.selectbox("Right Map Season", seasons, index=1 if len(seasons) > 1 else 0)

# -------------------------
# Map builder
# -------------------------
def build_map(season_choice):

    filtered = df[df["Season"] == season_choice].copy()

    filtered_gdf = counties_wgs84.merge(filtered, on="COUNTYFP", how="right")

    no_aqi = counties_wgs84[
        ~counties_wgs84["COUNTYFP"].isin(filtered_gdf["COUNTYFP"])
    ]

    no_aqi_json = json.loads(no_aqi.to_json())
    filtered_json = json.loads(filtered_gdf.to_json())

    background = alt.Chart(
        alt.Data(values=no_aqi_json["features"])
    ).mark_geoshape(
        fill="lightgray",
        stroke="white",
        strokeWidth=0.5
    )

    foreground = alt.Chart(
        alt.Data(values=filtered_json["features"])
    ).mark_geoshape(
        stroke="white",
        strokeWidth=0.5
    ).encode(
        color=alt.Color(
            "properties.temp_aqi_corr:Q",
            scale=alt.Scale(
                domain=[global_min, global_max],
                range=["#2166ac", "#f7f7f7", "#b2182b"]
            ),
            title="Temp–AQI Correlation"
        ),
        tooltip=[
            alt.Tooltip("properties.COUNTYFP:N", title="County"),
            alt.Tooltip("properties.temp_aqi_corr:Q", title="Correlation", format=".2f")
        ]
    )

    chart = (background + foreground).properties(
        height=550,
        title=season_choice
    ).project(
        type="mercator",
        fit=filtered_json["features"]
    )

    return chart


# -------------------------
# Build maps
# -------------------------
map_left = build_map(season_left)
map_right = build_map(season_right)

# -------------------------
# Show maps
# -------------------------
col1, col2 = st.columns(2)

with col1:
    st.altair_chart(map_left, use_container_width=True)

with col2:
    st.altair_chart(map_right, use_container_width=True)
