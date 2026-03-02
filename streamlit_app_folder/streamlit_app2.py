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
# Streamlit dropdown for season
# -------------------------
season_choice = st.selectbox(
    "Select a season",
    options=df["Season"].unique()
)

# Filter data by season
filtered = df[df["Season"] == season_choice].copy()
filtered["temp_aqi_corr"] = pd.to_numeric(filtered["temp_aqi_corr"], errors='coerce').fillna(0)

# Merge with geometry
filtered_gdf = counties_wgs84.merge(filtered, on="COUNTYFP", how="right")

# Split background/foreground
no_aqi = counties_wgs84[~counties_wgs84["COUNTYFP"].isin(filtered_gdf["COUNTYFP"])]

# Convert to GeoJSON
no_aqi_json = json.loads(no_aqi.to_json())
filtered_json = json.loads(filtered_gdf.to_json())

# Determine actual min/max correlations for scale
corr_min = filtered_gdf["temp_aqi_corr"].min()
corr_max = filtered_gdf["temp_aqi_corr"].max()

# -------------------------
# Background layer (gray)
# -------------------------
background = alt.Chart(
    alt.Data(values=no_aqi_json["features"])
).mark_geoshape(
    fill='lightgray',
    stroke='white',
    strokeWidth=0.5,
    opacity=1
)

# -------------------------
# Foreground layer (colored)
# -------------------------
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

# -------------------------
# Add a title
# -------------------------
st.title("Seasonality of AQI and Temperature Correlation")

# -------------------------
# Combine layers with adjusted projection
# -------------------------
chart = (background + foreground).properties(
    width=700,
    height=650
).project(
    type='mercator',
    center=[-89.3, 40.0],
    scale=5200
)

# -------------------------
# Display in Streamlit
# -------------------------
st.altair_chart(chart, use_container_width=True)