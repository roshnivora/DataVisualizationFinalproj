# import pandas as pd
# import numpy as np
# import os as os
# import geopandas as gpd
# from shapely.geometry import Point

# os.chdir(os.path.dirname(os.path.abspath(__file__)))
# PATH = os.path.join('data', 'derived-data')
# RAW_PATH = os.path.join('data', 'raw-data')
# temp_path = os.path.join(PATH, 'all_weather.csv')
# aqi_path = os.path.join(PATH, 'aqi_all.csv')
# temp_narrow = pd.read_csv(temp_path)
# aqi_df = pd.read_csv(aqi_path)

# # Convert lat/lon to geometry points
# geometry = [Point(xy) for xy in zip(temp_narrow['LONGITUDE'], temp_narrow['LATITUDE'])]
# temp_gdf = gpd.GeoDataFrame(temp_narrow, geometry=geometry)

# # Make sure you set a CRS (coordinate reference system)
# # Most lat/lon data is EPSG:4326
# temp_gdf.set_crs(epsg=4326, inplace=True)

# # Replace with the path to your county shapefile
# counties_path = os.path.join(RAW_PATH, 'tl_2025_us_county')
# counties = gpd.read_file(counties_path)

# # Make sure the counties GeoDataFrame is in the same CRS
# counties = counties.to_crs(temp_gdf.crs)

# # Perform spatial join
# temp_with_county = gpd.sjoin(
#     temp_gdf,
#     counties[['COUNTYFP', 'geometry']],  # keep only county_fp and geometry for spatial join
#     how="left",
#     predicate="intersects"
# )

# # ---------------------------
# # Step 1: Map county names to COUNTYFP for Illinois
# # Only include the counties present in temp_with_county
# county_to_fp = {
#     "Adams": "001",
#     "Champaign": "019",
#     "Clark": "023",
#     "Cook": "031",
#     "DuPage": "043",
#     "Jo Daviess": "085",
#     "Kane": "089",
#     "McHenry": "111",
#     "McLean": "113",
#     "Macon": "115",
#     "Mercer": "131",
#     "Peoria": "143",
#     "Randolph": "157",
#     "Rock Island": "161",
#     "Sangamon": "167",
#     "Winnebago": "201",
# }

# # Clean 'county Name' to match mapping (strip ' County' if present)
# aqi_df['county Name'] = aqi_df['county Name'].str.replace(" County$", "", regex=True)

# # Create COUNTYFP column in AQI df
# aqi_df['COUNTYFP'] = aqi_df['county Name'].map(county_to_fp)

# # Optional: check for unmatched counties
# missing = aqi_df[aqi_df['COUNTYFP'].isna()]['county Name'].unique()
# if len(missing) > 0:
#     print("Warning: These AQI counties did not map to COUNTYFP:", missing)

# # ---------------------------
# # Step 2: Restrict AQI df to counties present in temperature data
# temp_counties = temp_with_county['COUNTYFP'].unique()
# aqi_df_restricted = aqi_df[aqi_df['COUNTYFP'].isin(temp_counties)].copy()

# # ---------------------------
# # Step 3: Standardize date columns
# # Make sure temp_with_county['DATE'] and aqi_df_restricted['Date'] are datetime
# temp_with_county['DATE'] = pd.to_datetime(temp_with_county['DATE'])
# aqi_df_restricted['Date'] = pd.to_datetime(aqi_df_restricted['Date'])

# # ---------------------------
# # Step 4: Merge on Date and COUNTYFP
# merged_df = pd.merge(
#     temp_with_county,
#     aqi_df_restricted,
#     left_on=['DATE', 'COUNTYFP'],
#     right_on=['Date', 'COUNTYFP'],
#     how='inner'  # use 'left' if you want all temperature stations even if AQI missing
# )

# # ---------------------------
# # Step 5: (Optional) drop redundant columns
# merged_df = merged_df.drop(columns=['Date'])  # Keep only one date column if desired

# print("Merged DataFrame shape:", merged_df.shape)
# print("Columns:", merged_df.columns.tolist())

# import pandas as pd
# import geopandas as gpd
# import numpy as np

# # ---------------------------
# # Step 1: Make sure merged_df is ready
# # Columns needed: ['COUNTYFP', 'DATE', 'tavg_calc', 'AQI']
# # Dates as datetime
# merged_df['DATE'] = pd.to_datetime(merged_df['DATE'])

# # ---------------------------
# # Step 2: Define seasons
# def get_season(month):
#     if month in [12, 1, 2]:
#         return 'Winter'
#     elif month in [3, 4, 5]:
#         return 'Spring'
#     elif month in [6, 7, 8]:
#         return 'Summer'
#     else:
#         return 'Fall'

# merged_df['Season'] = merged_df['DATE'].dt.month.apply(get_season)

# # ---------------------------
# # Step 3: Calculate correlation by county with season fixed effects
# # We'll use groupby on COUNTYFP and Season to detrend by season
# county_list = merged_df['COUNTYFP'].unique()

# results = []

# for (county, season), group in merged_df.groupby(['COUNTYFP', 'Season']):
    
#     if len(group) > 1 and group['tavg_calc'].std() > 0 and group['AQI'].std() > 0:
#         corr = group['tavg_calc'].corr(group['AQI'])
#     else:
#         corr = np.nan
        
#     results.append({
#         'COUNTYFP': county,
#         'Season': season,
#         'temp_aqi_corr': corr
#     })

# corr_df = pd.DataFrame(results)


import pandas as pd
import numpy as np
import os
import geopandas as gpd
from shapely.geometry import Point
import streamlit as st

# ---------------------------------------------------
# Paths
# ---------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PATH = os.path.join(BASE_DIR, 'data', 'derived-data')
RAW_PATH = os.path.join(BASE_DIR, 'data', 'raw-data')

temp_path = os.path.join(PATH, 'all_weather.csv')
aqi_path = os.path.join(PATH, 'aqi_all.csv')
counties_path = os.path.join(RAW_PATH, 'tl_2025_us_county')


# ---------------------------------------------------
# Cache the entire data pipeline
# ---------------------------------------------------
@st.cache_data
def build_corr_df():

    # ---------------------------
    # Load datasets
    # ---------------------------
    temp_narrow = pd.read_csv(temp_path)
    aqi_df = pd.read_csv(aqi_path)

    # ---------------------------
    # Convert temp stations to GeoDataFrame
    # ---------------------------
    geometry = [Point(xy) for xy in zip(temp_narrow['LONGITUDE'], temp_narrow['LATITUDE'])]

    temp_gdf = gpd.GeoDataFrame(temp_narrow, geometry=geometry)
    temp_gdf.set_crs(epsg=4326, inplace=True)

    # ---------------------------
    # Load counties
    # ---------------------------
    counties = gpd.read_file(counties_path)
    counties = counties.to_crs(temp_gdf.crs)

    # ---------------------------
    # Spatial join
    # ---------------------------
    temp_with_county = gpd.sjoin(
        temp_gdf,
        counties[['COUNTYFP', 'geometry']],
        how="left",
        predicate="intersects"
    )

    # ---------------------------
    # Map county names to COUNTYFP
    # ---------------------------
    county_to_fp = {
        "Adams": "001",
        "Champaign": "019",
        "Clark": "023",
        "Cook": "031",
        "DuPage": "043",
        "Jo Daviess": "085",
        "Kane": "089",
        "McHenry": "111",
        "McLean": "113",
        "Macon": "115",
        "Mercer": "131",
        "Peoria": "143",
        "Randolph": "157",
        "Rock Island": "161",
        "Sangamon": "167",
        "Winnebago": "201",
    }

    # Clean county names
    aqi_df['county Name'] = aqi_df['county Name'].str.replace(" County$", "", regex=True)

    # Map to FIPS
    aqi_df['COUNTYFP'] = aqi_df['county Name'].map(county_to_fp)

    # ---------------------------
    # Restrict AQI counties
    # ---------------------------
    temp_counties = temp_with_county['COUNTYFP'].unique()

    aqi_df_restricted = aqi_df[
        aqi_df['COUNTYFP'].isin(temp_counties)
    ].copy()

    # ---------------------------
    # Standardize dates
    # ---------------------------
    temp_with_county['DATE'] = pd.to_datetime(temp_with_county['DATE'])
    aqi_df_restricted['Date'] = pd.to_datetime(aqi_df_restricted['Date'])

    # ---------------------------
    # Merge
    # ---------------------------
    merged_df = pd.merge(
        temp_with_county,
        aqi_df_restricted,
        left_on=['DATE', 'COUNTYFP'],
        right_on=['Date', 'COUNTYFP'],
        how='inner'
    )

    merged_df = merged_df.drop(columns=['Date'])

    # ---------------------------
    # Add seasons
    # ---------------------------
    def get_season(month):

        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Fall'

    merged_df['Season'] = merged_df['DATE'].dt.month.apply(get_season)

    # ---------------------------
    # Correlation calculation
    # ---------------------------
    results = []

    for (county, season), group in merged_df.groupby(['COUNTYFP', 'Season']):

        if (
            len(group) > 1
            and group['tavg_calc'].std() > 0
            and group['AQI'].std() > 0
        ):
            corr = group['tavg_calc'].corr(group['AQI'])
        else:
            corr = np.nan

        results.append({
            'COUNTYFP': county,
            'Season': season,
            'temp_aqi_corr': corr
        })

    corr_df = pd.DataFrame(results)

    return corr_df


# ---------------------------------------------------
# Build correlation dataframe (cached)
# ---------------------------------------------------
corr_df = build_corr_df()


# START OF STREAMLIT
import streamlit as st
import altair as alt
import json

# ---------------------------------------------------
# Cache counties so shapefile is only loaded once
# ---------------------------------------------------
@st.cache_data
def load_counties():
    counties_path = os.path.join(RAW_PATH, 'tl_2025_us_county')
    counties = gpd.read_file(counties_path)
    counties = counties[["COUNTYFP", "geometry"]]
    counties = counties.to_crs(epsg=4326)
    counties["COUNTYFP"] = counties["COUNTYFP"].astype(str).str.zfill(3)
    return counties

# ---------------------------------------------------
# Cache correlation dataframe prep
# ---------------------------------------------------
@st.cache_data
def prepare_corr_df(df):
    df = df.copy()
    df["COUNTYFP"] = df["COUNTYFP"].astype(str).str.zfill(3)
    df["temp_aqi_corr"] = pd.to_numeric(df["temp_aqi_corr"], errors="coerce")
    return df

df = prepare_corr_df(corr_df)
counties_wgs84 = load_counties()

# Global color scale
global_min = df["temp_aqi_corr"].min()
global_max = df["temp_aqi_corr"].max()

# ---------------------------------------------------
# Streamlit UI
# ---------------------------------------------------
st.title("Seasonality of AQI and Temperature Correlation")

seasons = sorted(df["Season"].unique())

col1, col2 = st.columns(2)

with col1:
    season_left = st.selectbox("Left Map Season", seasons, index=0)

with col2:
    season_right = st.selectbox(
        "Right Map Season", seasons, index=1 if len(seasons) > 1 else 0
    )

# ---------------------------------------------------
# Map builder
# ---------------------------------------------------
def build_map(season_choice):

    filtered = df[df["Season"] == season_choice]

    filtered_gdf = counties_wgs84.merge(filtered, on="COUNTYFP", how="left")

    no_data = filtered_gdf[filtered_gdf["temp_aqi_corr"].isna()]
    with_data = filtered_gdf[filtered_gdf["temp_aqi_corr"].notna()]

    no_data_json = json.loads(no_data.to_json())
    data_json = json.loads(with_data.to_json())

    background = alt.Chart(
        alt.Data(values=no_data_json["features"])
    ).mark_geoshape(
        fill="lightgray",
        stroke="white",
        strokeWidth=0.5
    )

    foreground = alt.Chart(
        alt.Data(values=data_json["features"])
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
            alt.Tooltip("properties.COUNTYFP:N", title="County FIPS"),
            alt.Tooltip("properties.temp_aqi_corr:Q", title="Correlation", format=".2f")
        ]
    )

    chart = (background + foreground).properties(
        height=550,
        title=season_choice
    ).project(
        type="mercator",
        fit=counties_wgs84
    )

    return chart

# ---------------------------------------------------
# Build maps
# ---------------------------------------------------
map_left = build_map(season_left)
map_right = build_map(season_right)

# ---------------------------------------------------
# Display maps
# ---------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.altair_chart(map_left, use_container_width=True)

with col2:
    st.altair_chart(map_right, use_container_width=True)