import pandas as pd
import numpy as np
import os as os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(BASE_DIR, 'data', 'raw-data')

csvs = ['US1ILAD0005.csv', 'USC00110072.csv', 'USC00110338.csv', 'USC00111329.csv',
        'USC00111577.csv', 'USC00114489.csv', 'USC00114629.csv', 'USC00115097.csv',
        'USC00115493.csv', 'USC00116200.csv', 'USC00117077.csv', 'USC00117391.csv',
        'USC00118293.csv', 'USC00118740.csv', 'USW00003887.csv', 'USW00014842.csv', 
        'USW00093822.csv', 'USW00094822.csv', 'USW00094870.csv']

all_dfs = []

for file in csvs:
    full_path = os.path.join(PATH, file)
    df = pd.read_csv(full_path)
    
    raw_name = df.loc[0, 'NAME']
    clean_name = raw_name.lower()
    clean_name = clean_name.replace(" ", "_")
    clean_name = re.sub(r'[^a-z0-9_]', '', clean_name)
    df['station_name'] = clean_name
    df['station_id'] = file.replace('.csv','')
    
    all_dfs.append(df)

master_df = pd.concat(all_dfs, ignore_index=True)
master_df = master_df.dropna(subset=['TMAX', 'TMIN'])

print(master_df.head())
print(f"Total rows: {len(master_df)}")


# Get inventory file to find COOP stations in IL
inv_path = os.path.join(PATH, 'ghcnd-inventory.txt')
inv = pd.read_fwf(inv_path, 
                    colspecs=[(0,11),(12,20),(21,30),(31,35),(36,40),(41,45)],
                    names=["station_id","lat","lon","element","first_year","last_year"])

# Filter for COOP stations (IDs starting with 'USC')
coop_stations = inv[inv['station_id'].str.startswith('USC')]

# Filter for Illinois by lat/lon
coop_il = coop_stations[
    (coop_stations['lat'] >= 36.98) &
    (coop_stations['lat'] <= 42.50) &
    (coop_stations['lon'] >= -91.52) &
    (coop_stations['lon'] <= -87.51)
]

# Keep only rows with TMAX or TMIN
coop_il_temp = coop_il[coop_il['element'].isin(['TMAX','TMIN'])]

# Only stations that have BOTH TMAX and TMIN
stations_with_both = (
    coop_il_temp.groupby('station_id')['element']
    .nunique()
    .loc[lambda x: x == 2]
    .index
)

# Now filter for stations with data covering 2018–2025
coop_il_recent = coop_il_temp[
    coop_il_temp['station_id'].isin(stations_with_both) &
    (coop_il_temp['first_year'] <= 2018) &
    (coop_il_temp['last_year'] >= 2025)
]

# Get unique station IDs
stations_recent_ids = coop_il_recent['station_id'].unique()

print("Illinois COOP stations with TMAX & TMIN covering 2018–2025:")
print(stations_recent_ids)


import geopandas as gpd
from shapely.geometry import Point

target_counties = [
    'Adams', 'Champaign', 'Clark', 'Cook', 'DuPage',
    'Effingham', 'Hamilton', 'Jersey', 'Jo Daviess', 'Kane',
    'Lake', 'Macon', 'Macoupin', 'Madison', 'McHenry',
    'McLean', 'Peoria', 'Randolph', 'Rock Island', 'Saint Clair',
    'Sangamon', 'Will', 'Winnebago'
]

counties_path = os.path.join(PATH, 'tl_2025_us_county')
counties = gpd.read_file(counties_path)
counties = counties[counties['NAME'].isin(target_counties)]

# Load inventory and filter for COOP + coverage
inv_path = os.path.join(PATH, 'ghcnd-inventory.txt')
inv = pd.read_fwf(
    inv_path,
    colspecs=[(0,11),(12,20),(21,30),(31,35),(36,40),(41,45)],
    names=["station_id","lat","lon","element","first_year","last_year"]
)

coop = inv[inv.station_id.str.startswith("USC")]
coop_il = coop[(coop.lat>=36.98)&(coop.lat<=42.50)&(coop.lon>=-91.52)&(coop.lon<=-87.51)]
coop_temp = coop_il[coop_il.element.isin(['TMAX','TMIN'])]
stations_both = coop_temp.groupby('station_id')['element'].nunique()
stations_both = stations_both[stations_both == 2].index

valid = coop_temp[
    (coop_temp.station_id.isin(stations_both)) &
    (coop_temp.first_year <= 2018) &
    (coop_temp.last_year >= 2025)
].drop_duplicates('station_id')

stations_gdf = gpd.GeoDataFrame(
    valid[['station_id','lat','lon','first_year','last_year']],
    geometry=gpd.points_from_xy(valid.lon, valid.lat),
    crs="EPSG:4326"
)


selected = []

for _, county in counties.iterrows():
    # stations within this county
    inside = stations_gdf[stations_gdf.within(county.geometry)]
    
    if len(inside) == 0:
        # If none, you could find nearest nearby later
        continue
    
    # pick one inside county
    # e.g., most complete coverage → longest span
    inside['span'] = inside['last_year'] - inside['first_year']
    best = inside.sort_values('span', ascending=False).iloc[0]
    
    selected.append({
        'county': county['NAME'],
        'station_id': best['station_id'],
        'lat': best['lat'],
        'lon': best['lon'],
        'first_year': best['first_year'],
        'last_year': best['last_year']
    })

final_df = pd.DataFrame(selected)
print(final_df)

def c_to_f_inplace(df, col_name):
    """
    Convert a dataframe column from Celsius to Fahrenheit in place.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe containing the column
    col_name : str
        Name of the column to convert

    Returns
    -------
    pandas.DataFrame
        The same dataframe with the column converted
    """
    
    df[col_name] = (df[col_name]/10) * 9/5 + 32
    return df

master_df = c_to_f_inplace(master_df, 'TMIN')
master_df = c_to_f_inplace(master_df, 'TMAX')
master_df['tavg_calc'] = (master_df['TMIN'] + master_df['TMAX'])/2

col_keep = ['STATION', 'DATE', 'LATITUDE', 'LONGITUDE', 'NAME', 'TMAX', 'TMIN', 'tavg_calc']

master_df = master_df[col_keep]

master_df = master_df[master_df['DATE'] >= '2015-01-01']

output_path = os.path.join('data', 'derived-data', 'all_weather2.csv')
#master_df.to_csv(output_path, index=False)


import pandas as pd
import numpy as np
import os as os
import geopandas as gpd
from shapely.geometry import Point

os.chdir(os.path.dirname(os.path.abspath(__file__)))
PATH = os.path.join('data', 'derived-data')
RAW_PATH = os.path.join('data', 'raw-data')
temp_path = os.path.join(PATH, 'all_weather.csv')
aqi_path = os.path.join(PATH, 'aqi_all.csv')
temp_narrow = pd.read_csv(temp_path)
aqi_df = pd.read_csv(aqi_path)

# Convert lat/lon to geometry points
geometry = [Point(xy) for xy in zip(temp_narrow['LONGITUDE'], temp_narrow['LATITUDE'])]
temp_gdf = gpd.GeoDataFrame(temp_narrow, geometry=geometry)

# Make sure you set a CRS (coordinate reference system)
# Most lat/lon data is EPSG:4326
temp_gdf.set_crs(epsg=4326, inplace=True)

# Replace with the path to your county shapefile
counties_path = os.path.join(RAW_PATH, 'tl_2025_us_county')
counties = gpd.read_file(counties_path)

# Make sure the counties GeoDataFrame is in the same CRS
counties = counties.to_crs(temp_gdf.crs)

# Perform spatial join
temp_with_county = gpd.sjoin(
    temp_gdf,
    counties[['COUNTYFP', 'geometry']],  # keep only county_fp and geometry for spatial join
    how="left",
    predicate="intersects"
)

# ---------------------------
# Step 1: Map county names to COUNTYFP for Illinois
# Only include the counties present in temp_with_county
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

# Clean 'county Name' to match mapping (strip ' County' if present)
aqi_df['county Name'] = aqi_df['county Name'].str.replace(" County$", "", regex=True)

# Create COUNTYFP column in AQI df
aqi_df['COUNTYFP'] = aqi_df['county Name'].map(county_to_fp)

# Optional: check for unmatched counties
missing = aqi_df[aqi_df['COUNTYFP'].isna()]['county Name'].unique()
if len(missing) > 0:
    print("Warning: These AQI counties did not map to COUNTYFP:", missing)

# ---------------------------
# Step 2: Restrict AQI df to counties present in temperature data
temp_counties = temp_with_county['COUNTYFP'].unique()
aqi_df_restricted = aqi_df[aqi_df['COUNTYFP'].isin(temp_counties)].copy()

# ---------------------------
# Step 3: Standardize date columns
# Make sure temp_with_county['DATE'] and aqi_df_restricted['Date'] are datetime
temp_with_county['DATE'] = pd.to_datetime(temp_with_county['DATE'])
aqi_df_restricted['Date'] = pd.to_datetime(aqi_df_restricted['Date'])

# ---------------------------
# Step 4: Merge on Date and COUNTYFP
merged_df = pd.merge(
    temp_with_county,
    aqi_df_restricted,
    left_on=['DATE', 'COUNTYFP'],
    right_on=['Date', 'COUNTYFP'],
    how='inner'  # use 'left' if you want all temperature stations even if AQI missing
)

# ---------------------------
# Step 5: (Optional) drop redundant columns
merged_df = merged_df.drop(columns=['Date'])  # Keep only one date column if desired

print("Merged DataFrame shape:", merged_df.shape)
print("Columns:", merged_df.columns.tolist())

import pandas as pd
import geopandas as gpd
import numpy as np

# ---------------------------
# Step 1: Make sure merged_df is ready
# Columns needed: ['COUNTYFP', 'DATE', 'tavg_calc', 'AQI']
# Dates as datetime
merged_df['DATE'] = pd.to_datetime(merged_df['DATE'])

# ---------------------------
# Step 2: Define seasons
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
# Step 3: Calculate correlation by county with season fixed effects
# We'll use groupby on COUNTYFP and Season to detrend by season
county_list = merged_df['COUNTYFP'].unique()

results = []

for (county, season), group in merged_df.groupby(['COUNTYFP', 'Season']):
    
    if len(group) > 1 and group['tavg_calc'].std() > 0 and group['AQI'].std() > 0:
        corr = group['tavg_calc'].corr(group['AQI'])
    else:
        corr = np.nan
        
    results.append({
        'COUNTYFP': county,
        'Season': season,
        'temp_aqi_corr': corr
    })

corr_df = pd.DataFrame(results)

corr_df

output_path2 = os.path.join('data', 'derived-data', 'streamlit_data.csv')
corr_df.to_csv(output_path2, index=False)