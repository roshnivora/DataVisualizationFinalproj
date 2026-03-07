import pandas as pd
import numpy as np
import os as os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(BASE_DIR, '..', 'data', 'raw-data')

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

output_path = os.path.join('..', 'data', 'derived-data', 'all_weather.csv')
master_df.to_csv(output_path, index=False)
