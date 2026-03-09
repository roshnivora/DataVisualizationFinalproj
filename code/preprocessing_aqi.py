import pandas as pd
import altair as alt
import time
import warnings
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = os.path.join(BASE_DIR, '..', 'data', 'raw-data')

years = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
aqi_data = {}

for year in years:
    path = os.path.join(RAW_DATA_PATH, f'daily_aqi_by_county_{year}.csv')
    df = pd.read_csv(path)
    df = df[df['State Name'] == 'Illinois']
    df['Date'] = pd.to_datetime(df['Date'])
    aqi_data[year] = df
    print(f"Loaded and processed: {year}, {len(df)} rows")

aqi_all = pd.concat(aqi_data.values(), ignore_index=True)
output_path = os.path.join(BASE_DIR, '..', 'data', 'derived-data', 'aqi_all.csv')
aqi_all.to_csv(output_path, index=False)
