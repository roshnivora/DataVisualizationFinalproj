# DataVisualizationFinalproj
Research Question: What can the relationship between temperature, AQI, and respiratory virus concentration in wastewater tell us about the risks urban communities face due to climate change?

Before running the code, it is important to download tl_2025_17_tract.zip and tl_2025_us_county.zip and save them in the raw-data folder. They can be downloaded via the following link: https://drive.google.com/drive/folders/1rqdVfdIgfpkBsoRhQFl1YsEAmzuPIc9M?usp=sharing 

The link to the streamlit is: https://datavisualizationfinalproj-bjm6ubjx67yzyvdxxvtkjd.streamlit.app/ 

The app must be "woken up" within 24 hours of using it. 

data/
  raw-data/                            # Raw, unmodified downloaded files
    daily_aqi_by_county_2018.csv       # EPA AQI data (one file per year 2018-2025)
    daily_aqi_by_county_2019.csv
    ... (through 2025)
    USC00110072.csv                    # NOAA weather station files
    ... (other station CSVs)
    ghcnd-inventory.txt                # GHCN station inventory
    tl_2025_us_county/                 # Census county shapefile folder
  derived-data/                        # Processed output files
    aqi_all.csv                        # Combined AQI data for Illinois
    all_weather.csv                    # Merged and cleaned weather data
    temperature.csv                    # Temperature data for static viz
    streamlit_data.csv                 # Correlation data for Streamlit app
    virus.csv                          # Wastewater virus data (static viz only)
code/
  preprocessing_aqi.py                 # Processes EPA AQI data
  preprocessing.py                     # Processes NOAA weather + merges with AQI
streamlit_app_folder/
  streamlit_app3.py                    # Streamlit app
  IL_County_Boundaries/                # Spatial file with Illinois county boundaries
  streamlit_data.csv                   # Data for Streamlit app
  requirements.txt                     # Streamlit dependencies
writeup_final.qmd                      # Final writeup
requirements.txt                       # Python dependencies

