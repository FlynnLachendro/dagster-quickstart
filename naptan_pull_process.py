import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry import mapping
import io
import json
import os

if not os.path.exists('tmp'):
    os.makedirs('tmp')

def naptan_pull_process():
    url = 'https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv'
    response = requests.get(url)

    if response.status_code == 200:
        with open('tmp/naptan_raw.csv', 'wb') as file:
            file.write(response.content)
        print("Successfully pulled data")
        
        string_object = io.StringIO(response.text)
        df = pd.read_csv(string_object)
        print("Columns in DataFrame:", df.columns)  # This line will show you all column names
        return df
    else:
        print(f"Data pull error. Status code: {response.status_code}")
        return None


df = naptan_pull_process()

def clean_column_names(df):
    df.columns = [col.replace(' ', '_').lower() for col in df.columns]
    return df

def drop_lang_columns(df):
    # Filter column names that contain 'lang'
    columns_to_drop = [col for col in df.columns if 'lang' in col.lower()]
    
    # Drop these columns from the DataFrame
    df = df.drop(columns=columns_to_drop)
    
    return df

df = drop_lang_columns(df)

if df is not None:
    df = clean_column_names(df)
    print(df)
else:
    print("No data to process")

def add_geojson(df):
    # Ensure the columns are float type for GeoDataFrame compatibility
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    
    # Create GeoDataFrame with valid point check
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
    
    # Remove invalid geometries if any
    gdf = gdf[gdf['geometry'].is_valid & ~gdf['geometry'].is_empty]

    # Convert each geometry to GeoJSON format and add as new column
    df['geom'] = gdf['geometry'].apply(lambda x: json.dumps(mapping(x)) if x is not None else None)
    return df

if df is not None:
    df = add_geojson(df)
    print(df)
    print(df.columns)
else:
    print("Failed to process data - dataframe is empty")

print(df)
print(df.columns)

def save_cleaned_data(df, filename='tmp/cleaned_naptan.csv'):
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

if df is not None:
    save_cleaned_data(df)
