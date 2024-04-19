from dagster import solid, AssetMaterialization
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, mapping
import io
import json
import os

@solid
def naptan_pull_solid(context):
    url = 'https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv'
    response = requests.get(url)

    if response.status_code == 200:
        with open('tmp/naptan_raw.csv', 'wb') as file:
            file.write(response.content)
        context.log.info("Successfully pulled data")

        string_object = io.StringIO(response.text)
        df = pd.read_csv(string_object)

        context.log.info("Columns in DataFrame:", df.columns)

        return df
    else:
        context.log.error(f"Data pull error. Status code: {response.status_code}")
        return None

@solid
def clean_column_names_solid(context, df):
    df.columns = [col.replace(' ', '_').lower() for col in df.columns]
    return df

@solid
def drop_lang_columns_solid(context, df):
    columns_to_drop = [col for col in df.columns if 'lang' in col.lower()]
    df = df.drop(columns=columns_to_drop)
    return df

@solid
def add_geojson_solid(context, df):
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
    gdf = gdf[gdf['geometry'].is_valid & ~gdf['geometry'].is_empty]

    df['geom'] = gdf['geometry'].apply(lambda x: json.dumps(mapping(x)) if x is not None else None)
    return df

@solid
def save_cleaned_data_solid(context, df):
    filename = 'tmp/cleaned_naptan.csv'
    df.to_csv(filename, index=False)
    context.log.info(f"Data saved to {filename}")
    yield AssetMaterialization(asset_key="cleaned_naptan_csv", description="Cleaned NAPTAN data saved as CSV", metadata={"filename": filename})

naptan_df = naptan_pull_solid()
cleaned_df = clean_column_names_solid(naptan_df)
dropped_df = drop_lang_columns_solid(cleaned_df)
geojson_df = add_geojson_solid(dropped_df)
save_cleaned_data_solid(geojson_df)
