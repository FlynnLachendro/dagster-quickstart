#importing the relevant packages
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry import mapping
import io
import json
import os

#Creating a folder in the main working directory called tmp if it doesn't already exist
if not os.path.exists('tmp'):
    os.makedirs('tmp')

def naptan_pull_process():
    url = 'https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv' # defining the url for the GET request from the NAPTAN webpage
    response = requests.get(url) # the response object is created

    if response.status_code == 200: # if the status code variable of the response object is all good (200 = all good for HTLM get requests) then do the following
        with open('tmp/naptan_raw.csv', 'wb') as file: #with a new file called naptan_raw in the tmp folder, write binary (just the plain bytes 1s and 0s, don't write directly as a csv)
            file.write(response.content) #write the information taken from the GET request to the naptan_raw file
        print("Successfully pulled data") #print the success message
        
        string_object = io.StringIO(response.text) #change the text variable in the response object to the file / object type so pd.read_csv can recognise it properly as an input
        df = pd.read_csv(string_object) #read in the csv created through the GET request and StringIO data type change
        print("Columns in DataFrame:", df.columns)  #show you all column names in the dataframe
        return df #return the dataframe so it is declared as a variable for use in later functions
    else:
        print(f"Data pull error. Status code: {response.status_code}") #otherwise, if something's broken, print the error message and corresponding status code
        return None #return nothing to ensure the df object doesn't exist if it isn't perfectly created in the first place


df = naptan_pull_process() #create the df object by declaring it here for use later on

def clean_column_names(df): #define the column names cleaning function = bigquery likes lower case without spaces
    df.columns = [col.replace(' ', '_').lower() for col in df.columns] #the column headers in the current dataframe now have their spaces, dashes and upper case letters removed
    return df #return the new, cleaned df object

def drop_lang_columns(df): #similar to the above function, but just dropping the columns that specify the language of different categories - I thought they were a bit unnecessary
    # Filter column names that contain 'lang'
    columns_to_drop = [col for col in df.columns if 'lang' in col.lower()]
    
    # Drop these columns from the dataframe
    df = df.drop(columns=columns_to_drop)
    
    return df #return the new, cleaned df object like before

df = drop_lang_columns(df) #re-declare the df object

if df is not None: #if the df is an object with content inside (i.e. if the GET request and df creation have run properly)
    df = clean_column_names(df) #run the clean column names function on the dataframe with the lang columns dropped from it
    print(df) #print the new dataframe to check
else:
    print("No data to process") #if the dataframe hasnt been created properly, print this error message to remind the user to go back and check the GET request / dataframe creation worked

def add_geojson(df): #define the geom column adding function
    # Ensure the columns are float type for GeoDataFrame compatibility
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    
    # Create GeoDataFrame with valid point check
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
    
    # Remove invalid geometries if any
    gdf = gdf[gdf['geometry'].is_valid & ~gdf['geometry'].is_empty]

    # Convert each geometry to GeoJSON format and add to the dataframe as a new column
    df['geom'] = gdf['geometry'].apply(lambda x: json.dumps(mapping(x)) if x is not None else None)
    return df

if df is not None: #similar to above, only do this if the dataframe has been processed correctly to this point
    df = add_geojson(df) #update the df object with the geom column from the latitude / longitude data
    print(df) #print it to check the geom column is there
    print(df.columns) #same thing but check the geom column header is here at the end of the list
else:
    print("Failed to process data - dataframe is empty") #print error message if the dataframe hasn't been created properly

def save_cleaned_data(df, filename='tmp/cleaned_naptan.csv'): #define the function to save the processed data to a csv with the name cleaned_naptan
    df.to_csv(filename, index=False) #use this filename as an argument for the pandas function .to_csv, don't include an index column as the first column
    print(f"Data saved to {filename}") #print the success message and where the csv was saved to

if df is not None: #similar to above, if everything has worked properly, then save the processed dataframe using the above function
    save_cleaned_data(df)
