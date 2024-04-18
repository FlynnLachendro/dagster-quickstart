import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
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
        print(df.columns)
    else:
        print("Data pull error. Status code {response.status_code}")
        

naptan_pull_process()
