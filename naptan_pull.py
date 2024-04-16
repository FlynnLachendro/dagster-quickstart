import requests as rq
import pandas as pd

url = 'https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv'
response = requests.get(url)