import json
import pandas as pd
from pandas.io.json import json_normalize #package for flattening json in pandas df

#load json object
with open('raw_nyc_phil.json') as f:
    d = json.load(f)

#lets put the data into a pandas df
#clicking on raw_nyc_phil.json under "Input Files"
#tells us parent node is 'programs'
nycphil = json_normalize(d['programs'])
print(nycphil.head(3))