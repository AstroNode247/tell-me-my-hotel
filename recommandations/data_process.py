from tqdm import tqdm
import pandas as pd
import numpy as np

import json

def extract_lines(file):
    while True:
        line = file.readline()
        if not line:
            break
        yield line
        
def transform_data(data, meta):
    json_data = json.loads(data)
    df = pd.json_normalize(json_data, meta=meta).replace('', np.nan)
    
    results = []
    for index, row in df.iterrows():
        results.append(row)
    
    return results, list(df.columns), df


def load_data(path, df, mode='a', header=False, columns=None):
    if not df.empty:
        df.to_csv(path,  mode=mode, header=header, columns=columns)

        
def offers_to_csv(origin, path):
    i = 0
    with open(origin) as f:
        for line in tqdm(extract_lines(f)):
            df_offers = transform_data(line, meta=['address'])[2]
            if i==0:
                load_data(path=path, df=df_offers, mode='w', header=True)  
            else:
                load_data(path=path, df=df_offers)
            i += 1

def reviews_to_csv(origin, path):
    i=0
    columns_reviews = []
    with open(origin) as f:
        for line in tqdm(extract_lines(f)):
            df_reviews = transform_data(line, meta=['ratings', 'author'])

            if len(df_reviews[1]) > len(columns_reviews):
                columns_reviews = df_reviews[1]

            load_data(path, df=df_reviews[2])
            i += 1
            
    df_reviews = pd.read_csv(path, names=columns_reviews)
    df_reviews.to_csv(path)
    df_full_reviews = pd.read_csv(path)
    
    return df_full_reviews