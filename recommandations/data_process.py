from tqdm import tqdm
import pandas as pd
import numpy as np
import sys

import json

columns_ = ['title', 'text', 'offering_id', 'num_helpful_votes', 'date', 'id', 'via_mobile', 'ratings.overall', 'author.username', 
           'author.id', 'author.location', 'date_stayed', 'ratings.service', 'ratings.cleanliness', 'ratings.value', 'ratings.location', 
           'ratings.sleep_quality', 'ratings.rooms', 'author.num_cities', 'author.num_helpful_votes', 'author.num_reviews', 
           'author.num_type_reviews', 'ratings.check_in_front_desk', 'ratings.business_service_(e_g_internet_access)']

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

def reviews_to_csv(origin, path, columns=None, lim=10000):
    i=0
    columns_reviews = []
    col_len = []
    with open(origin) as f:
        for line in tqdm(extract_lines(f)):
            df = transform_data(line, meta=['ratings', 'author'])
            df_reviews = pd.concat([pd.DataFrame(columns=columns_), df[2]])
 
            if i == 0:
                load_data(path, df=df_reviews, mode='w', header=True)
            elif i==lim:
                break
            else:
                load_data(path, df=df_reviews)
            i += 1

def main():
    print('Processuce en cours')
    if len(sys.argv) == 4:
        type, path_origin, path_output = sys.argv[1:]

        if type=='offering':
            print('Chargement de l\'offre vers ', path_output)
            offers_to_csv(path_origin, path_output)
        elif type=='reviews':
            print('Chargement des reviews vers ', path_output)
            reviews_to_csv(path_origin, path_output, lim=0)
        else:
            print('Veuillez spécifier le type de fichier (offering|reviews)')
    
    else:
        print("Veuillez spécifier les bons arguments: \n\
        > python data_process.py <type> <path/to/file/origin> <path/to/file/output> \n\
        Description: \n\
        <type> - (reviews|offering) : Le type de fichier à charger \n\
        <path/to/file/origin> : Chemin du fichier à transformer \n\
        <path/to/file/ouptut> : Destination du fichier")

if __name__ == '__main__':
    main()