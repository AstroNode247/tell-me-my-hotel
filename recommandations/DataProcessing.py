import json
import numpy as np
import pandas as pd

from tqdm import tqdm


class RecsData:
    data = []
    
    def __init__(self, path):
        self.data = []
        print('Initialize the dataset')
        with open(path) as f:
            for line in tqdm(f):
                self.data.append(line.strip())
    
    def to_json(self, prc=1):
        json_data = []
        
        rand_idx = np.random.randint(len(self.data), size=int(len(self.data) * prc))
        for i in tqdm(rand_idx):
            json_data.append(json.loads(self.data[i]))
        
        return json_data

    def to_dataframe(self, prc=1):
        print('Parse into json')
        json_data = self.to_json(prc=prc)

        keys = []
        for key in json_data[0]:
            keys.append(key)

        data_dict = {}
        for key in keys:
            data_dict[key] = []

        print('Read to dataframe')
        for data in tqdm(json_data):
            for key in data_dict:
                try:
                    if data[key] == "" or data[key] is None:
                        data_dict[key].append(np.nan)
                    else:
                        data_dict[key].append(data[key])
                except KeyError:
                    data_dict[key].append(np.nan)

        return pd.DataFrame(data_dict)

    