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

    def to_dataframe(self):
        data_list = []
        print('Parse into dictionnary')
        for item in tqdm(self.data):
            data_list.append(json.loads(item))

        keys = []
        for key in data_list[0]:
            keys.append(key)

        data_dict = {}
        for key in keys:
            data_dict[key] = []

        print('Clean the dictionnary')
        for data in tqdm(data_list):
            for key in data_dict:
                try:
                    if data[key] == "" or data[key] is None:
                        data_dict[key].append(np.nan)
                    else:
                        data_dict[key].append(data[key])
                except KeyError:
                    data_dict[key].append(np.nan)

        return pd.DataFrame(data_dict)
