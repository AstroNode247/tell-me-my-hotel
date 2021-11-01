import json
import numpy as np
import pandas as pd

from tqdm import tqdm


def to_json(data, prc):
    data_list = []
    for i in tqdm(range(0, int(len(data) * prc))):
        data_list.append(json.loads(data[i]))

    return data_list


class RecsData:
    data = []

    def __init__(self, path):
        self.data = []
        print('Initialize the dataset')
        with open(path) as f:
            for line in tqdm(f):
                self.data.append(line.strip())

    def to_dataframe(self, prc=1):
        print('Parse into json')
        data_list = to_json(self.data, prc=prc)

        keys = []
        for key in data_list[0]:
            keys.append(key)

        data_dict = {}
        for key in keys:
            data_dict[key] = []

        print('Read to dataframe')
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
