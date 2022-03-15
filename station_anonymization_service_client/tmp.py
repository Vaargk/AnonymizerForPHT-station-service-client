import os.path

import pandas as pd

if __name__ == '__main__':
    df = pd.read_csv(os.path.abspath('healthcare-dataset-stroke-data_part1.csv'))
    df.set_index(df.columns[0], inplace = True)
    df.index += 10000
    print(df.to_csv())
    df.to_csv('1.csv')