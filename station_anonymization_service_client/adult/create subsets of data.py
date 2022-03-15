import os.path

import pandas as pd


def divide_table(number, table_path):
    df = pd.read_csv(os.path.abspath(table_path))
    del df['fnlwgt']
    del df['capital-gain']
    del df['capital-loss']
    del df['education']
    print(df.head().to_string())
    df.reset_index(inplace=True)
    print(df.head().to_string())
    df = df.loc[df['gender'] == 'Female', :]
    df = df.loc[df['age'] >= 50, :]
    length_of_table = len(df)
    length_of_table = int(length_of_table / number)
    start_index = 0
    for i in range(1, number + 1):
        if i < number:
            part_df = df.iloc[range(start_index, start_index + length_of_table), :]
        else:
            part_df = df.iloc[range(start_index, len(df)), :]
        part_df = part_df.loc[:, part_df.columns != part_df.columns[0]]
        part_df.to_csv(os.path.abspath(str(i) + '.csv'), index=False)
        start_index += length_of_table + 1


if __name__ == '__main__':
    divide_table(6, 'adult.csv')
