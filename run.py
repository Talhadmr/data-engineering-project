import pandas as pd
import os
from src.convertor import Convertor
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)    

def main():
    convertor = Convertor('data/yellow_tripdata_2022-01.parquet')
    convertor.convert_to_csv()
    df = pd.read_csv('data/yellow_tripdata_2022-01.csv', nrows=100)
    print(df.head())

if __name__ == "__main__":
    main()