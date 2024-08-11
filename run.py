import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from src.convertor import Convertor
from src.db_connection import PostgreSQLDB
from src.data_io_manager import PostgresDataHandler

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)    

def main():
    convertor = Convertor('data/yellow_tripdata_2022-01.parquet')
    convertor.convert_to_csv()

    df = pd.read_csv('data/yellow_tripdata_2022-01.csv', nrows=100)

    postgres_data_handler = PostgresDataHandler()
    postgres_data_handler.write(df, 'yellow_tripdata_2022_01')    




if __name__ == "__main__":
    main()