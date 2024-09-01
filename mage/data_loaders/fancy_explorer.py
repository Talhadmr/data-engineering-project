import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    path_of_csv = 'magic-zoomcamp/data/yellow_tripdata_2021-01.csv'
    path_of_par = 'magic-zoomcamp/data/yellow_tripdata_2021-01.parquet'

    df = pd.read_csv(path_of_csv, nrows = 100)
    df_par = pd.read_parquet(path_of_par , engine='pyarrow') 

    return (df_par)