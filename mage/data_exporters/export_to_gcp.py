from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import os
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def is_file_already_exist(config_path, config_profile, bucket_name, object_key):
    # Initialize Google Cloud Storage client
    storage_client = GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile))
    
    # Check if the file already exists in the bucket
    bucket = storage_client.client.bucket(bucket_name)
    blob = bucket.blob(object_key)
    
    if blob.exists():
        raise FileExistsError(f"File '{object_key}' already exists in the bucket '{bucket_name}'.")
    return storage_client


@data_exporter
def export_data_to_google_cloud_storage(*args, **kwargs) -> None:
    file_path = 'magic-zoomcamp/data/mt_cars.parquet'
    file_extension = os.path.splitext(file_path)[1].lower()
    
    # Load the data from the file
    if file_extension == '.parquet':
        df = pd.read_parquet(file_path)
    elif file_extension in ['.xls', '.xlsx']:
        df = pd.read_excel(file_path)
    elif file_extension == '.json':
        df = pd.read_json(file_path)
    elif file_extension == '.h5':
        df = pd.read_hdf(file_path)
    elif file_extension == '.feather':
        df = pd.read_feather(file_path)
    elif file_extension == '.pkl':
        df = pd.read_pickle(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'de-project12'
    object_key = 'car_data' + file_extension

    storage_client = is_file_already_exist(config_path, config_profile, bucket_name, object_key)

    # Export the DataFrame to Google Cloud Storage
    storage_client.export(
        df,
        bucket_name,
        object_key,
    )
