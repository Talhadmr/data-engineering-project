

if __name__ == '__main__':
    data = pd.read_parquet('data/yellow_tripdata_2021-01.parquet')


    postgres_handler = PostgresDataHandler()
    postgres_handler.write(data, 'ny_taxi')

    data_from_postgres = postgres_handler.read('ny_taxi')
    print(data_from_postgres.head())
    
