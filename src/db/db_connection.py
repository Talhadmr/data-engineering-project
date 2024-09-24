from pandas import DataFrame
from os import path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker as sessionMaker
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
from dotenv import load_dotenv
import os
from abc import ABC, abstractmethod
import json
import datetime
from typing import Dict

# Load the .env file
load_dotenv()

class BaseDBConnection(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def connect(self):
        pass

class PostgreSQLDB(BaseDBConnection):
    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST")
        self.port = os.getenv("POSTGRES_PORT")
        self.db = os.getenv("POSTGRES_DB")
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        print("db: ", self.db)
        print("user: ", self.user)
        print("password: ", self.password)
        print("host: ", self.host)
        print("port: ", self.port)
        print("PostgreSQLDB object created.")
        self.engine: Engine = None
        self.Session: Session = None

    def connect(self):
        try:
            self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
            self.Session = sessionMaker(bind=self.engine)
            print("Successfully connected to: ", self.engine.url)
        except Exception as e:
            print("Connection failed: ", e)
        return self.engine, self.Session

    def disconnect(self):
        try:
            if self.engine:
                self.engine.dispose()
                print("Disconnected from: ", self.engine.url)
            else:
                print("Already disconnected.")    
        except Exception as e:
            print("Disconnection failed: ", e)

    def execute_query(self, query):
        try: 
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                print("Successfully executed.")
                return result
        except Exception as e:
            print("Failed: ", e)
            return None

class BaseDataHandler(ABC):
    @abstractmethod
    def read(self) -> pd.DataFrame:
        """Abstract method to read data from a source."""
        pass

    @abstractmethod
    def write(self, data: pd.DataFrame) -> None:
        """Abstract method to write data to a destination."""
        pass

class PostgresDataHandler(BaseDataHandler):
    def __init__(self):
        self.postgres = PostgreSQLDB()
        self.engine, self.session = self.postgres.connect()

    def _get_postgres_type(self, series: pd.Series) -> str:
        type_mapping = {
            'object': 'TEXT',
            'int64': 'BIGINT',
            'int32': 'INT',
            'float64': 'DOUBLE PRECISION',
            'float32': 'FLOAT',
            'datetime64[ns]': 'TIMESTAMP'
        }

        series_dtype = str(series.dtype)
        return type_mapping.get(series_dtype, 'TEXT')

    def _create_table(self, table_name: str, columns_info: str) -> None:
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_info})"
        try:
            self.postgres.execute_query(create_table_query)
            print(f"Table created successfully: {table_name}")
        except Exception as e:
            raise RuntimeError(f"Failed to create table {table_name}: {e}")

    def write(self, data: pd.DataFrame, table_name: str) -> None:
        try:
            column_types = {column: self._get_postgres_type(data[column]) for column in data.columns}
            columns_info = ', '.join([f"{col} {col_type}" for col, col_type in column_types.items()])
            self._create_table(table_name, columns_info)
            data.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            print(f"Successfully written DataFrame to PostgreSQL table: {table_name}")
        except Exception as e:
            raise RuntimeError(f"Failed to write DataFrame to PostgreSQL table {table_name}: {e}")
        finally:
            self.postgres.disconnect()

    def read(self, table_name: str) -> pd.DataFrame:
        try:
            query = f"SELECT * FROM {table_name}"
            result = self.postgres.execute_query(query)
            if result is None:
                raise RuntimeError(f"Query failed or table {table_name} does not exist.")
            data = result.fetchall()
            columns = result.keys()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            raise RuntimeError(f"Failed to read data from PostgreSQL table {table_name}: {e}")