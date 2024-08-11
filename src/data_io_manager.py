import pandas as pd
from abc import ABC, abstractmethod
from src.db_connection import PostgreSQLDB
import json
import os
import datetime
from sqlalchemy import text
from typing import Dict

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
            self.postgres.execute_query(text(create_table_query))
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
            data = result.fetchall()
            columns = result.keys()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            raise RuntimeError(f"Failed to read data from PostgreSQL table {table_name}: {e}")