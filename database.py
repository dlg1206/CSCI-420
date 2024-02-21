"""
File: database.py
Description: database model to be used for data analyse

@author Derek Garcia
"""
import psycopg2
import pandas as pd
from pandas import DataFrame, Series


class Database:

    def __init__(self, database, user, password, host, port):
        try:
            self.connection = psycopg2.connect(
                database=database,
                user=user,
                password=password,
                host=host,
                port=port
            )
        except Exception as e:
            print("Failed to establish connection with database!")
            exit(1)

    def get_column(self, table: str, column: str) -> Series:
        """
        Get column from Database and convert it into a series

        :param table: Name of table to query
        :param column: Name of column to get
        :return: Series
        """

        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT {column} FROM {table}")
            records = cursor.fetchall()
            return pd.Series(map(lambda x: x[0], records))

    def get_columns(self, table: str, columns: list[str]) -> DataFrame:
        """
        Get serval columns from the Database and convert it into a datafram

        :param table: Name of table to query
        :param columns: Name of columns to get
        :return: Dataframe
        """
        with self.connection.cursor() as cursor:
            columns_str = (str(columns)
                           .replace("[", "")
                           .replace("]", "")
                           .replace("'", "\""))  # for postgres column

            cursor.execute(f"SELECT {columns_str} FROM {table}")
            return pd.DataFrame(cursor.fetchall(), columns=columns)
