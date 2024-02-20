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

    def get_column(self, column: str, table: str) -> Series:
        """
        Get column from Database and convert it into a series

        :param column: Name of column to get
        :param table: Name of table to query
        :return:
        """

        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT {column} FROM {table}")
            records = cursor.fetchall()
            return pd.Series(map(lambda x: x[0], records))