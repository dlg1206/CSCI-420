import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pandas import DataFrame, Series
import time
from database import Database

PATH_TO_ENV = ".env"
NUM_REVIEWS = 6739590

class Outlier:
    def __init__(self, q1: int, q3: int):
        self.q1 = q1
        self.q3 = q3
        self.iqr = q3 - q1

    def is_outlier(self, value: int) -> bool:
        return value < (self.q1 - 1.5 * self.iqr) or value > (self.q3 + 1.5 * self.iqr)

def find_outliers_IQR(df) -> DataFrame:
    """
    https://careerfoundry.com/en/blog/data-analytics/how-to-find-outliers/
    :param df:
    :return:
    """
    q1 = df.quantile(0.25)

    q3 = df.quantile(0.75)

    IQR = q3 - q1
    print(f"q1: {q1}")
    print(f"q3: {q3}")
    print(f"IQR: {IQR}")

    outliers = df[((df < (q1 - 1.5 * IQR)) | (df > (q3 + 1.5 * IQR)))]

    return outliers


def print_outlier_stats(df: Series | DataFrame) -> None:
    outliers = find_outliers_IQR(df)
    print(f"number of outliers: {len(outliers)}")
    print(f"% outliers: {round(100 * (len(outliers) / len(df)), 2)}")
    print(f"max outlier value: {outliers.max()}")
    print(f"min outlier value: {outliers.min()}")


if __name__ == '__main__':
    load_dotenv(dotenv_path=Path(PATH_TO_ENV))
    db = Database(
        database=os.getenv("database"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port")
    )

    # votes = db.get_column('amz_reviews', 'vote')
    # print_outlier_stats(votes)

    # WELL OPTIMIZED CODE THAT WILL DEFINITELY NOT EAT YOUR CPU AND RAM
    # ( but run at your own risk )
    query = "SELECT reviewtext FROM amz_reviews"
    word_count = []
    b = 0
    with db.connection.cursor(name='csr') as cursor:
        start = time.perf_counter()
        cursor.itersize = 500000
        cursor.execute(query)
        print(f"Batch {b} / {round(NUM_REVIEWS / 500000), 2}")
        b += 1
        for row in cursor:
            if row[0] is None:
                word_count.append(0)
            else:
                word_count.append(len(row[1].split()))

        print(f"Query done in {time.perf_counter() - start:.2f}s")

    print_outlier_stats(pd.DataFrame(data=word_count, columns=['word count']))
