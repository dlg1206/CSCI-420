import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from pandas import DataFrame

from database import Database

PATH_TO_ENV = ".env"


def find_outliers_IQR(df) -> DataFrame:
    """
    https://careerfoundry.com/en/blog/data-analytics/how-to-find-outliers/
    :param df:
    :return:
    """
    q1 = df.quantile(0.25)

    q3 = df.quantile(0.75)

    IQR = q3 - q1

    outliers = df[((df < (q1 - 1.5 * IQR)) | (df > (q3 + 1.5 * IQR)))]

    return outliers


def print_outlier_stats(df: DataFrame) -> None:
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

    votes = db.get_column('amz_reviews', 'vote')
    print_outlier_stats(votes)

