import os

from pathlib import Path
import time
from dotenv import load_dotenv
import pandas as pd

from database import Database
from outliers import Outlier


PATH_TO_ENV = ".env"
Q1 = 0
Q3 = 0

if __name__ == '__main__':
    load_dotenv(dotenv_path=Path(PATH_TO_ENV))
    db = Database(
        database=os.getenv("database"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port")
    )
    outlier_test = Outlier(Q1, Q3)
    query = "SELECT uid, vote FROM amz_reviews"
    outlier_uids = []
    with db.connection.cursor(name='csr') as cursor:
        start = time.perf_counter()
        cursor.itersize = 500000
        cursor.execute(query)

        for row in cursor:
            if outlier_test.is_outlier(row[1]):
                outlier_uids.append(row[0])

    with db.connection.cursor() as cursor:
        for v in outlier_uids:
            q = f"INSERT INTO vote_outliers VALUES ({v})"
            cursor.execute(q)
        db.connection.commit()

        print(f"Query done in {time.perf_counter() - start:.2f}s")
