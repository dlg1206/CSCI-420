import os
import time
from pathlib import Path

import psycopg2.extras
from dotenv import load_dotenv

from database import Database
from outliers import Outlier
from util.LogMessage import LogMessage

PATH_TO_ENV = ".env"
Q1 = 10
Q3 = 71
NUM_REVIEWS = 6739590
BATCH_SIZE = 500000

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
    query = "SELECT uid, reviewtext FROM amz_reviews"
    outlier_uids = []
    row_num = 0
    with db.connection.cursor(name='csr') as cursor:

        cursor.itersize = BATCH_SIZE
        cursor.execute(query)

        for row in cursor:
            progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"

            if row[1] is None:
                word_count = 0
            else:
                word_count = len(row[1].split())
            if outlier_test.is_outlier(word_count):
                outlier_uids.append((row[0],))
            LogMessage(progress, row_num, "SUCCESS", f"Read").log(True, False)
            row_num += 1

    with db.connection.cursor() as cursor:
        try:
            start = time.perf_counter()
            progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"
            q = f"INSERT INTO reviewtext_outliers VALUES %s"
            psycopg2.extras.execute_values(
                cursor, q, outlier_uids, template=None, page_size=100
            )
            LogMessage(progress, row_num, "SUCCESS", f"Upload Time: {time.perf_counter() - start:.2f}s").log()
        except Exception as e:
            LogMessage(progress, row_num, "FAILED", "Unknown Error", e).log(False, True)
        db.connection.commit()
