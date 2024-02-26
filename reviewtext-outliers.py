import os

from pathlib import Path
import time
from dotenv import load_dotenv
import pandas as pd
from util.LogMessage import LogMessage
from database import Database
from outliers import Outlier

PATH_TO_ENV = ".env"
Q1 = 10
Q3 = 71
NUM_REVIEWS = 6739590
LOG_FILE = "rto_log.txt"

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
    query = "SELECT uid, reviewtext FROM amz_reviews LIMIT 100"
    outlier_uids = []
    row_num = 0
    with db.connection.cursor(name='csr') as cursor:

        cursor.itersize = 50
        cursor.execute(query)

        for row in cursor:
            progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"

            if row[1] is None:
                word_count = 0
            else:
                word_count = len(row[1].split())
            if outlier_test.is_outlier(word_count):
                outlier_uids.append(row[0])
            LogMessage(progress, row_num, "SUCCESS", f"Read").log(True, False)
            row_num += 1

    row_num = 0
    with db.connection.cursor() as cursor:
        try:

            for v in outlier_uids:
                try:

                    start = time.perf_counter()
                    progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"
                    q = f"INSERT INTO reviewtext_outliers VALUES ({v})"
                    # print(q)
                    cursor.execute(q)
                    db.connection.commit()
                    LogMessage(progress, row_num, "SUCCESS", f"Upload Time: {time.perf_counter() - start:.2f}s").log()
                    row_num += 1
                except Exception as e:
                    LogMessage(progress, row_num, "FAILED", "ROLLBACK", e).log(False, True)
                    db.connection.rollback()
                    continue
        except Exception as e:
            LogMessage(progress, row_num, "FAILED", "Unknown Error", e).log(False, True)


        print(f"Query done in {time.perf_counter() - start:.2f}s")
