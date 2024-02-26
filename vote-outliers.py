import os
import time
from pathlib import Path

from dotenv import load_dotenv

from database import Database
from outliers import Outlier
from util.LogMessage import LogMessage

PATH_TO_ENV = ".env"
Q1 = 0
Q3 = 0
NUM_REVIEWS = 6739590
LOG_FILE = "vo_log.txt"

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
    query = "SELECT uid, vote FROM amz_reviews LIMIT 100"
    outlier_uids = []
    row_num = 0
    with db.connection.cursor(name='csr') as cursor:

        cursor.itersize = 500000
        cursor.execute(query)

        for row in cursor:
            progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"
            if outlier_test.is_outlier(row[1]):
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
                    q = f"INSERT INTO vote_outliers VALUES ({v})"
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
