import os
import time
from pathlib import Path

import psycopg2.extras
from dotenv import load_dotenv

from database import Database
from outliers import Outlier
from util.LogMessage import LogMessage

PATH_TO_ENV = ".env"
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
    query = "SELECT uid FROM amz_reviews WHERE uid NOT IN(SELECT uid FROM vote_outliers)"
    not_outlier_uids = []

    start_time = time.perf_counter()
    with db.connection.cursor(name='csr') as cursor:
        start = time.perf_counter()
        print(f"Number Connections: {round(BATCH_SIZE / NUM_REVIEWS, 2)}")
        LogMessage("0%", 0, "-", f"Querying database with batch size {BATCH_SIZE}. . .").log(False, False)
        cursor.itersize = BATCH_SIZE
        cursor.execute(query)



        old_len = 0
        row_num = 0
        for row in cursor:
            progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"
            if row_num % BATCH_SIZE == 0:
                old_len = len(not_outlier_uids)
                LogMessage(progress, row_num, "SUCCESS", f"Response in {time.perf_counter() - start:.2f}s").log()

            not_outlier_uids.append(row)
            row_num += 1
            LogMessage(progress, row_num, "", "").log(True,False)

            if row_num % BATCH_SIZE == 0:
                start = time.perf_counter()
                LogMessage(progress, row_num, "SUCCESS", f"Added {len(not_outlier_uids) - old_len} uids").log()
                LogMessage(progress, row_num, "-", f"Querying database with batch size {BATCH_SIZE}. . .").log()


    with db.connection.cursor() as cursor:
        try:
            start = time.perf_counter()
            LogMessage(progress, row_num, "-", f"Uploading {len(not_outlier_uids)} uids to database").log()

            q = f"INSERT INTO valid_vote VALUES %s"
            psycopg2.extras.execute_values(
                cursor, q, not_outlier_uids, template=None, page_size=BATCH_SIZE
            )
            progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"
            LogMessage(progress, row_num, "SUCCESS", f"Upload Time: {time.perf_counter() - start:.2f}s").log()
        except Exception as e:
            LogMessage(progress, row_num, "FAILED", "Unknown Error", e).log(False, True)
        finally:
            db.connection.commit()

        LogMessage(progress, row_num, "SUCCESS", f"Total Time: {time.perf_counter() - start_time:.2f}s").log()