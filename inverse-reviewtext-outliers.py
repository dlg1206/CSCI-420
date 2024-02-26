import os
import time
from pathlib import Path

import psycopg2.extras
from dotenv import load_dotenv

from database import Database
from outliers import Outlier
from util.LogMessage import LogMessage

PATH_TO_ENV = ".env"
Q1 = 0
Q3 = 0
NUM_REVIEWS = 6739590
BATCH_SIZE = 500000
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
    query = "SELECT uid, reviewtext FROM amz_reviews"
    not_outlier_uids = []
    row_num = 0
    start_time = time.perf_counter()
    with db.connection.cursor(name='csr') as cursor:

        start = time.perf_counter()
        print(f"Number Connections: {round(NUM_REVIEWS / BATCH_SIZE, 0)}")
        LogMessage("0%", row_num, "-", f"Querying database with batch size {BATCH_SIZE}. . .").log(False, False)
        cursor.itersize = BATCH_SIZE
        cursor.execute(query)

        old_len = 0
        for row in cursor:
            progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"
            if row_num % BATCH_SIZE == 0:
                old_len = len(not_outlier_uids)
                LogMessage(progress, row_num, "SUCCESS", f"Response in {time.perf_counter() - start:.2f}s").log()


            if row[1] is None:
                word_count = 0
            else:
                word_count = len(row[1].split())
            if not outlier_test.is_outlier(word_count):
                not_outlier_uids.append((row[0],))

            row_num += 1
            LogMessage(progress, row_num, "", "").log(True, False)

            if row_num % BATCH_SIZE == 0:
                start = time.perf_counter()
                LogMessage(progress, row_num, "SUCCESS", f"Added {len(not_outlier_uids) - old_len} uids").log()
                LogMessage(progress, row_num, "-", f"Querying database with batch size {BATCH_SIZE}. . .").log()

    with db.connection.cursor() as cursor:
        try:
            start = time.perf_counter()
            progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"
            q = f"INSERT INTO valid_reviewtext VALUES %s"
            psycopg2.extras.execute_values(
                cursor, q, not_outlier_uids, template=None, page_size=BATCH_SIZE
            )
            LogMessage(progress, row_num, "SUCCESS", f"Upload Time: {time.perf_counter() - start:.2f}s").log()
        except Exception as e:
            LogMessage(progress, row_num, "FAILED", "Unknown Error", e).log(False, True)
        db.connection.commit()

        LogMessage(progress, row_num, "SUCCESS", f"Total Time: {time.perf_counter() - start_time:.2f}s").log()
        LogMessage(progress, row_num, "SUCCESS", f"Uploaded {len(not_outlier_uids)} uids").log()