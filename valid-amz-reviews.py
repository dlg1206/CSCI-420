"""
File: valid-amz-reviews.py
Description: 

@author Derek Garcia
"""
import concurrent.futures
import os
import asyncio
import threading
import asyncpg
from pathlib import Path
import time
from multiprocessing.pool import ThreadPool

import psycopg2.extras
from dotenv import load_dotenv

from database import Database
from util.LogMessage import LogMessage

PATH_TO_ENV = ".env"
NUM_REVIEWS = 5486205
BATCH_SIZE = 500000
LOG_FILE = "vo_log.txt"


async def upload(uids):
    query = "INSERT into valid_amz_reviews values ( $1 )"
    conn = await asyncpg.connect(
            database=os.getenv("database"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"))
    # count = 0
    # leng = len(uids)
    # for uid in uids:
    #     await conn.execute(query, uid)
    #     count += 1
    #     print(f"{count / leng:.2f}%")
    await conn.executemany(query, uids)
    await conn.close()

    # with db.connection.cursor() as cursor:
    #     count = 0
    #
    #     for uid in uids:
    #         try:
    #             # q = f"INSERT INTO valid_amz_reviews VALUES %s"
    #             # psycopg2.extras.execute_values(
    #             #     cursor, q, uids, template=None, page_size=500
    #             # )
    #             count += 1
    #             start = time.perf_counter()
    #             progress = f"{round(100 * (count / len(uids)), 2)}%"
    #             cursor.execute("INSERT into valid_amz_reviews values (" + uid + ");")
    #
    #             LogMessage(f"{tid}: {progress}", count, "SUCCESS",
    #                        f"Upload Time: {time.perf_counter() - start:.2f}s").log()
    #         except Exception as e:
    #             LogMessage(f"{tid}: {progress}", count, "FAILED", "Unknown Error", e).log(
    #                 False, True)
    #         finally:
    #             db.connection.commit()


if __name__ == '__main__':
    load_dotenv(dotenv_path=Path(PATH_TO_ENV))
    db = Database(
        database=os.getenv("database"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port")
    )
    query = "SELECT amz_reviews.uid FROM amz_reviews NATURAL JOIN valid_vote NATURAL JOIN valid_reviewtext;"

    row_num = 0
    start_time = time.perf_counter()
    with db.connection.cursor(name='csr') as cursor:

        start = time.perf_counter()
        print(f"Number Connections: {round(NUM_REVIEWS / BATCH_SIZE, 0)}")
        LogMessage("0%", row_num, "-", f"Querying database with batch size {BATCH_SIZE}. . .").log(False, False)
        cursor.itersize = BATCH_SIZE
        cursor.execute(query)

        old_len = 0
        uids = []
        for row in cursor:
            progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"
            if row_num % BATCH_SIZE == 0:
                old_len = len(uids)
                LogMessage(progress, row_num, "SUCCESS", f"Response in {time.perf_counter() - start:.2f}s").log()

            uids.append((row[0],))

            row_num += 1
            LogMessage(progress, row_num, "", "").log(True, False)

            if row_num % BATCH_SIZE == 0:
                start = time.perf_counter()
                LogMessage(progress, row_num, "SUCCESS", f"Added {len(uids) - old_len} uids").log()
                LogMessage(progress, row_num, "-", f"Querying database with batch size {BATCH_SIZE}. . .").log()

    list_size = len(uids)
    with db.connection.cursor() as cursor:
        count = 0
        q = f"INSERT INTO valid_amz_reviews VALUES %s;"
        psycopg2.extras.execute_values(
            cursor, q, uids, template=None, page_size=500
        )
        # leng = len(uids)
        # cursor.executemany("INSERT into valid_amz_reviews values (%s);", )
        db.connection.commit()
        # for uid in uids:
        #     q = f"INSERT INTO valid_amz_reviews VALUES %s"
        #     psycopg2.extras.execute_values(
        #         cursor, q, uids, template=None, page_size=500
        #     )
        #
        #
        #     count += 1
        #     print(f"{count / leng:.2f}%")

        LogMessage(progress, row_num, "SUCCESS", f"Total Time: {time.perf_counter() - start_time:.2f}s").log()
        LogMessage(progress, row_num, "SUCCESS", f"Uploaded {list_size} uids").log()

    # data = []
    # start = 0
    # end = 0
    # tid = 0
    # while start < list_size:
    #     if end + 4500 > list_size:
    #         end = list_size
    #     else:
    #         end += 4500
    #
    #     data.append((tid, uids[start:end]))
    #     tid += 1
    #     start = end


    # print(f"Data: {len(data)}")
    # with ThreadPool(processes=1219) as pool:
    #     pool.starmap(upload, data)

    # asyncio.run(upload(uids))



