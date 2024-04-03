import os
import time
from pathlib import Path

import psycopg2.extras
from dotenv import load_dotenv
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from database import Database
from util.LogMessage import LogMessage

# Macros
PATH_TO_ENV = ".env"
NUM_REVIEWS = 5486205
BATCH_SIZE = 500

# Initialize Vader stuff
ANALYZER = SentimentIntensityAnalyzer()


def get_db_connection() -> Database:
    print("Connecting to the database. . . ")
    load_dotenv(dotenv_path=Path(PATH_TO_ENV))
    db = Database(
        database=os.getenv("database"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port")
    )
    print("Connected!")
    return db


def sentiment_analysis(row: tuple[int, str]) -> (int, float):
    """
    This function will perform eda on the given table
    :params: the table name as a string
    """

    # Create array an empty array of review text lengths
    if row[1] is None:
        i = 1
    vals = ANALYZER.polarity_scores(row[1])
    compound_value_normalized = (vals['compound'] + 1) / 2

    return row[0], compound_value_normalized


if __name__ == '__main__':
    db = get_db_connection()
    query = "SELECT uid, reviewtext FROM amz_reviews NATURAL JOIN valid_amz_reviews WHERE reviewtext is not null;"

    row_num = 0
    start_time = time.perf_counter()
    with db.connection.cursor(name='csr') as cursor:

        start = time.perf_counter()
        print(f"Number Connections: {round(NUM_REVIEWS / BATCH_SIZE, 0)}")
        LogMessage("0%", row_num, "-", f"Querying database with batch size {BATCH_SIZE}. . .").log(False, False)
        cursor.itersize = BATCH_SIZE
        cursor.execute(query)

        old_len = 0
        sentient_values = []
        for row in cursor:
            progress = f"{round(100 * (row_num / NUM_REVIEWS), 2)}%"
            if row_num % BATCH_SIZE == 0:
                old_len = len(sentient_values)
                LogMessage(progress, row_num, "SUCCESS", f"Response in {time.perf_counter() - start:.2f}s").log()

            sentient_values.append(sentiment_analysis(row))

            row_num += 1
            LogMessage(progress, row_num, "", "").log(True, False)

            if row_num % BATCH_SIZE == 0:
                start = time.perf_counter()
                LogMessage(progress, row_num, "SUCCESS", f"Processed {len(sentient_values) - old_len} reviews").log()
                LogMessage(progress, row_num, "-", f"Querying database with batch size {BATCH_SIZE}. . .").log()

    list_size = len(sentient_values)
    with db.connection.cursor() as cursor:
        count = 0
        q = f"INSERT INTO sentiment_analysis VALUES %s;"
        psycopg2.extras.execute_values(
            cursor, q, sentient_values, template=None, page_size=500
        )
        # leng = len(uids)
        db.connection.commit()
        LogMessage(progress, row_num, "SUCCESS", f"Total Time: {time.perf_counter() - start_time:.2f}s").log()
        LogMessage(progress, row_num, "SUCCESS", f"Uploaded {list_size} scores").log()
