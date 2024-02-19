"""
File: raw_to_db.py
Description: Clean.json.gz file and upload to db

@author Derek Garcia
"""
import gzip
import json
import os
import sys
import time
import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

NUM_REVIEWS = 6739590
PATH_TO_ENV = "../.env"

TABLE = "amz_reviews"
COLUMNS = ["reviewerID", "asin", "reviewerName", "vote", "reviewText", "overall", "summary", "unixReviewTime"]
POSTGRES_COLUMNS = (str(COLUMNS)
                    .replace("[", "")
                    .replace("]", "")
                    .replace("'", "\""))  # for postgres column

LOG_FILE = "db_log.txt"


def parse(path):
    """
    Open and load .json.gz file

    :param path: path to .json.gz file
    :return: .json.gz object
    """
    g = gzip.open(path, 'r')
    for l in g:
        yield json.loads(l)


def clean(info: dict) -> None:
    """
    Clean and prepare the data before uploading to the database

    :param info: Raw entry
    """
    # Strip newlines from review
    if info.get('reviewText') is None:
        info['reviewText'] = None
    else:
        info['reviewText'] = "".join(info['reviewText'].splitlines()).replace("'", "''")  # '' prevents postgres errors

    # Strip newlines from summary
    # TODO need summary?
    if info.get('summary') is None:
        info['summary'] = None
    else:
        info['summary'] = "".join(info['summary'].splitlines()).replace("'", "''")  # '' prevents postgres errors

    # Remove unused columns
    info.pop("image", None)
    info.pop("reviewTime", None)
    info.pop("style", None)

    # Replace null with 0
    if info.get('vote') is None:
        info['vote'] = 0


def upload(row: dict, db: psycopg2) -> None:
    """
    Upload a row to a database

    :param row: row to upload
    :param db: database to use
    """
    cursor = db.cursor()

    # Clean row values
    values_str = ""
    for col in COLUMNS:
        value = row[col]
        values_str += f"'{value}', "

    values_str = values_str[:len(values_str) - 2]  # truncate trailing ' ,'

    # attempt to add and log result
    insert_query = f"INSERT INTO {TABLE} ({POSTGRES_COLUMNS}) VALUES ( {values_str} );"
    status = "Failed"  # assume failure
    try:
        cursor.execute(insert_query)
        db.commit()
        status = "Success"
    except Exception as e:
        print(e)
    finally:
        with open(LOG_FILE, "a+") as f:
            f.write(f"{datetime.datetime.now()} | {status} | {insert_query}\n")


if __name__ == '__main__':
    """
    usage: py raw_to_db.py <path to data>
    """
    # Attempt to establish connection w/ db
    db = None
    try:
        print("Attempting to establish db connection. . .")
        load_dotenv(dotenv_path=Path(PATH_TO_ENV))
        db = psycopg2.connect(
            database=os.getenv("database"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port")
        )
        print("Success!")
    except Exception as e:
        print("Failed to establish connection with database!")
        print(f"Make sure the following are defined in the .env folder at {PATH_TO_ENV}:")
        print("\thost: Host URL of DB"
              "\n\tdatabase: Database to access"
              "\n\tuser: User to access db as"
              "\n\tpassword: password for user"
              "\n\tport: port of db")
        print("See '.env-empty' for example")
        exit(1)

    # Attempt open data
    print(f"Opening {sys.argv[1]}. . .")
    start = time.perf_counter()
    try:
        data = parse(sys.argv[1])
    except Exception as e:
        print("Operation failed")
        print(e)
        exit(1)
    finally:
        print(f"Done in {time.perf_counter() - start:.2f}s")

    # Clean and Upload
    print("Cleaning Data and Uploading to DB")
    print("Start                                          End")
    open(LOG_FILE, 'w').close()  # clear logging file
    row_num = 0
    for r in data:
        # uncomment for testing
        # if row_num > 5:
        #     break
        # progress bar
        if row_num % round(NUM_REVIEWS % 50, 0) == 0:
            print("=", end="")
        # Keep only electronics
        if r['asin'][0] != "B":
            continue

        clean(r)        # preprocess data
        upload(r, db)   # upload to DB
        row_num += 1

    print()

    print(f"Done in {time.perf_counter() - start:.2f}s")
    print(f"Execution log can be found at {LOG_FILE}")
