import os
import time
from pathlib import Path
from random import sample

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from dotenv import load_dotenv
from scipy.stats import ttest_ind, ttest_rel
from scipy import stats
from sqlalchemy.dialects.postgresql import psycopg2

from database import Database
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Macros
PATH_TO_ENV = ".env"
NUM_REVIEWS = 6739590
BATCH_SIZE = 500000
LOG_FILE = "vo_log.txt"

# Load the database info
load_dotenv(dotenv_path=Path(PATH_TO_ENV))
db = Database(
    database=os.getenv("database"),
    user=os.getenv("user"),
    password=os.getenv("password"),
    host=os.getenv("host"),
    port=os.getenv("port")
)


def sentiment_analysis(suffix: str):
    """
    This function will perform eda on the given table
    :params: the table name as a string
    """
    # Get all the info into a dataframe
    start = time.perf_counter()
    all_data = db.get_all(suffix)
    print(f"Time to get all {time.perf_counter() - start:.2f}s")

    # Initialize Vader stuff
    analyzer = SentimentIntensityAnalyzer()

    # Create array an empty array of review text lengths
    start = time.perf_counter()
    df = pd.DataFrame()
    sentiment_values = []
    ids = []
    for index, row in all_data.iterrows():
        vals = analyzer.polarity_scores(row['reviewtext'])
        compound_value_normalized = (vals['compound'] + 1)/2
        sentiment_values.append(compound_value_normalized)
        id_value = row['uid']
        ids.append(id_value)

    print(f"Time to perform sentiment analysis {time.perf_counter() - start:.2f}s")

    df['uid'] = ids
    df['sentiment_value'] = sentiment_values

    # Add to db
    start = time.perf_counter()
    with db.connection.cursor() as cursor:
        count = 0
        q = f"INSERT INTO sentiment_analysis VALUES %s;"
        psycopg2.extras.execute_values(
            cursor, q, df, template=None, page_size=500
        )

    print(f"Time to add to database {time.perf_counter() - start:.2f}s")


if __name__ == '__main__':
    sentiment_analysis("valid_amz_reviews")
