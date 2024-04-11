import os
import time
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from dotenv import load_dotenv
from sklearn import preprocessing
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split

from util.LogMessage import LogMessage

# review length vs sentiment
# vote vs sentiment
PATH_TO_ENV = ".env"

from database import Database

BATCH_SIZE = 500


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


def load_data() -> pd.DataFrame:
    db = get_db_connection()
    query = ("SELECT length, score, vote "
             "FROM amz_reviews "
             "NATURAL JOIN valid_reviewtext "
             "NATURAL JOIN sentiment_analysis;")

    row_num = 0
    data = []

    with db.connection.cursor(name='csr') as cursor:
        start = time.perf_counter()
        LogMessage("0%", row_num, "-", f"Querying database with batch sizes {BATCH_SIZE}. . .").log(False, False)
        # batch query db to get data
        cursor.itersize = BATCH_SIZE
        cursor.execute(query)
        data += cursor.fetchall()
        LogMessage("100%", row_num, "SUCCESS", f"Complete in {time.perf_counter() - start:.2f}s").log()
    df = pd.DataFrame(data, columns=['review_len', 'score', 'vote'])
    return df


if __name__ == '__main__':
    new_df = load_data()

    # Scale the dataframe
    scaler = preprocessing.MinMaxScaler()
    d = scaler.fit_transform(new_df)
    scaled_df = pd.DataFrame(d, columns=new_df.columns)

    # Shuffle the data frame
    shuffled_df = scaled_df.sample(frac=1)

    # Split the data
    train, test = train_test_split(shuffled_df, test_size=0.3)

    # train and fit
    kmeans = KMeans(n_clusters=6, random_state=0, n_init='auto')
    trainKmeans = train[['review_len', 'score', 'vote']].copy()
    kmeans.fit(trainKmeans)

    # make graphs
    sns.scatterplot(data=trainKmeans, x='review_len', y='score', hue=kmeans.labels_)
    plt.title('Review Length vs Sentiment')
    # Set x-axis label
    plt.xlabel('Review Length ( characters )')
    # Set y-axis label
    plt.ylabel('Sentiment Score')
    plt.show()

    sns.scatterplot(data=trainKmeans, x='review_len', y='vote', hue=kmeans.labels_)
    plt.title('Review Length vs Helpfulness')
    # Set x-axis label
    plt.xlabel('Review Length ( characters )')
    # Set y-axis label
    plt.ylabel('Upvotes')
    plt.show()

    sns.scatterplot(data=trainKmeans, x='vote', y='score', hue=kmeans.labels_)
    plt.title('Helpfulness vs Sentiment')
    # Set x-axis label
    plt.xlabel('Upvotes')
    # Set y-axis label
    plt.ylabel('Sentiment Score')
    plt.show()
