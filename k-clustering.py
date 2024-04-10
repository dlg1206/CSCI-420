import os
import time
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from dotenv import load_dotenv
from sklearn import preprocessing
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.model_selection import train_test_split

from util.LogMessage import LogMessage

# review length vs sentiment
# vote vs sentiment
PATH_TO_ENV = ".env"

from database import Database

BATCH_SIZE = 500
NUM_REVIEWS = 6700000


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
    query = ("SELECT min_max as review_len, score, vote FROM amz_reviews NATURAL JOIN temp NATURAL JOIN "
             "sentiment_analysis;")

    row_num = 0
    start_time = time.perf_counter()
    df = pd.DataFrame(columns=['review_len', 'score', 'vote'])
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

            df.insert(row, column=[])

            row_num += 1
            LogMessage(progress, row_num, "", "").log(True, False)

            if row_num % BATCH_SIZE == 0:
                start = time.perf_counter()
                LogMessage(progress, row_num, "SUCCESS", f"Processed {len(sentient_values) - old_len} reviews").log()
                LogMessage(progress, row_num, "-", f"Querying database with batch size {BATCH_SIZE}. . .").log()
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

    kmeans = KMeans(n_clusters=7, random_state=0, n_init='auto')
    trainKmeans = train[['review_len', "score"]].copy()
    kmeans.fit(trainKmeans)

    sns.scatterplot(data=trainKmeans, x='review_len', y='score', hue=kmeans.labels_)
    plt.show()

    K = range(2, 9)
    fits = []
    score = []

    for k in K:
        # train the model for current value of k on training data
        model = KMeans(n_clusters=k, random_state=0, n_init='auto').fit(trainKmeans)

        # append the model to fits
        fits.append(model)

        # Append the silhouette score to scores
        score.append(silhouette_score(trainKmeans, model.labels_, metric='euclidean'))

    for i in range(0, len(fits)):
        print(fits[i])
        print(score[i])
