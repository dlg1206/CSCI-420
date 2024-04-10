import tensorflow as tf
import keras
import numpy as np
import pandas as pd
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
import seaborn as sns
import matplotlib.pyplot as plt
from database import Database
import os
import time
from pathlib import Path
from random import sample
from dotenv import load_dotenv

# Macros
PATH_TO_ENV = ".env"
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


def GetAllData(sql_statement):
    # Get the data from the database
    start = time.perf_counter()
    df = db.get_all(sql_statement)
    print(f"Time to get all {time.perf_counter() - start:.2f}s")

    # Create array of review text lengths
    text_lengths = []
    for review in df['reviewtext']:
        if review is None:
            text_lengths.append(0)
        else:
            text_lengths.append(len(review.split()))

    # Get the numerical values in the dataframe
    new_df = df.select_dtypes(include=np.number)
    new_df = new_df.drop(['vote', 'unixreviewtime'], axis=1)
    new_df = new_df.drop([9, 10], axis=1)
    new_df.columns = new_df.columns.astype(str)
    new_df.rename(columns={'11': "score"})

    # Add a new column to the data frame that has the review text length
    new_df.insert(3, 'text_lengths', text_lengths)
    print(new_df)
    print("hi")

    # Scale the dataframe
    scaler = preprocessing.MinMaxScaler()
    d = scaler.fit_transform(new_df)
    scaled_df = pd.DataFrame(d, columns=new_df.columns)

    # Shuffle the data frame
    shuffled_df = scaled_df.sample(frac=1)

    # Split the data
    train, test = train_test_split(shuffled_df, test_size=0.3)
    return train, test


def Main():
    RunNeuralNet("amz_reviews JOIN valid_reviewtext ON amz_reviews.uid = valid_reviewtext.uid"
                 " JOIN sentiment_analysis ON amz_reviews.uid = sentiment_analysis.uid LIMIT 500")


def RunNeuralNet(table_name):
    # Get the data
    train, test = GetAllData(table_name)

    model = tf.keras.models.Sequential([
        tf.keras.layers.Dense(4, activation='relu', input_shape=(3,)),
        tf.keras.layers.Dense(1, activation='sigmoid')
    ])

    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

    model.fit(train['overall'], train['text_length'], train['score'],  epochs=1000)

    test_input = test
    test_output = model.predict(test_input)
    print(test_output)


if __name__ == '__main__':
    Main()
