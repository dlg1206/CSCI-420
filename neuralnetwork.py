import tensorflow as tf
import math
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
    new_df.columns = new_df.columns.astype(str)
    new_df = new_df.drop(['9', '10', '12'], axis=1)

    # Add a new column to the data frame that has the review text length
    new_df.insert(3, 'text_lengths', text_lengths)
    new_df.columns = ["overall", "uid", "text_lengths", "score"]
    print(new_df)

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
        tf.keras.layers.Dense(4, activation='relu', input_shape=(2,)),
        tf.keras.layers.Dense(16, activation='relu'),
        tf.keras.layers.Dense(32, activation='relu'),
        tf.keras.layers.Dense(16, activation='relu'),
        tf.keras.layers.Dense(4, activation='relu'),
        tf.keras.layers.Dense(1, activation='sigmoid')
    ])

    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

    test_input = test[['score', 'text_lengths']]
    history = model.fit(train[['score', 'text_lengths']].to_numpy(), train[['overall']].to_numpy(), epochs=100)
    test_output = model.predict(test_input)

    # Accuracy graphs
    plt.plot(history.history['accuracy'], label='accuracy')
    plt.xlabel('Epoch')
    plt.ylabel('Accuracy')
    plt.ylim([0.5, 1])
    plt.legend(loc='lower right')
    plt.savefig("neuralnet.pdf")

    # Print example outputs
    print("\n")
    total = 0
    overall_array = test[['overall']].to_numpy()
    for i in range(0, len(test_output)):
        # Calculate predicted output
        overall = math.ceil((test_output[i][0] * 5))

        if i < 100:
            print(f"{test[['score', 'text_lengths', 'overall']].iloc[i]} "
                  f"\nPredicted Output: {overall}\n")

        # Calculate the accuracy
        if overall == math.ceil(overall_array[i][0] * 5):
            total = total + 1

    # Evaluate the model
    print(f"\n\nAccuracy on Test Data As Classification Problem: {total/len(test_output)}")


if __name__ == '__main__':
    Main()
