import os
import time
from pathlib import Path
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from dotenv import load_dotenv

from database import Database
from util.LogMessage import LogMessage

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


def bivariateAnalysisTwoVariables(var1, var2):
    """
    This function will print the bi-variate analysis to the console and print a scatter plot showing the
    correlation between the two
    :param: var1: the first numeric attribute in the bi-variate analysis
    :param: var2: the second numeric attribute in the bi-variate analysis
    :return: All the summary statistics to the console
    """
    print("do stuff")


def multivariateAnalysisThreeVariables(var1, var2, var3):
    """
    This function will print the multivariate analysis to the console and print a scatter plot showing the
    correlation between the three
    :param: var1: the first numeric attribute in the multivariate analysis
    :param: var2: the second numeric attribute in the multivariate analysis
    :param: var3: the second numeric attribute in the multivariate analysis
    :return: All the summary statistics to the console
    """
    print("do stuff")


def getHistogram(data_num, column, suffix):
    """
    This function will get the histogram for a numeric value
    :param: data_num: the numeric values dataframe
    :param: column: the column
    :param: suffix: end of the sql statement
    :return: The histogram as an image
    """
    data_num[column].hist(figsize=(8, 8))
    plt.title(f"{column}: {suffix}.")
    plt.savefig((f"SavedImages/Univariate_histogram{column}{suffix}.png").replace(" ", ""))


def performEdaOnTable(suffix: str):
    """
    This function will perform eda on the given table
    :params: the table name as a string
    """
    # Get all the info into a dataframe
    start = time.perf_counter()
    all_data = db.get_all(suffix)
    print(f"Time to get all {time.perf_counter() - start:.2f}s")

    # Drop the 9 column if there is one
    if len(all_data.columns) > 9:
        all_data = all_data.drop([9], axis=1)

    # Create array of review text lengths
    start = time.perf_counter()
    text_lengths = []
    for review in all_data['reviewtext']:
        if review is None:
            text_lengths.append(0)
        else:
            text_lengths.append(len(review.split()))

    # Add a new column to the data frame that has the review text length
    all_data.insert(9, "text_lengths", text_lengths)
    print(f"Time to insert text lengths {time.perf_counter() - start:.2f}s")

    # Get summary statistics for numeric values
    data_num = all_data.drop(["reviewerid", "asin", "reviewername", "reviewtext", "summary",
                              "uid", "unixreviewtime"], axis=1)
    print(f"\n{suffix}:\n{data_num.describe()}\n")

    # Get histograms for the numeric values
    getHistogram(data_num, 'overall', suffix)
    getHistogram(data_num, 'text_lengths', suffix)
    getHistogram(data_num, 'vote', suffix)

    # Show correlations on numerical data
    plt.figure(figsize=(8, 8))
    sns.heatmap(data_num.corr(), annot=True, linewidths=.5, cmap="Blues")
    plt.title('Heatmap showing correlations between numerical data')
    plt.savefig((f"SavedImages/Multivariate_heatmap{suffix}.png").replace(" ", ""))


if __name__ == '__main__':
    performEdaOnTable("amz_reviews")
    performEdaOnTable("amz_reviews INNER JOIN valid_vote ON amz_reviews.uid = valid_vote.uid")
    performEdaOnTable("amz_reviews INNER JOIN valid_reviewtext ON amz_reviews.uid = valid_reviewtext.uid")
