import os
import time
from pathlib import Path
from random import sample

import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from dotenv import load_dotenv
from scipy.stats import ttest_ind, ttest_rel
from scipy import stats
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


def bivariateAnalysisCountPlot(data_num, var1, var2, suffix):
    """
    This function will print the bi-variate analysis to the console and print a scatter plot showing the
    correlation between the two
    :param: data_num: the numeric data values
    :param: var1: the first numeric attribute in the bi-variate analysis
    :param: var2: the second numeric attribute in the bi-variate analysis
    :suffix: the table being taken from
    :return: All the summary statistics to the console
    """
    plt.figure(figsize=(12, 12))
    sns.boxplot(x=var1, y=var2, data=data_num)
    plt.savefig((f"SavedImages/boxplot{var1}{var2}{suffix}.png").replace(" ", ""))
    plt.clf()

    data = data_num[(np.abs(stats.zscore(data_num[var2])) < 3)]

    var1_1 = data[data[var1] > 3.0][var2]
    var1_0 = data[data[var1] <= 3.0][var2]

    var1_0 = var1_0.values.tolist()
    var1_0 = sample(var1_0, 100)
    var1_1 = var1_1.values.tolist()
    var1_1 = sample(var1_1, 100)

    ttest, pval = ttest_ind(var1_1, var1_0, equal_var=False)
    print(f"{suffix}"
          "\nNull Hypothesis :- there is no difference in Mean of review length in overall > 3.0 and overall < 3.0. "
          "\nAlternate Hypothesis :- there is difference in Mean of review length in overall > 3.0 and overall < 3.0.\n")
    print("ttest", ttest)
    print('p value', format(pval, '.70f'))

    if pval < 0.05:
        print("we reject null hypothesis\n")
    else:
        print("we accept null hypothesis\n")


def getHistogram(data_num, column, suffix):
    """
    This function will get the histogram for a numeric value
    :param: data_num: the numeric values dataframe
    :param: column: the column
    :param: suffix: end of the sql statement
    :return: The histogram as an image
    """
    data_num[column].hist(figsize=(12, 12))
    plt.title(f"{column}: {suffix}.")
    plt.savefig((f"SavedImages/Univariate_histogram{column}{suffix}.png").replace(" ", ""))
    plt.clf()


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
    plt.figure(figsize=(12, 12))
    sns.heatmap(data_num.corr(), annot=True, linewidths=.5, cmap="Blues")
    plt.title('Heatmap showing correlations between numerical data')
    plt.savefig((f"SavedImages/Multivariate_heatmap{suffix}.png").replace(" ", ""))
    plt.clf()

    # Create countplot
    bivariateAnalysisCountPlot(data_num, "overall", "text_lengths", suffix)

    # Binning Overall and Text Lengths
    high_values = len(data_num[data_num['overall'] > 3.0]) / len(data_num['overall'])
    low_values = len(data_num[data_num['overall'] <= 3.0]) / len(data_num['overall'])
    print(f"\nWe will define our high values as overall scores > 3.0"
          "\nOur low value will be overall scores <= 3.0"
          f"\n% in high values is {high_values}"
          f"\n% in low values is {low_values}")


if __name__ == '__main__':
    performEdaOnTable("amz_reviews")
    performEdaOnTable("amz_reviews INNER JOIN valid_vote ON amz_reviews.uid = valid_vote.uid")
    performEdaOnTable("amz_reviews INNER JOIN valid_reviewtext ON amz_reviews.uid = valid_reviewtext.uid")
