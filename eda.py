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


def getSummaryStatisticsNumericAttributes():
    """
    This function will get the summary statistics for all the numeric attributes and print them to the console
    :return: All the summary statistics to the console
    """
    print("do stuff")


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


def getHistogram(numericValue):
    """
    This function will get the summary statistics for all the numeric attributes and print them to the console
    :param: numericAttribute: the attribute we will be getting the histogram for
    :return: The histogram as an image
    """
    print("do stuff")


def performEdaOnTable():
    query = "SELECT * FROM amz_reviews"
    row_num = 0

    # Get all of the info into a dataframe
    start = time.perf_counter()
    allData = db.get_all()
    print(f"Time to get all {time.perf_counter() - start:.2f}s")

    # Add a new column to the data frame that has the review text length


if __name__ == '__main__':
    performEdaOnTable()

