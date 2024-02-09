import csv
import gzip
import json
import sys
import time
"""
File: raw_to_csv.py
Description: Convert .json.gz file to csv

@author Derek Garcia
"""
HEADER = ["reviewerID", "asin", "reviewerName", "vote", "style", "reviewText", "overall", "summary", "unixReviewTime",
          "reviewTime", "image"]


def parse(path):
    """
    Open and load .json.gz file

    :param path: path to .json.gz file
    :return: .json.gz object
    """
    g = gzip.open(path, 'r')
    for l in g:
        yield json.loads(l)


def build_row(info: dict) -> list:
    """
    Convert JSON to CSV row for dataset

    :param info: JSON data
    :return: List for CSV Row
    """
    return ["" if info.get('reviewerID') is None else info.get('reviewerID'),
            "" if info.get('asin') is None else info.get('asin'),
            "" if info.get('reviewerName') is None else info.get('reviewerName'),
            "" if info.get('vote') is None else info.get('vote'),
            "" if info.get('style') is None else info.get('style'),
            "" if info.get('reviewText') is None else info.get('reviewText'),
            "" if info.get('overall') is None else info.get('overall'),
            "" if info.get('summary') is None else info.get('summary'),
            "" if info.get('unixReviewTime') is None else info.get('unixReviewTime'),
            "" if info.get('reviewTime') is None else info.get('reviewTime'),
            "" if info.get('image') is None else info.get('image')
            ]


if __name__ == '__main__':
    """
    usage: py raw_to_csv.py <path to data> <path to output>
    """

    #  Attempt open data
    print(f"Opening {sys.argv[1]}. . .")
    start = time.perf_counter()
    try:
        data = parse(sys.argv[1])
    except Exception as e:
        print("Operation failed")
        print(e)
        exit(1)
    print(f"Done in {time.perf_counter() - start:.2f}s")

    # Convert to CSV
    print("Converting to CSV")
    count = 0
    with open(sys.argv[2], 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(HEADER)

        # each row is json obj
        for r in data:
            writer.writerow(build_row(r))
            # Currently capped to 10 rows
            if count > 10:
                break
            count += 1

    print(f"Wrote to {sys.argv[2]}")
    print(f"Done in {time.perf_counter() - start:.2f}s")
