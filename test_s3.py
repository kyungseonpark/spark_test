import pandas as pd
import random
from datetime import datetime, timedelta

from sklearn.datasets import make_classification
from pyspark.sql import SparkSession

def generate_random_offsets_in_month(year, month, num_offsets) -> list[int]:
    # Calculate the number of days in the specified month
    days_in_month = (datetime(year, month % 12 + 1, 1) - timedelta(days=1)).day

    # Set the start and end datetime for the specified month
    start_datetime = datetime(year, month, 1, 0, 0, 0)
    end_datetime = datetime(year, month, days_in_month, 23, 59, 59)

    # Calculate the time difference in seconds
    time_diff_seconds = (end_datetime - start_datetime).total_seconds()

    # Generate num_offsets random offsets in seconds within the range
    random_offsets = [random.randint(0, int(time_diff_seconds)) for _ in range(num_offsets)]

    return random_offsets

def random_datetimes_in_month(year, month, num_datetimes) -> list[datetime]:
    random_offsets = generate_random_offsets_in_month(year, month, num_datetimes)

    # Get the start datetime for the specified month
    start_datetime = datetime(year, month, 1, 0, 0, 0)

    # Generate random datetimes using the precomputed random offsets
    random_datetimes = [start_datetime + timedelta(seconds=offset) for offset in random_offsets]

    return random_datetimes


def make_classification_data(num_col: int, num_row: int) -> pd.DataFrame:
    X, y = make_classification(
        n_samples=num_row,
        n_features=num_col,
        n_informative=1,
        n_redundant=0,
        n_clusters_per_class=1,
        random_state=720,
        weights=(0.9, 0.1)
    )

    cols = list()
    for i in range(1, num_col + 1):
        cols.append(f'column{i}')

    res_df = pd.DataFrame(data=X, columns=cols)
    res_df['target'] = y

    # Example usage for January (month=1) of the year 2023 with almost a million random datetimes
    year = 2023
    month = 1
    random_datetimes = random_datetimes_in_month(year, month, num_row)
    res_df['date_time'] = random_datetimes

    return res_df


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Spark AWS Credentials Example") \
        .getOrCreate()

    for _ in range(100):
        res_df = make_classification_data(100, 10000)
        df = spark.createDataFrame(res_df)
        df.write.mode('append').parquet("s3a://test-ml-storage/ml-test/aizen_de_test_1.parquet")
    spark.stop()
