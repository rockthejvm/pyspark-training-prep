from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import time
import pandas as pd
import numpy as np
import os,sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Arrow Demo") \
    .getOrCreate()

def multiply_by_two(x):
    return x * 2

# column is of type pandas.Series
# returns pandas.Series
def multiply_by_two_vectorized(column):
    return column * 2

def demo_arrow():
    num_rows = 1000000
    df = spark.createDataFrame(
        pd.DataFrame({
            "id": np.arange(num_rows),
            "value": np.random.randn(num_rows)
        })
    )

    regular_udf = udf(multiply_by_two, DoubleType()) # 1.7s

    start_time = time()
    df_with_regular_udf = df.withColumn("result_regular", regular_udf(col("value")))
    df_with_regular_udf.show()
    print(f"Regular UDF time: ${time() - start_time} seconds")

    # enable Arrow
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    vectorized_udf = pandas_udf(multiply_by_two_vectorized, DoubleType())
    start_time = time()
    df_with_vectorized_udf = df.withColumn("result_vectorized", vectorized_udf(col("value")))
    df_with_vectorized_udf.show()
    print(f"Vectorized UDF time: ${time() - start_time} seconds") # 0.3s


if __name__ == '__main__':
    demo_arrow()