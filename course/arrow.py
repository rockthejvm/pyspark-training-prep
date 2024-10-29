from pyspark.sql import SparkSession
import os, sys
import pandas as pd
import numpy as np
from time import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("SparkPlayground") \
    .getOrCreate()

# make sure you have pyarrow and pandas installed
# 13 seconds (Macbook M3)
def demo_no_arrow():
    start_time = time()
    pdf = pd.DataFrame(np.random.rand(1000000, 3))
    df = spark.createDataFrame(pdf)
    df.show()
    print(f"Time taken to build/convert a Pandas DF to Spark (no Arrow): {time() - start_time}")

# 2.6s (Macbook M3)
def demo_with_arrow():
    # enables Arrow for data serialization
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    start_time = time()
    pdf = pd.DataFrame(np.random.rand(1000000, 3))
    df = spark.createDataFrame(pdf)
    df.show()
    print(f"Time taken to build/convert a Pandas DF to Spark (no Arrow): {time() - start_time}")

"""
    Arrow is great for Python lists/Pandas transformations to/from Spark DFs
        - Pandas
        - lists
        - numpy
        - ranges
        - vectorized transformations
    Enabled by default in Spark 4.
"""
if __name__ == '__main__':
    demo_with_arrow()