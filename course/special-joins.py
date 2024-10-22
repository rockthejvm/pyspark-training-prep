from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
import os, sys
from time import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Special Joins") \
    .getOrCreate()

def demo_large_small_join():
    df_a = spark.range(1, 100000000)
    df_b = spark.createDataFrame([Row(0, "zero"), Row(1, "first"), Row(2, "second"), Row(3, "third")]) \
        .withColumnRenamed("_1", "id")
    joined = df_a.join(df_b, "id")
    joined.explain()
    start_time = time()
    joined.show()
    print(f"Time taken: {time() - start_time} seconds") # 5s

def demo_broadcast_join():
    df_a = spark.range(1, 100000000)
    df_b = spark.createDataFrame([Row(0, "zero"), Row(1, "first"), Row(2, "second"), Row(3, "third")]) \
        .withColumnRenamed("_1", "id")
    joined = df_a.join(broadcast(df_b), "id")
    joined.explain()
    start_time = time()
    joined.show()
    print(f"Time taken: {time() - start_time} seconds") # 1s

if __name__ == '__main__':
    demo_broadcast_join()