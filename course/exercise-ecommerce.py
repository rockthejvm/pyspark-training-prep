from pyspark.sql import SparkSession
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.sql.adaptive.enabled", "false") \
    .appName("Spark Exercise - eCommerce") \
    .getOrCreate()

"""
    Exercise - fictitious eCommerce platform
    Tasks
        - total revenue by customer id, in descending order
        - total revenue by product category in descending order
        
    Write the simplest code that does the job, then optimize it.
"""


if __name__ == '__main__':
    pass