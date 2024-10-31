from pyspark.sql import SparkSession

import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Exercise2") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

"""
    "eBay for used gaming laptops"
        - laptops
            - unique configuration id
            - make
            - model
            - performance score aka "procSpeed"
        - offers = "actual laptop instances people can buy"
            - make
            - model
            - procSpeed
            - salePrice
    When someone clicks on a unique config, I want to show the average sale price of "similar" laptop instances on sale
    "Similar" = same make/model + perf score within 0.1 tolerance.
    
    For every unique config, calculate the average sale price of "similar laptops".
    Make the fastest job possible.
"""
def exercise2():
    pass


if __name__ == '__main__':
    pass