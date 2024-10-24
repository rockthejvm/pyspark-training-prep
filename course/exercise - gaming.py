from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import time, sleep
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Spark Exercise - Gaming Platform") \
    .getOrCreate()

"""
    Exercise - fictitious eBay of gaming laptops
    Datasets
        - laptops: reg id (unique), make, model, perf score (procSpeed)
        - offers: make, model, perf score (proc_speed), sale price
        
    Calculate, for every unique reg id, the average price of "similar" laptop instances (in the offers table)
        - similar = same make & model, perf_score at MOST 0.1 different than the config
"""

if __name__ == '__main__':
    pass