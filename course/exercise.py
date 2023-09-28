from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .appName("Spark Essentials") \
    .getOrCreate()

"""
    We have a online store selling used gaming laptops.
    Every laptop "state"/config has
    - an id
    - make, model
    - perf score ("procSpeed") 
    
    We have a "marketplace" with people selling actual gaming laptops.
    - make, model
    - perf score ("procSpeed")
    - price (USD)
    
    We want to make a search tool, so that when someone clicks on a "laptop" (make, model, state),
    we would like to show "similar laptops" (items in the "offers" DF with same make/model, with perf score <= +/-0.1 difference)
    
    1. For every item in "laptops", we want the average price of "similar laptops"
    2. Optimize the most out of that job.
"""

if __name__ == '__main__':
    pass