from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.adaptive.enabled", "false") \
    .appName("Big Exercise") \
    .getOrCreate()

"""
    We're running an online marketplace for used gaming laptops.
    - laptops DF -> unique laptop configurations (unique id, make, model, expected performance/proc speed)
    - offers DF -> actual laptop items for sale (make, model, actual performance, price)
    
    We want to implement a smart suggestion tool, so that when someone clicks on a laptop config, they should see
        "similar laptops" = items in the offers DF with the same make/model and performance <= 0.1 difference
        e.g. Alienware Area51, perf 3.9 => all laptops in offers DF, of type Alienware Area51, with perf score 3.8, 3.9, 4.0
        
    Question #1: for every laptop config, compute the average price of "similar laptops".
    Question #2: make the query as fast as possible.
    
"""
def big_exercise():
    pass



if __name__ == '__main__':
    pass