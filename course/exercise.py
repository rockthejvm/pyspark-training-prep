from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import sleep

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

def aneta_v1():
    laptops = spark.read.option('header', True).csv('../data/laptops')  # | registration | make | model| procSpeed
    offers = spark.read.option('header', True).csv('../data/offers')  # | make | model | procSpeed | salePrice
    offers = offers.withColumnRenamed('procSpeed', 'expectedSpeed')

    laptops_new = laptops.dropDuplicates(['make', 'model', 'procSpeed'])
    laptops_new = laptops_new.join(offers, on=['make', 'model'])
    laptops_new = laptops_new.withColumn('speedDiff', round(col('procSpeed') - col('expectedSpeed'), 2))
    laptops_new = laptops_new.where((col('speedDiff') <= 0.1) & (col('speedDiff') >= -0.1))

    result = laptops_new.groupby(['make', 'model', 'procSpeed']).agg(avg(col('salePrice')))
    result = laptops.join(broadcast(result), on=['make', 'model', 'procSpeed'])

    result.show()
    # 7.5s

def patrik_v1():
    laptops = spark \
        .read \
        .option("header", "true") \
        .csv("../data/laptops/part-00000-b5f16ee1-9e74-4764-adfa-27b979670a91-c000.csv")
    offers = spark \
        .read \
        .option("header", "true") \
        .csv("../data/offers/part-00000-4032814f-b3ea-4443-aa3f-4bba24c7c35c-c000.csv")
    laptops = laptops.withColumnRenamed("procSpeed", "exp_proc_speed")
    offers = offers \
        .withColumnRenamed("procSpeed", "act_proc_speed") \
        .groupBy("make", "model", "act_proc_speed") \
        .agg(count("*").alias("count"),
             sum("salePrice").alias("salePriceSum"))
    result = laptops \
        .join(offers, ['make', 'model'], "left_outer") \
        .filter(col("act_proc_speed").between(col("exp_proc_speed") - 0.1, col("exp_proc_speed") + 0.1)) \
        .withColumn("count", sum("count").over(Window.partitionBy("registration"))) \
        .withColumn("salePriceSum", sum("salePriceSum").over(Window.partitionBy("registration"))) \
        .dropDuplicates(["registration"]) \
        .withColumn("avg_price", col("salePriceSum") / col("count")) \
        .select("registration", "make", "model", "avg_price")
    result.show() # 4s

def basic_solution():
    laptops = spark.read.option('header', True).csv('../data/laptops')
    offers = spark.read.option('header', True).csv('../data/offers')
    joined = laptops\
        .join(offers, ["make", "model"]) \
        .filter(abs(offers.procSpeed - laptops.procSpeed) <= 0.1) \
        .groupBy("registration") \
        .agg(avg("salePrice").alias("avgPrice"))

    joined.show() # 2-3 mins


if __name__ == '__main__':
    basic_solution()
    sleep(9999999)
