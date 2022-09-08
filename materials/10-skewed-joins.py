from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import sleep

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Skewed Joins") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

############################################################
# Skewed joins
############################################################

"""
Exercise: 
    We're running an online store of used gaming laptops.
    Each laptop is defined by
        - unique identifier
        - make & model
        - performance score (procSpeed in the data)
    
    We want to make our search tool smart, so that when someone clicks on a laptop (maybe they like its physical shape), 
        - the user should find all similar laptops (make + model) in the offers dataset with a perf score within 0.1
        - we can compute the average price of all such laptops
    
    Part 1: write the simplest Spark job you can think of: for every laptop compute average sale price of similar laptops.
    Part 2: optimize it.
"""

def demo_skew():
    laptops = spark.read.option("header", "true").csv("../data/laptops/laptops")
    laptopOffers = spark.read.option("header", "true").csv("../data/laptops/offers")
    joined = laptops.join(laptopOffers, ["make", "model"]) \
        .filter(abs(laptopOffers.procSpeed - laptops.procSpeed) <= 0.1) \
        .groupBy("registration") \
        .agg(avg("salePrice").alias("averagePrice"))
    """
        == Physical Plan ==
        *(4) HashAggregate(keys=[registration#4], functions=[avg(salePrice#20)])
        +- Exchange hashpartitioning(registration#4, 200), true, [id=#99]
           +- *(3) HashAggregate(keys=[registration#4], functions=[partial_avg(salePrice#20)])
              +- *(3) Project [registration#4, salePrice#20]
                 +- *(3) SortMergeJoin [make#5, model#6], [make#17, model#18], Inner, (abs((procSpeed#19 - procSpeed#7)) <= 0.1)
                    :- *(1) Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST], false, 0
                    :  +- Exchange hashpartitioning(make#5, model#6, 200), true, [id=#77]
                    :     +- LocalTableScan [registration#4, make#5, model#6, procSpeed#7]
                    +- *(2) Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST], false, 0
                       +- Exchange hashpartitioning(make#17, model#18, 200), true, [id=#78]
                          +- LocalTableScan [make#17, model#18, procSpeed#19, salePrice#20]
    """
    joined.explain()
    joined.show()

    laptops2 = laptops.withColumn("procSpeed", explode(array(col("procSpeed") - 0.1, col("procSpeed"), col("procSpeed") + 0.1)))
    joined2 = laptops2.join(laptopOffers, ["make", "model", "procSpeed"]) \
        .groupBy("registration") \
        .agg(avg("salePrice").alias("averagePrice"))
    """
        == Physical Plan ==
        *(4) HashAggregate(keys=[registration#4], functions=[avg(salePrice#20)])
        +- Exchange hashpartitioning(registration#4, 200), true, [id=#107]
           +- *(3) HashAggregate(keys=[registration#4], functions=[partial_avg(salePrice#20)])
              +- *(3) Project [registration#4, salePrice#20]
                 +- *(3) SortMergeJoin [make#5, model#6, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43))], [make#17, model#18, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19))], Inner
                    :- *(1) Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43)) ASC NULLS FIRST], false, 0
                    :  +- Exchange hashpartitioning(make#5, model#6, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43)), 200), true, [id=#85]
                    :     +- Generate explode(array((procSpeed#7 - 0.1), procSpeed#7, (procSpeed#7 + 0.1))), [registration#4, make#5, model#6], false, [procSpeed#43]
                    :        +- LocalTableScan [registration#4, make#5, model#6, procSpeed#7]
                    +- *(2) Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19)) ASC NULLS FIRST], false, 0
                       +- Exchange hashpartitioning(make#17, model#18, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19)), 200), true, [id=#86]
                          +- LocalTableScan [make#17, model#18, procSpeed#19, salePrice#20]
    """


if __name__ == '__main__':
    demo_skew()
    sleep(1000)