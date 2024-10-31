from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import time, sleep
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Exercise2") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.adaptive.enabled", "false") \
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

def ebay_exercise():
    laptops_df = spark.read.option("inferSchema", "true").csv("../data/laptops", header=True).withColumnRenamed("procSpeed", "laptopProcSpeed")
    offers_df = spark.read.option("inferSchema", "true").csv("../data/offers", header=True)

    offers_df.printSchema()
    similar_laptops_df = (
        laptops_df
        .join(
            offers_df,
            on=["make", "model"]
        )
    )

    # similar_laptops_df.show(3)

    similar_laptops_df = (
        similar_laptops_df
        .filter(
            abs(col("procSpeed") - col("laptopProcSpeed")) <= 0.1
        )
    )

    final_df = (
        similar_laptops_df
        .groupBy("registration")
        .avg("salePrice")
    )

    # final_df.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[registration#17], functions=[avg(salePrice#45)])
       +- Exchange hashpartitioning(registration#17, 200), ENSURE_REQUIREMENTS, [plan_id=82]
          +- HashAggregate(keys=[registration#17], functions=[partial_avg(salePrice#45)])
             +- Project [registration#17, salePrice#45]
                +- SortMergeJoin [make#18, model#19], [make#42, model#43], Inner, (abs((procSpeed#44 - laptopProcSpeed#50)) <= 0.1)
                   :- Sort [make#18 ASC NULLS FIRST, model#19 ASC NULLS FIRST], false, 0
                   :  +- Exchange hashpartitioning(make#18, model#19, 200), ENSURE_REQUIREMENTS, [plan_id=74]
                   :     +- Project [registration#17, make#18, model#19, procSpeed#20 AS laptopProcSpeed#50]
                   :        +- Filter ((isnotnull(procSpeed#20) AND isnotnull(make#18)) AND isnotnull(model#19))
                   :           +- FileScan csv [registration#17,make#18,model#19,procSpeed#20] Batched: false, DataFilters: [isnotnull(procSpeed#20), isnotnull(make#18), isnotnull(model#19)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(procSpeed), IsNotNull(make), IsNotNull(model)], ReadSchema: struct<registration:string,make:string,model:string,procSpeed:double>
                   +- Sort [make#42 ASC NULLS FIRST, model#43 ASC NULLS FIRST], false, 0
                      +- Exchange hashpartitioning(make#42, model#43, 200), ENSURE_REQUIREMENTS, [plan_id=75]
                         +- Filter ((isnotnull(procSpeed#44) AND isnotnull(make#42)) AND isnotnull(model#43))
                            +- FileScan csv [make#42,model#43,procSpeed#44,salePrice#45] Batched: false, DataFilters: [isnotnull(procSpeed#44), isnotnull(make#42), isnotnull(model#43)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(procSpeed), IsNotNull(make), IsNotNull(model)], ReadSchema: struct<make:string,model:string,procSpeed:double,salePrice:double>
    """

    # start_time = time()
    # final_df.show() # takes a lot of time
    # print(f"Time (basic): {time() - start_time}s") # 22s (Daniel's machine)

    # SALTING - add a new (usually numeric) column, in a range (sqrt of the max task time/median task time) = 54
    # one DF is getting ALL the values in the spectrum -> multiply the smaller table by that salt range (laptops x 54)
    # the other DF is getting ONE of the values in the spectrum at random
    # do the join by the columns + the salt
    laptops_salted = laptops_df.withColumn("salt", explode(sequence(lit(0), lit(54))))
    offers_salted = offers_df.withColumn("salt", floor(rand() * 54))

    result_df = laptops_salted \
        .join(offers_salted, ["make", "model", "salt"]) \
        .filter(abs(col("procSpeed") - col("laptopProcSpeed")) <= 0.1) \
        .groupBy("registration") \
        .avg("salePrice")

    # result_df.explain()
    """
    == Physical Plan ==
    *(6) HashAggregate(keys=[registration#17], functions=[avg(salePrice#51)])
    +- Exchange hashpartitioning(registration#17, 200), ENSURE_REQUIREMENTS, [plan_id=108]
       +- *(5) HashAggregate(keys=[registration#17], functions=[partial_avg(salePrice#51)])
          +- *(5) Project [registration#17, salePrice#51]
             +- *(5) SortMergeJoin [make#18, model#19, cast(salt#74 as bigint)], [make#48, model#49, salt#80L], Inner, (abs((procSpeed#50 - laptopProcSpeed#25)) <= 0.1)
                :- *(2) Sort [make#18 ASC NULLS FIRST, model#19 ASC NULLS FIRST, cast(salt#74 as bigint) ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(make#18, model#19, cast(salt#74 as bigint), 200), ENSURE_REQUIREMENTS, [plan_id=90]
                :     +- *(1) Generate explode(org.apache.spark.sql.catalyst.expressions.UnsafeArrayData@9e38ecd2), [registration#17, make#18, model#19, laptopProcSpeed#25], false, [salt#74]
                :        +- *(1) Project [registration#17, make#18, model#19, procSpeed#20 AS laptopProcSpeed#25]
                :           +- *(1) Filter ((isnotnull(procSpeed#20) AND isnotnull(make#18)) AND isnotnull(model#19))
                :              +- FileScan csv [registration#17,make#18,model#19,procSpeed#20] Batched: false, DataFilters: [isnotnull(procSpeed#20), isnotnull(make#18), isnotnull(model#19)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(procSpeed), IsNotNull(make), IsNotNull(model)], ReadSchema: struct<registration:string,make:string,model:string,procSpeed:double>
                +- *(4) Sort [make#48 ASC NULLS FIRST, model#49 ASC NULLS FIRST, salt#80L ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(make#48, model#49, salt#80L, 200), ENSURE_REQUIREMENTS, [plan_id=99]
                      +- *(3) Filter (((isnotnull(procSpeed#50) AND isnotnull(make#48)) AND isnotnull(model#49)) AND isnotnull(salt#80L))
                         +- *(3) Project [make#48, model#49, procSpeed#50, salePrice#51, FLOOR((rand(-1552496326801789285) * 54.0)) AS salt#80L]
                            +- FileScan csv [make#48,model#49,procSpeed#50,salePrice#51] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<make:string,model:string,procSpeed:double,salePrice:double>
    """
    # start_time = time()
    # result_df.show()
    # print(f"Time (salted): {time() - start_time}s") # 4.3 seconds (Daniel's machine)

    # alternative
    laptops_proto_salted = laptops_df.withColumn("procSpeed", explode(array(col("laptopProcSpeed"), col("laptopProcSpeed") - 0.1, col("laptopProcSpeed") + 0.1)))
    result_df = laptops_proto_salted \
        .join(offers_df, ["make", "model", "procSpeed"]) \
        .groupBy("registration") \
        .avg("salePrice")

    result_df.explain()
    start_time = time()
    result_df.show()
    print(f"Time (proto-salted): {time() - start_time}s") # 2.9s


if __name__ == '__main__':
    ebay_exercise()
    sleep(99999)