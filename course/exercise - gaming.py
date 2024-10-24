from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import time, sleep
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
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
def gaming_exc():
    offers_df = spark.read.option("header", "true").csv("../data/offers")
    laptops_df = spark.read.option("header", "true").csv("../data/laptops")

    # Standard solution
    offers_df = offers_df.withColumnRenamed('procSpeed', 'procSpeed_right')

    df = laptops_df.join(offers_df, on=['make', 'model'])
    df = df.filter(abs(col('procSpeed_right') - col('procSpeed')) <= lit(0.1))
    df = df.groupBy('registration').agg(avg(col('salePrice')))

    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[registration#42], functions=[avg(cast(salePrice#20 as double))])
       +- Exchange hashpartitioning(registration#42, 200), ENSURE_REQUIREMENTS, [plan_id=82]
          +- HashAggregate(keys=[registration#42], functions=[partial_avg(cast(salePrice#20 as double))])
             +- Project [registration#42, salePrice#20]
                +- SortMergeJoin [make#43, model#44], [make#17, model#18], Inner, (abs((cast(procSpeed_right#50 as double) - cast(procSpeed#45 as double))) <= 0.1)
                   :- Sort [make#43 ASC NULLS FIRST, model#44 ASC NULLS FIRST], false, 0
                   :  +- Exchange hashpartitioning(make#43, model#44, 200), ENSURE_REQUIREMENTS, [plan_id=74]
                   :     +- Filter ((isnotnull(procSpeed#45) AND isnotnull(make#43)) AND isnotnull(model#44))
                   :        +- FileScan csv [registration#42,make#43,model#44,procSpeed#45] Batched: false, DataFilters: [isnotnull(procSpeed#45), isnotnull(make#43), isnotnull(model#44)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(procSpeed), IsNotNull(make), IsNotNull(model)], ReadSchema: struct<registration:string,make:string,model:string,procSpeed:string>
                   +- Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST], false, 0
                      +- Exchange hashpartitioning(make#17, model#18, 200), ENSURE_REQUIREMENTS, [plan_id=75]
                         +- Project [make#17, model#18, procSpeed#19 AS procSpeed_right#50, salePrice#20]
                            +- Filter ((isnotnull(procSpeed#19) AND isnotnull(make#17)) AND isnotnull(model#18))
                               +- FileScan csv [make#17,model#18,procSpeed#19,salePrice#20] Batched: false, DataFilters: [isnotnull(procSpeed#19), isnotnull(make#17), isnotnull(model#18)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(procSpeed), IsNotNull(make), IsNotNull(model)], ReadSchema: struct<make:string,model:string,procSpeed:string,salePrice:string>
                               
    No AQE:
    == Physical Plan ==
    *(6) HashAggregate(keys=[registration#42], functions=[avg(cast(salePrice#20 as double))])
    +- Exchange hashpartitioning(registration#42, 200), ENSURE_REQUIREMENTS, [plan_id=104]
       +- *(5) HashAggregate(keys=[registration#42], functions=[partial_avg(cast(salePrice#20 as double))])
          +- *(5) Project [registration#42, salePrice#20]
             +- *(5) SortMergeJoin [make#43, model#44], [make#17, model#18], Inner, (abs((cast(procSpeed_right#50 as double) - cast(procSpeed#45 as double))) <= 0.1)
                :- *(2) Sort [make#43 ASC NULLS FIRST, model#44 ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(make#43, model#44, 200), ENSURE_REQUIREMENTS, [plan_id=86]
                :     +- *(1) Filter ((isnotnull(procSpeed#45) AND isnotnull(make#43)) AND isnotnull(model#44))
                :        +- FileScan csv [registration#42,make#43,model#44,procSpeed#45] Batched: false, DataFilters: [isnotnull(procSpeed#45), isnotnull(make#43), isnotnull(model#44)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(procSpeed), IsNotNull(make), IsNotNull(model)], ReadSchema: struct<registration:string,make:string,model:string,procSpeed:string>
                +- *(4) Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(make#17, model#18, 200), ENSURE_REQUIREMENTS, [plan_id=95]
                      +- *(3) Project [make#17, model#18, procSpeed#19 AS procSpeed_right#50, salePrice#20]
                         +- *(3) Filter ((isnotnull(procSpeed#19) AND isnotnull(make#17)) AND isnotnull(model#18))
                            +- FileScan csv [make#17,model#18,procSpeed#19,salePrice#20] Batched: false, DataFilters: [isnotnull(procSpeed#19), isnotnull(make#17), isnotnull(model#18)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(procSpeed), IsNotNull(make), IsNotNull(model)], ReadSchema: struct<make:string,model:string,procSpeed:string,salePrice:string>
    """

    # salting
    # generate a number between 0-10 in the bigger DF
    # in the smaller DF, will EXPLODE ALL values 0-10
    # how big should you make the salt?
    #   + tasks being split
    #   - duplicated dataset
    # best to pick a salt = sqrt(max task time / median task time)
    laptops2 = laptops_df.withColumn("salt", explode(sequence(lit(0), lit(99)))) # 100x as big
    offers2 = offers_df.withColumn("salt", floor(rand() * 100)) # every row will have a random 0-9

    offers2 = offers2.withColumnRenamed('procSpeed', 'procSpeed_right')

    result2 = laptops2.join(offers2, on=['make', 'model', 'salt'])
    result2 = result2.filter(abs(col('procSpeed_right') - col('procSpeed')) <= lit(0.1))
    result2 = result2.groupBy('registration').agg(avg(col('salePrice')))

    """
    == Physical Plan ==
    *(6) HashAggregate(keys=[registration#42], functions=[avg(cast(salePrice#20 as double))])
    +- Exchange hashpartitioning(registration#42, 200), ENSURE_REQUIREMENTS, [plan_id=112]
       +- *(5) HashAggregate(keys=[registration#42], functions=[partial_avg(cast(salePrice#20 as double))])
          +- *(5) Project [registration#42, salePrice#20]
             +- *(5) SortMergeJoin [make#43, model#44, cast(salt#74 as bigint)], [make#17, model#18, salt#80L], Inner, (abs((cast(procSpeed_right#50 as double) - cast(procSpeed#45 as double))) <= 0.1)
                :- *(2) Sort [make#43 ASC NULLS FIRST, model#44 ASC NULLS FIRST, cast(salt#74 as bigint) ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(make#43, model#44, cast(salt#74 as bigint), 200), ENSURE_REQUIREMENTS, [plan_id=94]
                :     +- *(1) Generate explode(org.apache.spark.sql.catalyst.expressions.UnsafeArrayData@8bc285df), [registration#42, make#43, model#44, procSpeed#45], false, [salt#74]
                :        +- *(1) Filter ((isnotnull(procSpeed#45) AND isnotnull(make#43)) AND isnotnull(model#44))
                :           +- FileScan csv [registration#42,make#43,model#44,procSpeed#45] Batched: false, DataFilters: [isnotnull(procSpeed#45), isnotnull(make#43), isnotnull(model#44)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(procSpeed), IsNotNull(make), IsNotNull(model)], ReadSchema: struct<registration:string,make:string,model:string,procSpeed:string>
                +- *(4) Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST, salt#80L ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(make#17, model#18, salt#80L, 200), ENSURE_REQUIREMENTS, [plan_id=103]
                      +- *(3) Filter (((isnotnull(procSpeed_right#50) AND isnotnull(make#17)) AND isnotnull(model#18)) AND isnotnull(salt#80L))
                         +- *(3) Project [make#17, model#18, procSpeed#19 AS procSpeed_right#50, salePrice#20, FLOOR((rand(-8558564163830623940) * 10.0)) AS salt#80L]
                            +- FileScan csv [make#17,model#18,procSpeed#19,salePrice#20] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<make:string,model:string,procSpeed:string,salePrice:string>
    """

    result2.explain()
    start_time = time()
    result2.show()
    print(f"Time taken: {time() - start_time} seconds")
    # 193.28611588478088 seconds - basic solution (AQE enabled)
    # 151.5258321762085 seconds (AQE disabled)
    # massive data skew!
    # 18.554410219192505 seconds with salting (salt = 10) - 10x perf increase
    # 19.44153594970703 seconds with salting (salt = 100)
    # 40.59525728225708 seconds with salting (salt = 1000)



if __name__ == '__main__':
    gaming_exc()
    sleep(9999)