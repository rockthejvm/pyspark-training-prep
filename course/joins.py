from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from time import sleep

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "false") \
    .appName("Joins") \
    .getOrCreate()


def demo_large_small_join():
    numbers = spark.range(1, 100000000)
    lookup_table = spark.createDataFrame([Row(147329, "gold"), Row(45631298, "silver"), Row(37, "bronze")]) \
        .withColumnRenamed("_1", "id")
    joined = numbers.join(lookup_table, "id")
    joined.explain()
    joined.show() # 52s!
    """
    == Physical Plan ==
    *(5) Project [id#0L, _2#3]
    +- *(5) SortMergeJoin [id#0L], [id#6L], Inner
       :- *(2) Sort [id#0L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=31]
       :     +- *(1) Range (1, 100000000, step=1, splits=12)
       +- *(4) Sort [id#6L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(id#6L, 200), ENSURE_REQUIREMENTS, [plan_id=37]
             +- *(3) Project [_1#2L AS id#6L, _2#3]
                +- *(3) Filter isnotnull(_1#2L)
                   +- *(3) Scan ExistingRDD[_1#2L,_2#3]
    """

def demo_broadcast_join():
    numbers = spark.range(1, 100000000)
    lookup_table = spark.createDataFrame([Row(147329, "gold"), Row(45631298, "silver"), Row(37, "bronze")]) \
        .withColumnRenamed("_1", "id")
    joined = numbers.join(broadcast(lookup_table), "id")
    joined.explain()
    joined.show() # 5s (10x perf!)
    """
    == Physical Plan ==
    *(2) Project [id#0L, _2#3]
    +- *(2) BroadcastHashJoin [id#0L], [id#6L], Inner, BuildRight, false
       :- *(2) Range (1, 100000000, step=1, splits=12)
       +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [plan_id=27]
          +- *(1) Project [_1#2L AS id#6L, _2#3]
             +- *(1) Filter isnotnull(_1#2L)
                +- *(1) Scan ExistingRDD[_1#2L,_2#3]
    """

# read data from files -> spark will estimate the data size
# spark.sql.autoBroadcastJoinThreshold = 10MB
# 100MB broadcast _usually_ fine

if __name__ == '__main__':
    demo_broadcast_join()
    sleep(999999)
