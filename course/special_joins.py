
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from time import sleep

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Special Joins") \
    .getOrCreate()


def demo_small_large_join():
    df_a = spark.range(100000000)
    df_b = spark.createDataFrame([Row(0, "zero"), Row(1, "first"), Row(2, "second"), Row(3, "third")]) \
        .withColumnRenamed("_1", "id")
    joined = df_a.join(df_b, "id")
    joined.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, _2#3]
       +- SortMergeJoin [id#0L], [id#6L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=26]
          :     +- Range (0, 100000000, step=1, splits=1)
          +- Sort [id#6L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#6L, 200), ENSURE_REQUIREMENTS, [plan_id=25]
                +- Project [_1#2L AS id#6L, _2#3]
                   +- Filter isnotnull(_1#2L)
                      +- Scan ExistingRDD[_1#2L,_2#3]
    """
    joined.show() # 36s!

# broadcast joins - copy the small DF across the entire cluster - no need to shuffle the big DF

def demo_broadcast_join():
    df_a = spark.range(100000000)
    df_b = spark.createDataFrame([Row(0, "zero"), Row(1, "first"), Row(2, "second"), Row(3, "third")]) \
        .withColumnRenamed("_1", "id")
    joined = df_a.join(broadcast(df_b), "id")
    joined.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, _2#3]
       +- BroadcastHashJoin [id#0L], [id#6L], Inner, BuildRight, false
          :- Range (0, 100000000, step=1, splits=1)
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [plan_id=24]
             +- Project [_1#2L AS id#6L, _2#3]
                +- Filter isnotnull(_1#2L)
                   +- Scan ExistingRDD[_1#2L,_2#3]
    """
    joined.show() # 4s!

def demo_auto_broadcast_join():
    df_a = spark.range(100000000)
    df_b = spark.read.option("header", "true").csv("../data/ranks.csv")
    joined = df_a.join(df_b, "id")
    joined.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, rank#20]
       +- BroadcastHashJoin [id#0L], [cast(id#19 as bigint)], Inner, BuildRight, false
          :- Range (0, 100000000, step=1, splits=1)
          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, string, false] as bigint)),false), [plan_id=41]
             +- Filter isnotnull(id#19)
                +- FileScan csv [id#19,rank#20] Batched: false, DataFilters: [isnotnull(id#19)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:string,rank:string>"""
    joined.show() # 1s!

if __name__ == '__main__':
    demo_auto_broadcast_join()
    sleep(100000)