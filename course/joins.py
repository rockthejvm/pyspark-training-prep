from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import *
from time import time
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("JoinOpt") \
    .getOrCreate()

def demo_large_small_join():
    dfa = spark.range(1, 100000000)
    dfb = spark.createDataFrame(
        [
            Row(id=1, medal="first"),
            Row(id=2, medal="second"),
            Row(id=3, medal="third")
        ]
    )
    medalists = dfa.join(dfb, "id")
    medalists.explain()
    start_time = time()
    medalists.show()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, medal#3]
       +- SortMergeJoin [id#0L], [id#2L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=22]
          :     +- Range (1, 100000000, step=1, splits=16)
          +- Sort [id#2L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#2L, 200), ENSURE_REQUIREMENTS, [plan_id=21]
                +- Filter isnotnull(id#2L)
                   +- Scan ExistingRDD[id#2L,medal#3]
    """
    print(f"Time taken large-small join: {time() - start_time} seconds")
    # 5s (Macbook M3, 100M rows)

def demo_broadcast_join():
    dfa = spark.range(1, 100000000)
    dfb = spark.createDataFrame(
        [
            Row(id=1, medal="first"),
            Row(id=2, medal="second"),
            Row(id=3, medal="third")
        ]
    )
    medalists = dfa.join(broadcast(dfb), "id")
    medalists.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#17L, medal#20]
       +- BroadcastHashJoin [id#17L], [id#19L], Inner, BuildRight, false
          :- Range (1, 100000000, step=1, splits=16)
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=156]
             +- Filter isnotnull(id#19L)
                +- Scan ExistingRDD[id#19L,medal#20]
    """
    start_time = time()
    medalists.show()
    print(f"Time taken broadcast join: {time() - start_time} seconds")
    # 0.14s (Macbook M3, 100M rows)

# joining a large table with a "lookup" table for tagging etc.
# Spark can auto-broadcast a table < 10MB
# config spark.sql.autoBroadcastJoinThreshold
# 100MB, MAYBE 1GB (raw data) for broadcasting - careful with the size!
# how to tell the size of the DF: cache a slice/sample (1%) the DF, extrapolate

if __name__ == '__main__':
    demo_large_small_join()
    demo_broadcast_join()