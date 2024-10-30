from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.storagelevel import *

from time import time, sleep
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "100m")\
    .appName("CachingCheckpointing") \
    .getOrCreate()

def demo_caching():
    people_df = spark.read.option("sep", ":").csv("../data/people-1m") \
        .withColumnRenamed("_c6", "salary")

    # expensive DF worth saving to speed up later computations
    highest_paid_df = people_df.orderBy(col("salary").desc())

    # cache this expensive DF
    # highest_paid_df.cache() #  === persist()
    # cache the DF by a table name
    # highest_paid_df.createOrReplaceTempView("highest_paid_df")
    # spark.catalog.cacheTable("highest_paid_df")

    # the most general form of caching -> persist()
    highest_paid_df.persist(
        # in the JVM:
        # default - MEMORY_AND_DISK_DESER
        # StorageLevel.MEMORY_AND_DISK_DESER # stores in RAM (raw data), dumps to disk if not enough
        # StorageLevel.MEMORY_ONLY # stores in RAM (compressed data), has to recompute if the partitions are evicted
        # StorageLevel.MEMORY_ONLY_2 # stores in RAM (compressed), 2x replication
        # StorageLevel.DISK_ONLY # caches only on disk
        # StorageLevel.DISK_ONLY_2 # adds 2x replication (3x replication possible)
        # StorageLevel.MEMORY_AND_DISK # in memory (compressed), dumps to disk if not enough
        # StorageLevel.MEMORY_AND_DISK_2 # adds 2x replication
        # outside the JVM:
        StorageLevel.OFF_HEAP # in memory, outside the JVM, serialized (Tungsten), must be enabled and configured, dangerous (can crash the cluster node)
    )

    final_df = highest_paid_df.select(col("_c1").alias("first_name"), col("_c3").alias("last_name"), col("salary"))
    start_time = time()
    final_df.show()
    print(f"Time (first eval) {time() - start_time} s") # 2.2206380367279053 s
    start_time = time()
    final_df.show()
    print(f"Time (second eval) {time() - start_time} s") # 0.03325319290161133 s

def demo_checkpoint():
    people_df = spark.read.option("sep", ":").csv("../data/people-1m") \
        .withColumnRenamed("_c6", "salary") \
        .withColumnRenamed("_c1", "first_name") \
        .withColumnRenamed("_c3", "last_name") \

    # expensive DF worth saving to reduce the chance of further computations FAILING
    highest_paid_df = people_df.orderBy(col("salary").desc())

    # must set checkpoint dir to store checkpointed DFs
    spark.sparkContext.setCheckpointDir("checkpoints") # can store this outside the Spark cluster (HDFS, Hive...)

    # store the data on disk at that location
    checkpointed_df = highest_paid_df.checkpoint() # new DF will have a DIFFERENT query plan

    highest_paid_df.explain() # query plan is the original
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=true
    +- == Final Plan ==
       *(2) Sort [salary#31 DESC NULLS LAST], true, 0
       +- AQEShuffleRead coalesced
          +- ShuffleQueryStage 0
             +- Exchange rangepartitioning(salary#31 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=39]
                +- *(1) Project [_c0#17, _c1#18 AS first_name#40, _c2#19, _c3#20 AS last_name#48, _c4#21, _c5#22, _c6#23 AS salary#31]
                   +- FileScan csv [_c0#17,_c1#18,_c2#19,_c3#20,_c4#21,_c5#22,_c6#23] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<_c0:string,_c1:string,_c2:string,_c3:string,_c4:string,_c5:string,_c6:string>
    """
    checkpointed_df.select("first_name", "last_name", "salary").explain() # query plan was TRUNCATED
    """
    == Physical Plan ==
    *(1) Project [first_name#40, last_name#48, salary#31]
    +- *(1) Scan ExistingRDD[_c0#17,first_name#40,_c2#19,last_name#48,_c4#21,_c5#22,salary#31]
    ## everything below was truncated
    """

if __name__ == '__main__':
    demo_checkpoint()