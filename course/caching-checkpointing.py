from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.storagelevel import *
from time import sleep, time
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.memory.offHeap.enable", "false") \
    .appName("Caching and Checkpointing") \
    .getOrCreate()

def demo_caching():
    people_df = spark.read\
        .option("sep", ":") \
        .csv("../data/people-1m") \
        .withColumnRenamed("_c0", "id") \
        .withColumnRenamed("_c6", "salary")

    highest_paid_df = people_df.orderBy(col("salary").desc()) # "expensive" computation
    # if this is used in multiple places, good idea to CACHE it

    highest_paid_df.cache()

    # cache this here - use the persist function to set WHERE the DF will be stored
    highest_paid_df.persist(
        # StorageLevel.NONE - no caching
        # StorageLevel.MEMORY_ONLY - only in RAM, serialized (compacted, more CPU intensive) + if partition is evicted must be recomputed
        # StorageLevel.MEMORY_ONLY_2 - same, 2x replication
        # StorageLevel.MEMORY_ONLY_3 - same, 3x replication
        # StorageLevel.DISK_ONLY - stores only on disk (saves RAM, far slower)
        # StorageLevel.DISK_ONLY_2, DISK_ONLY_3 - same, 2x/3x replication
        # StorageLevel.MEMORY_AND_DISK, _2 - store in RAM first (serialized = compressed, more CPU intensive), if evicted move to disk
        # StorageLevel.OFF_HEAP - outside the JVM, can save JVM space, but more dangerous (can crash your executor)

        StorageLevel.MEMORY_ONLY
        # default: MEMORY_AND_DISK_DESER
    )

    # perform some transformations
    final_df = highest_paid_df.selectExpr("id", "_c1 as firstName", "_c3 as lastName", "salary * 10")
    start_time = time()
    final_df.explain()
    final_df.show() # 2s
    print(f"First job time: {time() - start_time} seconds")
    start_time = time()
    final_df.show() # much faster here - 40ms
    print(f"Second job time: {time() - start_time} seconds")

    highest_paid_df.unpersist() # remove something from cache

    # cache the DF under a human-readable name
    highest_paid_df.createOrReplaceTempView("highest_paid")
    spark.catalog.cacheTable("highest_paid")
    highest_paid_df.show()


def demo_offheap():
    def run_computes(df):
        result1 = df.groupBy(col("id") % 3).count()
        result1.show()
        result2 = df.agg(max(col("id")))
        result2.show()
        result3 = df.agg(sum(col("id")))
        result3.show()

    num_records = 10000000
    df = spark.range(num_records)

    # time with normal cache
    df.cache() # == df.persist() == df.persist(MEMORY_AND_DISK_DESER)
    start_time = time()
    run_computes(df)
    print(f"Time for computes on standard cache: {time() - start_time} seconds")
    spark.stop()

    spark_off_heap = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.memory.offHeap.enable", "true") \
        .config("spark.memory.offHeap.size", "1g") \
        .appName("Caching and Checkpointing OffHeap") \
        .getOrCreate()

    df_off = spark_off_heap.range(num_records)
    df_off.persist(StorageLevel.OFF_HEAP)
    start_time = time()
    run_computes(df_off)
    print(f"Time for computes on off-heap cache: {time() - start_time} seconds")

def demo_checkpoint():
    people_df = spark.read \
        .option("sep", ":") \
        .csv("../data/people-1m") \
        .withColumnRenamed("_c0", "id") \
        .withColumnRenamed("_c6", "salary")

    spark.sparkContext.setCheckpointDir("../checkpoints")

    highest_paid_df = people_df.orderBy(col("salary").desc()) # "expensive" computation
    final_df = highest_paid_df.selectExpr("id", "_c1 as firstName", "_c3 as lastName", "salary * 10")
    final_df.explain()
    checkpointed_df = highest_paid_df.checkpoint() # save a DF to disk for reuse
    # all the dependencies (query plan) are pruned

    final_df = checkpointed_df.selectExpr("id", "_c1 as firstName", "_c3 as lastName", "salary * 10")
    final_df.explain()

if __name__ == '__main__':
    demo_checkpoint()
    sleep(99999)
