
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import sleep
from pyspark.storagelevel import StorageLevel

spark = SparkSession \
    .builder \
    .master("local") \
    .config("spark.sql.warehouse.dir", "../spark-warehouse") \
    .appName("Caching and Checkpointing") \
    .getOrCreate()

sc = spark.sparkContext

def demo_caching():
    people_df = spark.read.option("sep", ":").csv("../data/people-1m/people-1m.txt") \
        .withColumnRenamed("_c6", "salary")

    # expensive operation
    highest_paid_df = people_df.orderBy(desc("salary")) # incurs a shuffle
    highest_paid_df.persist(
        # no argument = memory and disk, deserialized (uncompressed), 1x replicated
        # StorageLevel.MEMORY_AND_DISK_DESER # the same
        # StorageLevel.MEMORY_ONLY # store only in memory, and if memory is too small, evict from the cache
        # StorageLevel.DISK_ONLY # store on disk (slower than memory)
        # StorageLevel.OFF_HEAP # outside the JVM (saves executor memory, BUT very dangerous)
        # StorageLevel.MEMORY_ONLY_2 # in memory, 2x replication (uses 2x more memory, fault tolerance)
        # all flags except MEMORY_AND_DISK_DESER are SERIALIZED formats (compressed), use less memory, but use more CPU
    )  # stores the DF in memory (stores just the data we need)

    # when you use a DF multiple times, consider caching
    # incurs a penalty the first time, but the later access is very fast


    # further operations on highest_paid_df
    final_df = highest_paid_df.selectExpr("_c1 as first_name", "_c3 as last_name", "salary")
    final_df.show() # ~15s (including the persist)
    final_df.show() # 70ms

def demo_checkpoint():
    people_df = spark.read.option("sep", ":").csv("../data/people-1m/people-1m.txt") \
        .withColumnRenamed("_c1", "first_name") \
        .withColumnRenamed("_c3", "last_name") \
        .withColumnRenamed("_c6", "salary")

    highest_paid_df = people_df.orderBy(desc("salary"))

    sc.setCheckpointDir("../spark-warehouse/checkpoints")

    # checkpointing stores a DF to DISK only, forgets its query plan/lineage
    # used when an expensive DF can fail computation when reused
    highest_paid_df.limit(10).explain() # normal query plan
    checkpointed_df = highest_paid_df.checkpoint() # forgets its lineage
    checkpointed_df.limit(10).explain() # different query plan ???

    """
    == Physical Plan ==
    TakeOrderedAndProject(limit=10, orderBy=[salary#48 DESC NULLS LAST], output=[_c0#17,first_name#31,_c2#19,last_name#40,_c4#21,_c5#22,salary#48])
    +- *(1) Project [_c0#17, _c1#18 AS first_name#31, _c2#19, _c3#20 AS last_name#40, _c4#21, _c5#22, _c6#23 AS salary#48]
       +- FileScan csv [_c0#17,_c1#18,_c2#19,_c3#20,_c4#21,_c5#22,_c6#23] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<_c0:string,_c1:string,_c2:string,_c3:string,_c4:string,_c5:string,_c6:string>


    == Physical Plan ==
    CollectLimit 10
    +- *(1) Scan ExistingRDD[_c0#17,first_name#31,_c2#19,last_name#40,_c4#21,_c5#22,salary#48]
    """

if __name__ == '__main__':
    demo_checkpoint()