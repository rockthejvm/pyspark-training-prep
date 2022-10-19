from pyspark.sql import SparkSession
from pyspark.storagelevel import *
from time import sleep
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Spark Optimization") \
    .config("spark.sql.warehouse.dir", "../spark-warehouse") \
    .getOrCreate()
# ^^ add the warehouse dir when checkpointing

sc = spark.sparkContext

# memory architecture
# [code] caching & checkpointing
def demo_caching():
    people_df = spark.read \
        .option("inferSchema", "true") \
        .option("sep", ":") \
        .option("mode", "dropMalformed") \
        .csv("../data/people-1m") \
        .withColumnRenamed("_c6", "salary") \

    # simulate an "expensive" operation
    highest_paid_df = people_df.orderBy(desc("salary"))

    # scenario: use this DF multiple times
    highest_paid_df.persist(
        # no argument = MEMORY_AND_DISK
        StorageLevel.MEMORY_ONLY # cache the DF in memory EXACTLY - CPU efficient, memory expensive
        # StorageLevel.DISK_ONLY # cache the DF to DISK - CPU efficient and mem efficient, but slower
        # StorageLevel.MEMORY_AND_DISK # cache this DF to both the heap AND the disk - first caches to memory, but if the DF is EVICTED, will be written to disk

        # replication:
        # StorageLevel.MEMORY_ONLY_2 # memory only, replicated twice - for resiliency, 2x memory usage
        # same _2 for DISK_ONLY, MEMORY_AND_DISK

        # off-heap
        # StorageLevel.OFF_HEAP # cache outside the JVM, done with Tungsten, still stored on the machine RAM, needs to be configured, CPU efficient and memory efficient
    )

    final_df = highest_paid_df.selectExpr("_c1 as firstName", "_c3 as lastName", "salary")
    final_df.show(5, False)  # 2s
    sleep(10)
    final_df.show(5, False)  # 13ms (without caching it's 300ms, but only show them if they ask)

    # remove from cache
    # highest_paid_df.unpersist() # remove this DF from cache

    # # change cache name
    # highest_paid_df.createOrReplaceTempView("highestPaid")
    # spark.catalog.cacheTable("highestPaid")
    # final_df.count()
    #
    # # RDDs - same concept
    # highest_paid_rdd = ordered_cars_df.rdd
    # highest_paid_rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    # highest_paid_rdd.count()


def demo_checkpoint():
    people_df = spark.read \
        .option("inferSchema", "true") \
        .option("sep", ":") \
        .option("mode", "dropMalformed") \
        .csv("../data/people-1m") \
        .withColumnRenamed("_c6", "salary") \
        .withColumnRenamed("_c1", "firstName") \
        .withColumnRenamed("_c3", "lastName")

    # simulate an "expensive" operation
    highest_paid_df = people_df.orderBy(desc("salary"))

    # checkpointing = storing a RDD/DF to disk
    # used to avoid failure
    # needs to be configured
    sc.setCheckpointDir("../checkpoints")

    # checkpointed RDD/DF is fetched directly from disk
    # all upstream dependencies are pruned
    highest_paid_df.selectExpr("firstName", "lastName", "salary").limit(10).explain()
    # query plans are different
    checkpointed_df = highest_paid_df.checkpoint()
    checkpointed_df.selectExpr("firstName", "lastName", "salary").limit(10).explain()

