from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import sleep
from pyspark.storagelevel import StorageLevel


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "../warehouse") \
    .appName("Caching & Checkpointing") \
    .getOrCreate()


sc = spark.sparkContext

def demo_caching():
    people_df = spark.read \
        .option("sep", ":") \
        .option("header", "false") \
        .csv("../data/people-1m") \
        .withColumnRenamed("_c6", "salary")

    # simulate something expensive
    highest_paid_df = people_df.orderBy(desc("salary"))

    highest_paid_df.createOrReplaceTempView("highest_paid")  # set the name/identifier of this DF -> you'll see this in the storage tab in the Spark UI

    # cache whatever is expensive
    highest_paid_df.persist(
        # no argument = MEMORY_AND_DISK_DESER
        StorageLevel.MEMORY_ONLY  # memory only SERIALIZED (saves up memory, uses more CPU)
        # StorageLevel.MEMORY_ONLY_2  # memory only 2x replicated - used for fault tolerance in case an executor fails
        # StorageLevel.DISK_ONLY  # only on disk, not on ram (saves up memory, same speed as writing/reading from disk)
        # StorageLevel.DISK_ONLY_2 -> for fault tolerance
        # StorageLevel.DISK_ONLY_3
        # StorageLevel.MEMORY_AND_DISK_2 -> memory and disk with 2x replication (fault tolerance)
        # StorageLevel.OFF_HEAP -> stores the data outside the JVM (saves up all the executor memory AND stores the DF in RAM) but very dangerous
    )

    # highest_paid_df.cache() # same as persist()
    # highest_paid_df.unpersist()  # removes it from cache()

    # something simple compared to the other transformation
    final_df = highest_paid_df.select(col("_c1").alias("firstName"), col("_c3").alias("lastName"), col("salary"))
    final_df.show() # 2.5s + time it takes for caching
    sleep(10)
    final_df.show() # 0.5s after caching


# checkpointing
def demo_checkpoint():
    people_df = spark.read \
        .option("sep", ":") \
        .option("header", "false") \
        .csv("../data/people-1m") \
        .withColumnRenamed("_c1", "firstName") \
        .withColumnRenamed("_c3", "lastName") \
        .withColumnRenamed("_c6", "salary")

    # simulate something expensive
    highest_paid_df = people_df.orderBy(desc("salary"))
    highest_paid_df.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Sort [salary#48 DESC NULLS LAST], true, 0
       +- Exchange rangepartitioning(salary#48 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=29]
          +- Project [_c0#17, _c1#18 AS firstName#31, _c2#19, _c3#20 AS lastName#40, _c4#21, _c5#22, _c6#23 AS salary#48]
             +- FileScan csv [_c0#17,_c1#18,_c2#19,_c3#20,_c4#21,_c5#22,_c6#23] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<_c0:string,_c1:string,_c2:string,_c3:string,_c4:string,_c5:string,_c6:string>
    """

    # checkpointing saves to disk only
    sc.setCheckpointDir("../warehouse/checkpoints")
    checkpointed_df = highest_paid_df.checkpoint()  # writes the data into a warehouse checkpoint dir
    # after that, the DF forgets its lineage

    # DF is not reused, it's reread from disk
    checkpointed_df.explain()
    checkpointed_df.selectExpr("firstName", "lastName", "salary * 12").explain()

    # the DF can be "reused" from another job
    # checkpointing does NOT help with performance, it will help with potential failures

    # the job goes slow? use caching
    # the job risks failing? use checkpointing

if __name__ == '__main__':
    demo_checkpoint()
    sleep(999999)