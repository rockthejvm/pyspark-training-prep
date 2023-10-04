from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import sleep
from pyspark.storagelevel import StorageLevel


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Caching & Checkpointing") \
    .getOrCreate()


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

if __name__ == '__main__':
    demo_caching()
    sleep(999999)