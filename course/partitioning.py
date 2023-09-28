
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from time import sleep

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Partitioning") \
    .getOrCreate()

sc = spark.sparkContext

def process_numbers(n_partitions):
    numbers = spark.range(300000000, numPartitions=n_partitions)
    # simulate a computation: the sum of all elements
    numbers.select(sum("id")).show()


def demo_partition_sizes():
    process_numbers(1)  # 2.5GB/partition - 0.7s
    process_numbers(2)  # ~1GB/partition - 0.3s - best!
    process_numbers(20)  # ~100MB/partition - 0.7s
    process_numbers(200)  # ~10MB/partition - 2s
    process_numbers(2000)  # ~1MB/partition - 15s
    process_numbers(20000)  # ~100kB/partition - 3mins!
    sleep(99999999)

# optimal partition size ~1GB/partition (in RAM)!
# (in snappy parquet that's 100MB/file, for ~10x compression)

# BUT - need to take into account cluster distribution
# DF 2GB total
# optimal distribution = 2 partitions (1GB/partition)
# 10 executors, 0.5GB for computation
# split in 10 partitions => 200 MB/partition

"""
    How to estimate the size of your DF
    - go to the dataset details - look at file sizes (snappy.parquet should be ~100MB each)
    - cache the DF, look at the storage tab
    - (needs the Scala API) - look at the query plan internal API (rarely used)
"""


def demo_repartition_coalesce():
    numbers_rdd = sc.parallelize(range(1, 10000000)) # 80 mb
    print(numbers_rdd.getNumPartitions()) # initially 12 partitions (Daniel's computer)

    repartitioned_rdd = numbers_rdd.repartition(2) # 40mb/partition
    print(repartitioned_rdd.count()) # 17s

    coalesced_rdd = numbers_rdd.coalesce(2) # 40mb/partition (avg)
    print(coalesced_rdd.count()) # 2s! (almost 10x perf!)


if __name__ == '__main__':
    demo_repartition_coalesce()
    sleep(99999999)