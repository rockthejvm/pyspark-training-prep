from pyspark.sql import SparkSession
from time import time
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Partitioning") \
    .getOrCreate()

def process_items(n_partitions):
    start_time = time()
    # 1GB of data
    numbers = spark.range(250 * 10**6, numPartitions=n_partitions)
    # run a "heavy" compute
    numbers.selectExpr("sum(id)").show()
    print(f"Time taken ({n_partitions} partitions): {time() - start_time} s")

def demo_partition_sizes():
    process_items(1)        # 1GB/partition     2s
    process_items(10)       # 100MB/partition   0.2s
    process_items(100)      # 10MB/partition    0.2s
    process_items(1000)     # 1MB/partition     0.6s
    process_items(10000)    # 100kB/partition   4.3s
    process_items(100000)   # 10kB/partition    1m2s
    # optimal partition size ~100MB (maybe more up to 1GB) raw data

    # estimate the size of your data - cache a sample and extrapolate
    # parquet 10x compressed vs raw data

def demo_repartition_coalesce():
    numbers = spark.range(10000000) # splits = 16 (n of cores on Daniel's machine)

    # repartitioning redistributes the data EVENLY between partitions
    repartitioned_df = numbers.repartition(2)
    start_time = time()
    repartitioned_df.show()
    print(f"Time (repartitioning): {time() - start_time}s")

    # coalescing "stitches" partitions together (not necessarily evenly)
    coalesced_df = numbers.coalesce(2) # 1.7s
    start_time = time()
    coalesced_df.show()
    print(f"Time (coalescing): {time() - start_time}s") # 0.1s


if __name__ == '__main__':
    demo_repartition_coalesce()