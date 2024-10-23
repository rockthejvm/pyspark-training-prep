from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os, sys
from time import time, sleep

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .appName("Partitioning Problems") \
    .getOrCreate()

def process_numbers(n_partitions):
    numbers = spark.range(250 * 10**6, numPartitions=n_partitions) # 1GB over n_partitions chunks
    start_time = time()
    numbers.selectExpr("sum(id)").show() # force a wide transformation
    print(f"Time for {n_partitions}: {time() - start_time}")

# optimal - 100MB per partition
def demo_partition_sizes():
    process_numbers(1)          # 1GB/partition - 1s
    process_numbers(10)         # 100MB/partition - 0.24
    process_numbers(100)        # 10MB/partition - 0.25
    process_numbers(1000)       # 1MB/partition - 0.66
    process_numbers(10000)      # 100kB/partition - 4.7
    process_numbers(100000)     # 10kB/partition - ???


# how to determine the size of your data
# sample 1% of your data, cache it - read the memory used
# snappy parquet = 10x compression vs raw part-0000-62783467-3627.snappy.parquet
# use Spark Reporter!

# tips: use coalesce most of the time, if the data is (roughly) evenly distributed
# how to tell: the largest partition should be <1 order of magnitude bigger than the smallest file
# repartition & coalesce do worse if you're joining/grouping the data later
#   because that will repartition the data again (by the column(s) you want to join
def repartition_coalesce():
    df = spark.range(10000000) # splits = n cores on the machine

    # repartitions by a column => all rows with the same value stay on the same partition
    repartitioned_by_mod_df = df.withColumn("mod", col("id") % 5).repartition("mod")

    repartitioned_df = df.repartition(2) # repartitions into 2 partitions
    repartitioned_df.show()

    coalesced_df = df.coalesce(2) # repartitions into 2 partitions
    coalesced_df.show()

def add_columns(df, n):
    new_columns = [f"id * {i} as newCol_{i}" for i in range(n)]
    return df.selectExpr("id", *new_columns)

def exercise():
    table = spark.range(1, 10000000).repartition(10) # given
    narrow_table = spark.range(1, 5000000).repartition(7) # given

    # the code that you can edit
    wide_table = add_columns(table, 30)
    join1 = wide_table.join(narrow_table, "id")
    start_time = time()
    join1.explain()
    join1.show() # 5 seconds
    print(f"Regular job: {time() - start_time} seconds")

    # can you optimize it?
    # Akos - broadcasting
    wide_table = add_columns(table, 30)
    join2 = wide_table.join(broadcast(narrow_table), "id")
    start_time = time()
    join2.show()  # 2x perf
    print(f"Broadcast job: {time() - start_time} seconds")

    # Udo solution
    # lesson: partition EARLY by the right partitioner before your big computations!
    start_time = time()
    table = table.repartition(col("id"))
    narrow_table = narrow_table.repartition(col("id"))

    # happens AFTER the repartition
    wide_table = add_columns(table, 30)

    join3 = wide_table.join(narrow_table, "id")
    join3.explain()
    join3.show() # 0.5s!
    print(f"time: {time() - start_time} seconds")



if __name__ == '__main__':
    exercise()
    sleep(9999)