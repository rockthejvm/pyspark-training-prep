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


def demo_bucketing():
    large = spark.range(5000000).selectExpr("id * 5 as id").repartition(10)
    small = spark.range(10000).selectExpr("id * 3 as id").repartition(3)
    joined = large.join(small, "id")
    joined.explain()
    start_time = time()
    joined.show() # 1.8s
    print(f"Simple join: {time() - start_time} seconds")
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#2L]
       +- SortMergeJoin [id#2L], [id#6L], Inner
          :- Sort [id#2L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#2L, 200), ENSURE_REQUIREMENTS, [plan_id=33]
          :     +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=25]
          :        +- Project [(id#0L * 5) AS id#2L]
          :           +- Range (0, 5000000, step=1, splits=16)
          +- Sort [id#6L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#6L, 200), ENSURE_REQUIREMENTS, [plan_id=34]
                +- Exchange RoundRobinPartitioning(3), REPARTITION_BY_NUM, [plan_id=28]
                   +- Project [(id#4L * 3) AS id#6L]
                      +- Range (0, 10000, step=1, splits=16)
    """

    large.write.bucketBy(4, "id").sortBy("id").mode("overwrite").saveAsTable("bucketed_large")
    small.write.bucketBy(4, "id").sortBy("id").mode("overwrite").saveAsTable("bucketed_small")

    bucketed_large = spark.table("bucketed_large")
    bucketed_small = spark.table("bucketed_small")
    bucketed_join = bucketed_large.join(bucketed_small, "id")
    bucketed_join.explain()
    start_time = time()
    bucketed_join.show() # 0.3 seconds!
    print(f"Bucketed join: {time() - start_time} seconds")

    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#16L]
       +- SortMergeJoin [id#16L], [id#18L], Inner
          :- Sort [id#16L ASC NULLS FIRST], false, 0
          :  +- Filter isnotnull(id#16L)
          :     +- FileScan parquet spark_catalog.default.bucketed_large[id#16L] Batched: true, Bucketed: true, DataFilters: [isnotnull(id#16L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
          +- Sort [id#18L ASC NULLS FIRST], false, 0
             +- Filter isnotnull(id#18L)
                +- FileScan parquet spark_catalog.default.bucketed_small[id#18L] Batched: true, Bucketed: true, DataFilters: [isnotnull(id#18L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
    """

    # bucketBy + saveAsTable() into the Spark warehouse (persistent!)
    #   can use a finite number of buckets
    # partitionBy + save() into your regular persistent storage
    #   will create mini-directories for every unique value of your columns

def partitioned_storage():
    movies_df = spark.read.json("../data/movies")
    movies_df.write.partitionBy("Major_Genre", "Director").save("../data/movies-partitioned")

if __name__ == '__main__':
    pass