from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from time import time
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
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


def demo_bucketing():
    large = spark.range(1000000).select((col("id") * 5).alias("id"))
    small = spark.range(10000).select((col("id") * 3).alias("id"))
    joined = large.join(small, "id")
    joined.explain()
    start_time = time()
    joined.show()
    print(f"Time (normal join): {time() - start_time}s") # 1.5s

    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#2L]
       +- SortMergeJoin [id#2L], [id#6L], Inner
          :- Sort [id#2L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#2L, 200), ENSURE_REQUIREMENTS, [plan_id=25]
          :     +- Project [(id#0L * 5) AS id#2L]
          :        +- Range (0, 1000000, step=1, splits=16)
          +- Sort [id#6L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#6L, 200), ENSURE_REQUIREMENTS, [plan_id=26]
                +- Project [(id#4L * 3) AS id#6L]
                   +- Range (0, 10000, step=1, splits=16)
    """
    # store the data already correctly partitioned
    large.write\
        .bucketBy(4, "id")\
        .mode("overwrite")\
        .saveAsTable("bucketed_large") # warehouse (external e.g. HDFS, Hive, Hadoop, .... or can be local)

    small.write \
        .bucketBy(4, "id") \
        .mode("overwrite") \
        .saveAsTable("bucketed_small") # warehouse (external e.g. HDFS, Hive, Hadoop, .... or can be local)

    bucketed_large = spark.table("bucketed_large")
    bucketed_small = spark.table("bucketed_small")
    bucketed_join = bucketed_large.join(bucketed_small, "id")
    bucketed_join.explain()
    start_time = time()
    bucketed_join.show()
    print(f"Time (bucketed join): {time() - start_time}s") # 0.22s
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

if __name__ == '__main__':
    demo_bucketing()