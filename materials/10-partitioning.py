from pyspark.sql import SparkSession
from time import sleep

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Spark Optimization") \
    .config("spark.sql.warehouse.dir", "../spark-warehouse") \
    .getOrCreate()

sc = spark.sparkContext

############################################################
# demo with partition size
############################################################

def process_numbers(n_partitions):
    numbers = spark.range(100000000, numPartitions=n_partitions)  # 100M numbers, ~800MB size
    # computation I care about: a wide transformation e.g. a sum
    numbers.selectExpr("sum(id)").show()

def demo_partition_sizes():
    process_numbers(1)  # 800MB - 0.6s
    process_numbers(8)  # 100MB / partition - 0.5s
    process_numbers(80)  # 10MB / partition - 0.6s
    process_numbers(800)  # 1MB / partition - 2s
    process_numbers(8000)  # 100kB / partition - 9s
    process_numbers(80000)  # 10KB / partition - 100s
    sleep(5000)

"""
    How to estimate the size of your DF/RDD:
    Cache it, then look in the storage tab
        If the data is large, sample it and cache that instead

    (for the Scala/JavaAPI) - Look at the query plan, the object contains some additional data about the estimated size
"""

############################################################
# repartition and coalesce
############################################################

def demo_repartition_coalesce():
    numbers_rdd = sc.parallelize(range(1, 10000000))
    print(numbers_rdd.getNumPartitions())

    # repartitioning redistributes data evenly between partitions
    repartitioned_rdd = numbers_rdd.repartition(2)
    print(repartitioned_rdd.count())  # 4s

    # coalescing "stitches" partitions together
    coalesced_rdd = numbers_rdd.coalesce(2)
    print(coalesced_rdd.count())  # 0.4s

############################################################
# pre-partitioning
############################################################

"""
    Frame the following as an exercise.
    Write scenario 1, run it, then ask people to optimize it.
"""

def add_columns(df, n):
    new_columns = ["id * " + str(i) + " as newCol" + str(i) for i in range(n)]
    return df.selectExpr("id", *new_columns) # expand list to varargs

def demo_pre_partitioning():
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1") # deactivate broadcast joins
    initial_table = spark.range(1, 10000000).repartition(10) # RoundRobinPartitioning(10)
    narrow_table = spark.range(1, 5000000).repartition(7) # RoundRobinPartitioning(7)

    # scenario 1
    wide_table = add_columns(initial_table, 30)
    join1 = wide_table.join(narrow_table, "id")
    join1.explain()
    # print(join1.count()) # ~12s

    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, newCol0#8L, newCol1#9L, newCol2#10L, newCol3#11L, newCol4#12L, newCol5#13L, newCol6#14L, newCol7#15L, newCol8#16L, newCol9#17L, newCol10#18L, newCol11#19L, newCol12#20L, newCol13#21L, newCol14#22L, newCol15#23L, newCol16#24L, newCol17#25L, newCol18#26L, newCol19#27L, newCol20#28L, newCol21#29L, newCol22#30L, ... 7 more fields]
       +- SortMergeJoin [id#0L], [id#4L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=29]
          :     +- Project [id#0L, (id#0L * 0) AS newCol0#8L, (id#0L * 1) AS newCol1#9L, (id#0L * 2) AS newCol2#10L, (id#0L * 3) AS newCol3#11L, (id#0L * 4) AS newCol4#12L, (id#0L * 5) AS newCol5#13L, (id#0L * 6) AS newCol6#14L, (id#0L * 7) AS newCol7#15L, (id#0L * 8) AS newCol8#16L, (id#0L * 9) AS newCol9#17L, (id#0L * 10) AS newCol10#18L, (id#0L * 11) AS newCol11#19L, (id#0L * 12) AS newCol12#20L, (id#0L * 13) AS newCol13#21L, (id#0L * 14) AS newCol14#22L, (id#0L * 15) AS newCol15#23L, (id#0L * 16) AS newCol16#24L, (id#0L * 17) AS newCol17#25L, (id#0L * 18) AS newCol18#26L, (id#0L * 19) AS newCol19#27L, (id#0L * 20) AS newCol20#28L, (id#0L * 21) AS newCol21#29L, (id#0L * 22) AS newCol22#30L, ... 7 more fields]
          :        +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=21]
          :           +- Range (1, 10000000, step=1, splits=12)
          +- Sort [id#4L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#4L, 200), ENSURE_REQUIREMENTS, [plan_id=30]
                +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=24]
                   +- Range (1, 5000000, step=1, splits=12)
    """

    # scenario 2
    alt_narrow = narrow_table.repartition("id") # hash partitioning
    alt_initial = initial_table.repartition("id") # hash partitioning
    alt_wide = add_columns(alt_narrow, 30)
    join2 = alt_initial.join(alt_wide, "id")
    join2.explain()
    # print(join2.count()) # 6s

    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, newCol0#104L, newCol1#105L, newCol2#106L, newCol3#107L, newCol4#108L, newCol5#109L, newCol6#110L, newCol7#111L, newCol8#112L, newCol9#113L, newCol10#114L, newCol11#115L, newCol12#116L, newCol13#117L, newCol14#118L, newCol15#119L, newCol16#120L, newCol17#121L, newCol18#122L, newCol19#123L, newCol20#124L, newCol21#125L, newCol22#126L, ... 7 more fields]
       +- SortMergeJoin [id#0L], [id#4L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#0L, 200), REPARTITION_BY_COL, [plan_id=56]
          :     +- Range (1, 10000000, step=1, splits=12)
          +- Sort [id#4L ASC NULLS FIRST], false, 0
             +- Project [id#4L, (id#4L * 0) AS newCol0#104L, (id#4L * 1) AS newCol1#105L, (id#4L * 2) AS newCol2#106L, (id#4L * 3) AS newCol3#107L, (id#4L * 4) AS newCol4#108L, (id#4L * 5) AS newCol5#109L, (id#4L * 6) AS newCol6#110L, (id#4L * 7) AS newCol7#111L, (id#4L * 8) AS newCol8#112L, (id#4L * 9) AS newCol9#113L, (id#4L * 10) AS newCol10#114L, (id#4L * 11) AS newCol11#115L, (id#4L * 12) AS newCol12#116L, (id#4L * 13) AS newCol13#117L, (id#4L * 14) AS newCol14#118L, (id#4L * 15) AS newCol15#119L, (id#4L * 16) AS newCol16#120L, (id#4L * 17) AS newCol17#121L, (id#4L * 18) AS newCol18#122L, (id#4L * 19) AS newCol19#123L, (id#4L * 20) AS newCol20#124L, (id#4L * 21) AS newCol21#125L, (id#4L * 22) AS newCol22#126L, ... 7 more fields]
                +- Exchange hashpartitioning(id#4L, 200), REPARTITION_BY_COL, [plan_id=58]
                   +- Range (1, 5000000, step=1, splits=12)
   """

############################################################
# bucketing
############################################################

# lesson: how you write the DF matters a lot, because you get the DF pre-partitioned for the subsequent operations
def demo_bucketing():
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    large = spark.range(1000000).selectExpr("id * 5 as id").repartition(10)
    small = spark.range(10000).selectExpr("id * 3 as id").repartition(3)
    joined = large.join(small, "id")
    joined.explain()
    # joined.count() # ~2s
    """
        == Physical Plan ==
        AdaptiveSparkPlan isFinalPlan=false
        +- Project [id#2L]
           +- SortMergeJoin [id#2L], [id#6L], Inner
              :- Sort [id#2L ASC NULLS FIRST], false, 0
              :  +- Exchange hashpartitioning(id#2L, 200), ENSURE_REQUIREMENTS, [plan_id=33]
              :     +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=25]
              :        +- Project [(id#0L * 5) AS id#2L]
              :           +- Range (0, 1000000, step=1, splits=12)
              +- Sort [id#6L ASC NULLS FIRST], false, 0
                 +- Exchange hashpartitioning(id#6L, 200), ENSURE_REQUIREMENTS, [plan_id=34]
                    +- Exchange RoundRobinPartitioning(3), REPARTITION_BY_NUM, [plan_id=28]
                       +- Project [(id#4L * 3) AS id#6L]
                          +- Range (0, 10000, step=1, splits=12)
   """

    large.write \
        .bucketBy(4, "id") \
        .sortBy("id") \
        .mode("overwrite") \
        .saveAsTable("bucketed_large")

    small.write \
        .bucketBy(4, "id") \
        .sortBy("id") \
        .mode("overwrite") \
        .saveAsTable("bucketed_small")

    spark.sql("use default")
    bucketed_large = spark.table("bucketed_large")
    bucketed_small = spark.table("bucketed_small")
    bucketed_join = bucketed_large.join(bucketed_small, "id")
    bucketed_join.explain() # 0.6s, ignore the saveAsTable jobs
    bucketed_join.count()

    """
        == Physical Plan ==
        AdaptiveSparkPlan isFinalPlan=false
        +- Project [id#11L]
           +- SortMergeJoin [id#11L], [id#13L], Inner
              :- Sort [id#11L ASC NULLS FIRST], false, 0
              :  +- Filter isnotnull(id#11L)
              :     +- FileScan parquet default.bucketed_large[id#11L] Batched: true, DataFilters: [isnotnull(id#11L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/pyspark-training-prep/mate..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
              +- Sort [id#13L ASC NULLS FIRST], false, 0
                 +- Filter isnotnull(id#13L)
                    +- FileScan parquet default.bucketed_small[id#13L] Batched: true, DataFilters: [isnotnull(id#13L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/pyspark-training-prep/mate..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
    """