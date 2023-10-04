from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import sleep

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Partitioning") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")\
    .getOrCreate()

sc = spark.sparkContext

def process_numbers(n_partitions):
    numbers = spark.range(250000000, numPartitions=n_partitions)  # ~1GB of data raw
    # computation I care about - a wide transformation
    numbers.selectExpr("sum(id)").show()

def demo_partition_sizes():
    process_numbers(1)  # 1GB/partition - 1s
    process_numbers(10)  # 100 MB/partition - 0.6s
    process_numbers(100)  # 10 MB/partition - 1.1s
    process_numbers(1000)  # 1 MB/partition - 5.5s
    process_numbers(10000)  # 100kB/partition - 50s
    process_numbers(100000)  # 10kB/partition - ????
    # lesson - optimal partition size 100MB - 1GB in raw memory

    # DS 24 TB, into 240000 parquet files/partitions
    # DS.select(3 columns / 7) => each partition is ~45% of the original partition = 450MB
    # DS.avg by department => 10000 departments => ...

    # one tool to estimate the size of your DF:
    # take a sample of your DF -> 0.01% of the data, cache it! -> determine the size of the DF


# repartition and coalesce
def demo_repartition_coalesce():
    numbers_df = spark.range(10000000)
    numbers_rdd = sc.parallelize(range(10000000))
    print(numbers_rdd.getNumPartitions())  # RDD has access to partition-level data

    # change the number of partitions - repartition - redistributes the data evenly
    repartitioned_rdd = numbers_rdd.repartition(2)
    print(repartitioned_rdd.count())  # 20s

    # coalesce "combines" partitions together
    coalesced_rdd = numbers_rdd.coalesce(2)  # reduces the number of partitions
    print(coalesced_rdd.count())  # 2s - 10x perf boost
    # repartition(n) == coalesce(n, shuffle=true)!


def add_columns(df, n):
    new_columns = ["id * " + str(i) + " as newCol_" + str(i) for i in range(n)]
    return df.selectExpr("id", *new_columns)

def partitioning_exercise():
    # can't touch this
    initial_table = spark.range(1, 10000000).repartition(10)  # someone gives me this data
    another_table = spark.range(1, 5000000).repartition(7)  # someone gives me this data

    # can touch this
    wide_table = add_columns(initial_table, 30)  # assume this is actual business need
    joined_df = wide_table.join(another_table, "id")

    # exercise: can you make it faster?

    # idea 1 (Patrik) - join before the add_columns, that should (in theory) reduce the size of the shuffle
    joined_df.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, newCol_0#8L, newCol_1#9L, newCol_2#10L, newCol_3#11L, newCol_4#12L, newCol_5#13L, newCol_6#14L, newCol_7#15L, newCol_8#16L, newCol_9#17L, newCol_10#18L, newCol_11#19L, newCol_12#20L, newCol_13#21L, newCol_14#22L, newCol_15#23L, newCol_16#24L, newCol_17#25L, newCol_18#26L, newCol_19#27L, newCol_20#28L, newCol_21#29L, newCol_22#30L, ... 7 more fields]
       +- SortMergeJoin [id#0L], [id#4L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=29] <------ this shuffle can work on a data 30 times smaller
          :     +- Project [id#0L, (id#0L * 0) AS newCol_0#8L, (id#0L * 1) AS newCol_1#9L, (id#0L * 2) AS newCol_2#10L, (id#0L * 3) AS newCol_3#11L, (id#0L * 4) AS newCol_4#12L, (id#0L * 5) AS newCol_5#13L, (id#0L * 6) AS newCol_6#14L, (id#0L * 7) AS newCol_7#15L, (id#0L * 8) AS newCol_8#16L, (id#0L * 9) AS newCol_9#17L, (id#0L * 10) AS newCol_10#18L, (id#0L * 11) AS newCol_11#19L, (id#0L * 12) AS newCol_12#20L, (id#0L * 13) AS newCol_13#21L, (id#0L * 14) AS newCol_14#22L, (id#0L * 15) AS newCol_15#23L, (id#0L * 16) AS newCol_16#24L, (id#0L * 17) AS newCol_17#25L, (id#0L * 18) AS newCol_18#26L, (id#0L * 19) AS newCol_19#27L, (id#0L * 20) AS newCol_20#28L, (id#0L * 21) AS newCol_21#29L, (id#0L * 22) AS newCol_22#30L, ... 7 more fields]
          :        +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=21]
          :           +- Range (1, 10000000, step=1, splits=12)
          +- Sort [id#4L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#4L, 200), ENSURE_REQUIREMENTS, [plan_id=30]
                +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=24]
                   +- Range (1, 5000000, step=1, splits=12)
    """
    # joined_df.show() # 24s

    joined_patrik = initial_table.join(another_table, "id")
    result_patrik = add_columns(joined_patrik, 30)
    result_patrik.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, (id#0L * 0) AS newCol_0#101L, (id#0L * 1) AS newCol_1#102L, (id#0L * 2) AS newCol_2#103L, (id#0L * 3) AS newCol_3#104L, (id#0L * 4) AS newCol_4#105L, (id#0L * 5) AS newCol_5#106L, (id#0L * 6) AS newCol_6#107L, (id#0L * 7) AS newCol_7#108L, (id#0L * 8) AS newCol_8#109L, (id#0L * 9) AS newCol_9#110L, (id#0L * 10) AS newCol_10#111L, (id#0L * 11) AS newCol_11#112L, (id#0L * 12) AS newCol_12#113L, (id#0L * 13) AS newCol_13#114L, (id#0L * 14) AS newCol_14#115L, (id#0L * 15) AS newCol_15#116L, (id#0L * 16) AS newCol_16#117L, (id#0L * 17) AS newCol_17#118L, (id#0L * 18) AS newCol_18#119L, (id#0L * 19) AS newCol_19#120L, (id#0L * 20) AS newCol_20#121L, (id#0L * 21) AS newCol_21#122L, (id#0L * 22) AS newCol_22#123L, ... 7 more fields]
       +- SortMergeJoin [id#0L], [id#4L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=60]
          :     +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=53]
          :        +- Range (1, 10000000, step=1, splits=12)
          +- Sort [id#4L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#4L, 200), ENSURE_REQUIREMENTS, [plan_id=61]
                +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=55]
                   +- Range (1, 5000000, step=1, splits=12)
    """
    # result_patrik.show()  # 14s - ~2x perf!

    table_a_aneta = initial_table.repartition("id")
    table_b_aneta = another_table.repartition("id")
    joined_aneta = table_a_aneta.join(table_b_aneta, "id")
    result_aneta = add_columns(joined_aneta, 30)
    result_aneta.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, (id#0L * 0) AS newCol_0#167L, (id#0L * 1) AS newCol_1#168L, (id#0L * 2) AS newCol_2#169L, (id#0L * 3) AS newCol_3#170L, (id#0L * 4) AS newCol_4#171L, (id#0L * 5) AS newCol_5#172L, (id#0L * 6) AS newCol_6#173L, (id#0L * 7) AS newCol_7#174L, (id#0L * 8) AS newCol_8#175L, (id#0L * 9) AS newCol_9#176L, (id#0L * 10) AS newCol_10#177L, (id#0L * 11) AS newCol_11#178L, (id#0L * 12) AS newCol_12#179L, (id#0L * 13) AS newCol_13#180L, (id#0L * 14) AS newCol_14#181L, (id#0L * 15) AS newCol_15#182L, (id#0L * 16) AS newCol_16#183L, (id#0L * 17) AS newCol_17#184L, (id#0L * 18) AS newCol_18#185L, (id#0L * 19) AS newCol_19#186L, (id#0L * 20) AS newCol_20#187L, (id#0L * 21) AS newCol_21#188L, (id#0L * 22) AS newCol_22#189L, ... 7 more fields]
       +- SortMergeJoin [id#0L], [id#4L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#0L, 200), REPARTITION_BY_COL, [plan_id=84]
          :     +- Range (1, 10000000, step=1, splits=12)
          +- Sort [id#4L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#4L, 200), REPARTITION_BY_COL, [plan_id=86]
                +- Range (1, 5000000, step=1, splits=12)
    """
    result_aneta.show()  # 14s (on my poor computer) - should run at least as fast as the previous one.




if __name__ == '__main__':
    partitioning_exercise()
    sleep(999999)


