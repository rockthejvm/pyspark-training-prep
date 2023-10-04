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
    joined_df.show()  # ???

    # exercise: can you make it faster?

if __name__ == '__main__':
    demo_repartition_coalesce()
    sleep(999999)


