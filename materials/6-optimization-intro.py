from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Spark Optimization") \
    .getOrCreate()

sc = spark.sparkContext

# comparing the Spark APIs
def compare_spark_apis():
    df_10m = spark.range(1, 10000000)
    rdd_10m = sc.parallelize(range(1, 10000000))
    print(df_10m.count())  # 1s
    print(rdd_10m.count())  # 0.8s
    print(df_10m.rdd.count())  # 8s
    print(spark.createDataFrame(rdd_10m, IntegerType()).count())  # 6s
    sleep(1000000)

# spark job anatomy (slides)

# [code] query plans
def demo_query_plans():
    simple_numbers = spark.range(1, 1000000)  # DF with column "id"
    numbers_times5 = simple_numbers.selectExpr("id * 5 as id")
    numbers_times5.explain()
    """
    == Physical Plan ==
    *(1) Project [(id#0L * 5) AS id#2L]
    +- *(1) Range (1, 1000000, step=1, splits=12)
    """

    more_numbers = spark.range(1, 1000000, 2)
    split7 = more_numbers.repartition(7)
    split7.explain()
    """
    == Physical Plan ==
    Exchange RoundRobinPartitioning(7), false, [id=#16]
    +- *(1) Range (1, 1000000, step=2, splits=12)
    """

    split7.selectExpr("id * 5 as id").explain()

    ds1 = spark.range(1, 1000000)
    ds2 = spark.range(1, 100000, 2)
    ds3 = ds1.repartition(7)
    ds4 = ds2.repartition(9)
    ds5 = ds3.selectExpr("id * 3 as id")
    joined = ds5.join(ds4, "id")
    sum_df = joined.selectExpr("sum(id)")
    sum_df.explain()
    """
      == Physical Plan ==
      *(7) HashAggregate(keys=[], functions=[sum(id#18L)])
      +- Exchange SinglePartition, true, [id=#99]
        +- *(6) HashAggregate(keys=[], functions=[partial_sum(id#18L)])
          +- *(6) Project [id#18L]
            +- *(6) SortMergeJoin [id#18L], [id#12L], Inner
              :- *(3) Sort [id#18L ASC NULLS FIRST], false, 0
              :  +- Exchange hashpartitioning(id#18L, 200), true, [id=#83]
              :     +- *(2) Project [(id#10L * 3) AS id#18L]
              :        +- Exchange RoundRobinPartitioning(7), false, [id=#79]
              :           +- *(1) Range (1, 10000000, step=1, splits=6)
              +- *(5) Sort [id#12L ASC NULLS FIRST], false, 0
                +- Exchange hashpartitioning(id#12L, 200), true, [id=#90]
                  +- Exchange RoundRobinPartitioning(9), false, [id=#89]
                    +- *(4) Range (1, 20000000, step=2, splits=6)
    """