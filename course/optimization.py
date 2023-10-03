from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import sleep

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Optimization") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

sc = spark.sparkContext

def compare_spark_apis():
    df_10m = spark.range(1, 10000000)  # DF with a column called "id"
    rdd_10m = sc.parallelize(range(10000000))  # RDD of ints
    print(df_10m.count())  # 0.8s
    print(rdd_10m.count())  # 0.9s
    print(df_10m.rdd.count())  # 9s
    print(spark.createDataFrame(rdd_10m, IntegerType()).count())  # 6s


def demo_query_plans():
    simple_numbers = spark.range(1, 1000000)
    numbers_x5 = simple_numbers\
        .select(col("id"), (col("id") * 5).alias("x5"))\
        .filter(col("id") < 1000)
    """
    == Physical Plan ==
    *(1) Project [(id#0L * 5) AS x5#2L]
    +- *(1) Range (1, 1000000, step=1, splits=12)
    """
    numbers_x5.explain()
    # predicate pushdown

    """
    == Parsed Logical Plan ==
    'Filter ('id < 1000)
    +- Project [id#0L, (id#0L * cast(5 as bigint)) AS x5#2L]
       +- Range (1, 1000000, step=1, splits=Some(12))
    
    == Analyzed Logical Plan ==
    id: bigint, x5: bigint
    Filter (id#0L < cast(1000 as bigint))
    +- Project [id#0L, (id#0L * cast(5 as bigint)) AS x5#2L]
       +- Range (1, 1000000, step=1, splits=Some(12))
    
    == Optimized Logical Plan ==
    Project [id#0L, (id#0L * 5) AS x5#2L]
    +- Filter (id#0L < 1000)
       +- Range (1, 1000000, step=1, splits=Some(12))
    
    == Physical Plan ==
    *(1) Project [id#0L, (id#0L * 5) AS x5#2L]
    +- *(1) Filter (id#0L < 1000)
       +- *(1) Range (1, 1000000, step=1, splits=12)
    """
    numbers_x5.explain(True)  # explain(True) will give you all 4 query plans
    # filter occurs before the select => predicate pushdown

    more_numbers = spark.range(1, 100000, 2)
    split7 = more_numbers.repartition(7)  # shuffle - round robin
    split7.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=18]
       +- Range (1, 100000, step=2, splits=12)
    """

    df1 = spark.range(1, 100000000)
    df2 = spark.range(1, 50000000, 2)
    df3 = df1.repartition(7)
    df4 = df2.repartition(13)
    df5 = df3.select((col("id") * 3).alias("id"))
    joined = df5.join(df4, "id")
    sum_df = joined.select(sum(col("id")))
    sum_df.explain()
    # [1,2,3], [4,5,6], [7,8]
    # [6, 15, 15]
    # 36
    """
    == Physical Plan ==
    *(7) HashAggregate(keys=[], functions=[sum(id#17L)])
    +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=90]
       +- *(6) HashAggregate(keys=[], functions=[partial_sum(id#17L)])
          +- *(6) Project [id#17L]
             +- *(6) SortMergeJoin [id#17L], [id#11L], Inner
                :- *(3) Sort [id#17L ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(id#17L, 200), ENSURE_REQUIREMENTS, [plan_id=74]
                :     +- *(2) Project [(id#9L * 3) AS id#17L]
                :        +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=70]
                :           +- *(1) Range (1, 100000000, step=1, splits=12)
                +- *(5) Sort [id#11L ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(id#11L, 200), ENSURE_REQUIREMENTS, [plan_id=81]
                      +- Exchange RoundRobinPartitioning(13), REPARTITION_BY_NUM, [plan_id=80]
                         +- *(4) Range (1, 50000000, step=2, splits=12)
    """

if __name__ == '__main__':
    demo_query_plans()
    sleep(999999)

