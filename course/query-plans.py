from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "false") \
    .appName("Query Plans") \
    .getOrCreate()

def demo_query_plans():
    simple_numbers = spark.range(1, 1000000) # DF with col "id"
    numbers_x5 = simple_numbers.selectExpr("id * 5 as id")
    numbers_x5.explain()
    """
    == Physical Plan ==
    *(1) Project [(id#0L * 5) AS id#2L]
    +- *(1) Range (1, 1000000, step=1, splits=16)
    """

    more_numbers = spark.range(1, 1000000, 2)
    numbers_split_7 = more_numbers.repartition(7)
    numbers_split_7.explain()
    """
    == Physical Plan ==
    Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=16]
    +- *(1) Range (1, 1000000, step=2, splits=16)
    """

    ds1 = spark.range(1, 1000000000)
    ds2 = spark.range(1, 100000000, 2)
    ds3 = ds1.repartition(7)
    ds4 = ds2.repartition(9)
    ds5 = ds3.selectExpr("id * 3 as id")
    joined = ds5.join(ds4, "id")
    sum_df = joined.selectExpr("sum(id)")
    sum_df.explain()
    """
    == Physical Plan ==
    *(7) HashAggregate(keys=[], functions=[sum(id#16L)])
    +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=86]
       +- *(6) HashAggregate(keys=[], functions=[partial_sum(id#16L)])
          +- *(6) Project [id#16L]
             +- *(6) SortMergeJoin [id#16L], [id#10L], Inner
                :- *(3) Sort [id#16L ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(id#16L, 200), ENSURE_REQUIREMENTS, [plan_id=70]
                :     +- *(2) Project [(id#8L * 3) AS id#16L]
                :        +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=66]
                :           +- *(1) Range (1, 1000000000, step=1, splits=16)
                +- *(5) Sort [id#10L ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(id#10L, 200), ENSURE_REQUIREMENTS, [plan_id=77]
                      +- Exchange RoundRobinPartitioning(9), REPARTITION_BY_NUM, [plan_id=76]
                         +- *(4) Range (1, 100000000, step=2, splits=16)
    """

    df = spark.range(1, 100000)
    derived = df \
        .withColumn("name", expr("id * 2")) \
        .withColumn("name_2", expr("id * 2")) \
        .withColumn("name_3", expr("id * 2")) \
        .withColumn("name_4", expr("id * 2"))
    derived.explain(True)
    """
    == Parsed Logical Plan ==
    'Project [id#25L, ('id * 2) AS name#39, name_2#30L, name_3#34L]
    +- Project [id#25L, name#27L, name_2#30L, (id#25L * cast(2 as bigint)) AS name_3#34L]
       +- Project [id#25L, name#27L, (id#25L * cast(2 as bigint)) AS name_2#30L]
          +- Project [id#25L, (id#25L * cast(2 as bigint)) AS name#27L]
             +- Range (1, 100000, step=1, splits=Some(16))
    
    == Analyzed Logical Plan ==
    id: bigint, name: bigint, name_2: bigint, name_3: bigint
    Project [id#25L, (id#25L * cast(2 as bigint)) AS name#39L, name_2#30L, name_3#34L]
    +- Project [id#25L, name#27L, name_2#30L, (id#25L * cast(2 as bigint)) AS name_3#34L]
       +- Project [id#25L, name#27L, (id#25L * cast(2 as bigint)) AS name_2#30L]
          +- Project [id#25L, (id#25L * cast(2 as bigint)) AS name#27L]
             +- Range (1, 100000, step=1, splits=Some(16))
    
    == Optimized Logical Plan ==
    Project [id#25L, (id#25L * 2) AS name#39L, (id#25L * 2) AS name_2#30L, (id#25L * 2) AS name_3#34L]
    +- Range (1, 100000, step=1, splits=Some(16))
    
    == Physical Plan ==
    *(1) Project [id#25L, (id#25L * 2) AS name#39L, (id#25L * 2) AS name_2#30L, (id#25L * 2) AS name_3#34L]
    +- *(1) Range (1, 100000, step=1, splits=16)
    """

    derived_v2 = df.selectExpr(
        "id * 2 as name",
        "id * 2 as name_2",
        "id * 2 as name_3",
        "id * 2 as name_4",
    )
    derived_v2.explain()
    """
    == Physical Plan ==
    *(1) Project [(id#25L * 2) AS name#45L, (id#25L * 2) AS name_2#46L, (id#25L * 2) AS name_3#47L, (id#25L * 2) AS name_4#48L]
    +- *(1) Range (1, 100000, step=1, splits=16)
    """

if __name__ == '__main__':
    demo_query_plans()