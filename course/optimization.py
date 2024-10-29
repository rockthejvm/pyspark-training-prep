from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("SparkPlayground") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

def demo_query_plans():
    simple_numbers = spark.range(1000000) # DF with column "id"
    numbers_x5 = simple_numbers.select((col("id") * 5).alias("x5"))
    numbers_x5.explain() # shows a query plan
    """
    == Physical Plan ==
    *(1) Project [(id#0L * 5) AS x5#2L]
    +- *(1) Range (0, 1000000, step=1, splits=16)
    """
    # data flow - bottom to top
    # dependencies - top to bottom

    more_numbers = spark.range(1, 10000000, 2)
    split7 = more_numbers.repartition(7) # round robin redistribution (shuffle)
    split7.explain()
    # without AQE
    """
    == Physical Plan ==
    Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=16]
    +- *(1) Range (1, 10000000, step=2, splits=16)
    """
    # with AQE
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=14]
       +- Range (1, 10000000, step=2, splits=16)
    """

    ds1 = spark.range(1000000000)
    ds2 = spark.range(1,100000000, 2)
    ds3 = ds1.repartition(7)
    ds4 = ds2.repartition(9)
    ds5 = ds3.select((col("id") * 3).alias("id"))
    joined = ds5.join(ds4, "id")
    sum_df = joined.select(sum("id"))
    sum_df.explain(True)
    #  [] [] [] [] [] [] [] [] 1000 x 1M rows
    #  a  b  c  d  ....         1000 numbers (partial results)
    # exchange => [a,b,c,d,......]
    # => final result
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
                :           +- *(1) Range (0, 1000000000, step=1, splits=16)
                +- *(5) Sort [id#10L ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(id#10L, 200), ENSURE_REQUIREMENTS, [plan_id=77]
                      +- Exchange RoundRobinPartitioning(9), REPARTITION_BY_NUM, [plan_id=76]
                         +- *(4) Range (1, 100000000, step=2, splits=16)
    """

    # entire process:
    """
    == Parsed Logical Plan ==
    'Project [unresolvedalias(sum('id), Some(org.apache.spark.sql.Column$$Lambda$1834/0x0000000800c4c730@58ece1bf))]
    +- Project [id#16L]
       +- Join Inner, (id#16L = id#10L)
          :- Project [(id#8L * cast(3 as bigint)) AS id#16L]
          :  +- Repartition 7, true
          :     +- Range (0, 1000000000, step=1, splits=Some(16))
          +- Repartition 9, true
             +- Range (1, 100000000, step=2, splits=Some(16))
    
    == Analyzed Logical Plan ==
    sum(id): bigint
    Aggregate [sum(id#16L) AS sum(id)#20L]
    +- Project [id#16L]
       +- Join Inner, (id#16L = id#10L)
          :- Project [(id#8L * cast(3 as bigint)) AS id#16L]
          :  +- Repartition 7, true
          :     +- Range (0, 1000000000, step=1, splits=Some(16))
          +- Repartition 9, true
             +- Range (1, 100000000, step=2, splits=Some(16))
    
    == Optimized Logical Plan ==
    Aggregate [sum(id#16L) AS sum(id)#20L]
    +- Project [id#16L]
       +- Join Inner, (id#16L = id#10L)
          :- Project [(id#8L * 3) AS id#16L]
          :  +- Repartition 7, true
          :     +- Range (0, 1000000000, step=1, splits=Some(16))
          +- Repartition 9, true
             +- Range (1, 100000000, step=2, splits=Some(16))
    
    == Physical Plan ==
    *(7) HashAggregate(keys=[], functions=[sum(id#16L)], output=[sum(id)#20L])
    +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=86]
       +- *(6) HashAggregate(keys=[], functions=[partial_sum(id#16L)], output=[sum#23L])
          +- *(6) Project [id#16L]
             +- *(6) SortMergeJoin [id#16L], [id#10L], Inner
                :- *(3) Sort [id#16L ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(id#16L, 200), ENSURE_REQUIREMENTS, [plan_id=70]
                :     +- *(2) Project [(id#8L * 3) AS id#16L]
                :        +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=66]
                :           +- *(1) Range (0, 1000000000, step=1, splits=16)
                +- *(5) Sort [id#10L ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(id#10L, 200), ENSURE_REQUIREMENTS, [plan_id=77]
                      +- Exchange RoundRobinPartitioning(9), REPARTITION_BY_NUM, [plan_id=76]
                         +- *(4) Range (1, 100000000, step=2, splits=16)
    """

if __name__ == '__main__':
    demo_query_plans()