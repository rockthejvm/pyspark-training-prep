from pyspark.sql import SparkSession
from time import time
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Exercise1") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

def add_columns(df, n_columns):
    # "select id * 1 as id_1, id * 2 as id_2, ... id * 100 as id_100"
    new_columns = ["id * " + str(i) + " as id_" + str(i) for i in range(n_columns)]
    return df.selectExpr("id", *new_columns)

def exercise():
    initial_table = spark.range(10000000).repartition(10) # given
    another_table = spark.range(5000000).repartition(7) # given

    wide_table = add_columns(initial_table, 30) # you have to add 30 new columns to initial table
    join1 = wide_table.join(another_table, "id") # you have to do this join
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, id_0#8L, id_1#9L, id_2#10L, id_3#11L, id_4#12L, id_5#13L, id_6#14L, id_7#15L, id_8#16L, id_9#17L, id_10#18L, id_11#19L, id_12#20L, id_13#21L, id_14#22L, id_15#23L, id_16#24L, id_17#25L, id_18#26L, id_19#27L, id_20#28L, id_21#29L, id_22#30L, ... 7 more fields]
       +- SortMergeJoin [id#0L], [id#4L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=29]
          :     +- Project [id#0L, (id#0L * 0) AS id_0#8L, (id#0L * 1) AS id_1#9L, (id#0L * 2) AS id_2#10L, (id#0L * 3) AS id_3#11L, (id#0L * 4) AS id_4#12L, (id#0L * 5) AS id_5#13L, (id#0L * 6) AS id_6#14L, (id#0L * 7) AS id_7#15L, (id#0L * 8) AS id_8#16L, (id#0L * 9) AS id_9#17L, (id#0L * 10) AS id_10#18L, (id#0L * 11) AS id_11#19L, (id#0L * 12) AS id_12#20L, (id#0L * 13) AS id_13#21L, (id#0L * 14) AS id_14#22L, (id#0L * 15) AS id_15#23L, (id#0L * 16) AS id_16#24L, (id#0L * 17) AS id_17#25L, (id#0L * 18) AS id_18#26L, (id#0L * 19) AS id_19#27L, (id#0L * 20) AS id_20#28L, (id#0L * 21) AS id_21#29L, (id#0L * 22) AS id_22#30L, ... 7 more fields]
          :        +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=21]
          :           +- Range (0, 10000000, step=1, splits=16)
          +- Sort [id#4L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#4L, 200), ENSURE_REQUIREMENTS, [plan_id=30]
                +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=24]
                   +- Range (0, 5000000, step=1, splits=16)
    """
    # start_time = time()
    # join1.show()
    # print(f"Time: {time() - start_time}s") # 4s

    initial_table = initial_table.repartition('id')
    another_table = another_table.repartition('id')
    wide_table = add_columns(initial_table, 30)
    join2 = wide_table.join(another_table, "id")
    join2.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, id_0#104L, id_1#105L, id_2#106L, id_3#107L, id_4#108L, id_5#109L, id_6#110L, id_7#111L, id_8#112L, id_9#113L, id_10#114L, id_11#115L, id_12#116L, id_13#117L, id_14#118L, id_15#119L, id_16#120L, id_17#121L, id_18#122L, id_19#123L, id_20#124L, id_21#125L, id_22#126L, ... 7 more fields]
       +- SortMergeJoin [id#0L], [id#4L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Project [id#0L, (id#0L * 0) AS id_0#104L, (id#0L * 1) AS id_1#105L, (id#0L * 2) AS id_2#106L, (id#0L * 3) AS id_3#107L, (id#0L * 4) AS id_4#108L, (id#0L * 5) AS id_5#109L, (id#0L * 6) AS id_6#110L, (id#0L * 7) AS id_7#111L, (id#0L * 8) AS id_8#112L, (id#0L * 9) AS id_9#113L, (id#0L * 10) AS id_10#114L, (id#0L * 11) AS id_11#115L, (id#0L * 12) AS id_12#116L, (id#0L * 13) AS id_13#117L, (id#0L * 14) AS id_14#118L, (id#0L * 15) AS id_15#119L, (id#0L * 16) AS id_16#120L, (id#0L * 17) AS id_17#121L, (id#0L * 18) AS id_18#122L, (id#0L * 19) AS id_19#123L, (id#0L * 20) AS id_20#124L, (id#0L * 21) AS id_21#125L, (id#0L * 22) AS id_22#126L, ... 7 more fields]
          :     +- Exchange hashpartitioning(id#0L, 200), REPARTITION_BY_COL, [plan_id=21]
          :        +- Range (0, 10000000, step=1, splits=16)
          +- Sort [id#4L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#4L, 200), REPARTITION_BY_COL, [plan_id=24]
                +- Range (0, 5000000, step=1, splits=16)
    """
    start_time = time()
    join2.show()
    print(f"Time: {time() - start_time}s") # 2.2s

    join3 = add_columns(initial_table.join(another_table, "id"), 30)
    #                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ can get extra perf if by joining you get fewer rows
    join3.explain()
    start_time = time()
    join3.show()
    print(f"Time: {time() - start_time}s") # 1.3s!


if __name__ == '__main__':
    exercise()