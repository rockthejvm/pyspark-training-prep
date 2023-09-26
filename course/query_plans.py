
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Spark Essentials") \
    .getOrCreate()

def demo_query_plans():
    simple_numbers = spark.range(1, 1000000)  # DF with a single column "id"
    numbers_x5 = simple_numbers.selectExpr("id * 5 as id")
    # numbers_x5.explain()
    """
    == Physical Plan ==
    *(1) Project [(id#0L * 5) AS id#2L]
    +- *(1) Range (1, 1000000, step=1, splits=1)
    """
    # query plan = the sequence/graph of steps that Spark will run IF the DF gets evaluated
    # query plan is available BEFORE running the job

    more_numbers = spark.range(1, 1000000, 2)
    split_7 = more_numbers.repartition(7)  # round-robin partitioning
    split_7.explain()
    """
    AdaptiveSparkPlan isFinalPlan=false
    +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=6]
       +- Range (1, 1000000, step=2, splits=1)
    """

    ds1 = spark.range(1, 1000000)
    ds2 = spark.range(1, 10000, 2)
    ds3 = ds1.repartition(7)
    ds4 = ds2.repartition(9)
    ds5 = ds3.selectExpr("id * 3 as id")
    joined = ds5.join(ds4, "id")
    sum_df = joined.select(F.sum(F.col("id")))
    sum_df.explain(True)

if __name__ == '__main__':
    demo_query_plans()
