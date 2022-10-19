from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Spark Optimization") \
    .getOrCreate()

# join mechanics
# [code] broadcast joins
def demo_small_large_join():
    from pyspark.sql import Row
    a = spark.range(1, 100000000)
    b = spark.createDataFrame([Row(0, "zero"), Row(1, "first"), Row(2, "second"), Row(3, "third")])
    c = b.withColumnRenamed("_1", "id")
    joined = a.join(c, "id")
    joined.explain()
    joined.show()


def demo_broadcast_join():
    from pyspark.sql import Row
    a = spark.range(1, 100000000)
    b = spark.createDataFrame([Row(0, "zero"), Row(1, "first"), Row(2, "second"), Row(3, "third")])
    c = b.withColumnRenamed("_1", "id")
    joined = a.join(broadcast(c), "id")
    joined.explain()
    joined.show()