from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import sleep

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Optimization") \
    .getOrCreate()

sc = spark.sparkContext

def compare_spark_apis():
    df_10m = spark.range(1, 10000000)  # DF with a column called "id"
    rdd_10m = sc.parallelize(range(10000000))  # RDD of ints
    print(df_10m.count())  # 0.8s
    print(rdd_10m.count())  # 0.9s
    print(df_10m.rdd.count())  # 9s
    print(spark.createDataFrame(rdd_10m, IntegerType()).count())  # 6s


if __name__ == '__main__':
    compare_spark_apis()
    sleep(999999)

