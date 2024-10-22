from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("RDD Demo") \
    .getOrCreate()

# Spark session is the entry point to the DF API
# df = spark.read.json("../data/movies")

# RDDs need Spark Context
sc = spark.sparkContext

# chunks = partitions
numbers = sc.parallelize(range(1000000))

# FP primitives
tenx_numbers = numbers.map(lambda x: x * 10) # new RDD with new numbers
even_numbers = numbers.filter(lambda x: x % 2 == 0)
grouped_numbers = numbers.groupBy(lambda x: x % 3) # every unique value of the lambda creates a new group
# after groupBy you can do ___ByKey functions

# map, flatMap, filter, ... = transformations (LAZY)
# transformations are descriptions until triggered with an ACTION

# RDDs are the most performant (1% of the cases)
# iterator to iterator transformations with custom logic -> mapPartitions

# RDDs give you control over partitioning

# DF <-> RDD? yes
# DF -> RDD
movies_df = spark.read.json("../data/movies")
movies_rdd = movies_df.rdd # RDD[Row], Rows are of the schema from the DF
grouped_by_genre = movies_rdd.groupBy(lambda row: row.Major_Genre)

# RDD -> DF to get the DF/SQL
# must have RDD[Row], all Rows must have the same schema
movies_df_v2 = spark.createDataFrame(movies_rdd)

if __name__ == '__main__':
    print(tenx_numbers.collect()[:10]) # action