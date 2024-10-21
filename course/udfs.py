from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar")  \
    .appName("UDFs") \
    .getOrCreate()

# UDF = user-defined functions
# plymouth satellite -> Plymouth Satellite
def convert_case(name):
    words = name.split(" ")
    return " ".join([word[0].upper() + word[1:] for word in words if len(word) > 0])

# UDFs are narrow transformations
def demo_udf():
    convert_case_udf = udf(convert_case, StringType()) # register a UDF for a particular column type
    cars_formatted_df = spark.read.json("../data/cars").select(convert_case_udf(col("Name")).alias("Name_Capitalized"))
    cars_formatted_df.show()

# UDAF = user-defined AGGREGATE functions
# need pyarrow, pandas for this example
# example: for every movie, compute IMDB_Rating - avg(IMDB_Rating for its genre)
def demo_udaf():
    def diff_vs_mean(pandas_df):
        return pandas_df.assign(Rating_Diff=pandas_df.IMDB_Rating - pandas_df.IMDB_Rating.mean())

    movies_df = spark.read.json("../data/movies") \
        .filter(col("Major_Genre").isNotNull() & col("IMDB_Rating").isNotNull()) \
        .select("Title", "Major_Genre", "IMDB_Rating") \
        .withColumn("Rating_Diff", lit(0.0))

    subtract_mean = pandas_udf(diff_vs_mean, movies_df.schema, PandasUDFType.GROUPED_MAP) # because we'll use groupBy
    diff_vs_mean_df = movies_df.groupby("Major_Genre").apply(subtract_mean)
    diff_vs_mean_df.show()


if __name__ == '__main__':
    demo_udaf()
