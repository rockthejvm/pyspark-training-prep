from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar")\
    .appName("DataFrames") \
    .getOrCreate()


# DataFrame is:
# immutable (can't be changed) - a transformation returns a new DataFrame
# lazily evaluated - not computed until some "action" is performed
# distributed - stored on multiple machines
# "table" = list of rows of the same "shape" (schema)

# action = triggers evaluation
# transformation = a description of a computation

# select the titles with the total money made from every movie
movies_df = spark.read.json("../data/movies") \
    .select("Title", (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).alias("MONEY"))
#                    ^^^^^^^^^^^^^^^ column object
#                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ column object
comedies_df = movies_df.where(col("Major_Genre") == "Comedy")
comedies_schema = comedies_df.schema

comedies_schema_v2 = StructType([
    StructField("title", StringType()),
    StructField("MONEY", DoubleType())
])


stocks_schema = StructType([
    StructField("name", StringType()),
    StructField("date", DateType()),
    StructField("price", DoubleType())
])

# data formats
# CSV, JSON, parquet, JDBC, ...

# CSV
# options: inferSchema, header, sep/delimiter, nullValue, dateFormat
def write_comedies_csv():
    comedies_df.write\
        .option("sep", "\t")\
        .option("header", "true")\
        .option("format", "csv")\
        .save("../data/movies_tsv")

def read_stocks():
    stocks_df = spark.read\
        .option("dateFormat", "MMM d YYYY")\
        .schema(stocks_schema) \
        .csv("../data/stocks")

# JSON
# options: allowSingleQuotes, nullValue, dateFormat

# Parquet - highly compressed, ~10x compared to raw
# fast, contains the schema inside the files
# the default format to read/write data in Spark
# in Palantir you have a special format for timeseries ("soho")

# simple_df = spark.read.load(".../data/whatever") # assumed to be parquet!
# comedies_df.write.parquet(path="../data/comedies", compression="snappy")

# JDBC = read/write data to/from a regular DB
employees_df = spark.read\
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5443/rtjvm") \
    .option("user", "docker") \
    .option("password", "docker") \
    .option("dbtable", "public.employees") \
    .load()


if __name__ == '__main__':
    employees_df.show()