from pyspark.sql import SparkSession, Window
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

"""
    Exercises:
        - movies DF: compute ALL the profit from ALL the movies => a single number
        - count how many distinct directors we have
        - print the mean/standard deviation of IMDB rating and RT rating
        - compute the average IMDB rating and US gross revenue PER DIRECTOR
"""

# Schema
# df.printSchema()
# 1. All profit from all movies
def exercises_v1():
    movies_df \
        .select((col("US_Gross") + col("Worldwide_Gross") + coalesce(col("US_DVD_Sales"), lit(0)) - col("Production_Budget")).alias("profit"))\
        .select(sum(col("profit"))).show()
    # ^^ aggregation

    total_profit_aneta = movies_df \
        .select(sum("US_Gross").alias("US"), sum("Worldwide_Gross").alias("world"), sum(coalesce(col("US_DVD_Sales"), lit(0))).alias("DVDs")) \
        .select(col("US") + col("world") + col("DVDs"))

    # 2. All distinct directors
    movies_df.select(countDistinct("Director")).show()

    # 3. Mean/stddev IMDB and RT
    movies_df.select(
        mean("IMDB_Rating"),
        mean("Rotten_Tomatoes_Rating"),
        stddev("IMDB_Rating"),
        stddev("Rotten_Tomatoes_Rating")
    ).show()
    mean_std_aneta = movies_df.agg({'Rotten_Tomatoes_rating': 'mean', 'IMDB_Rating': 'mean'})

    # 4. Mean IMDB and US Gross per director
    movies_df.groupby("Director").agg(mean("IMDB_Rating"), mean("US_Gross")).show()


"""
    Exercises
"""
def read_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5443/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table_name) \
        .load()

employees_df = read_table("employees")
departments_df = read_table("departments")
dept_emp_df = read_table("dept_emp")
dept_manager_df = read_table("dept_manager")
salaries_df = read_table("salaries")
titles_df = read_table("titles")

"""
    Exercises
    - show all employees and their max salary (all time)
    - show all employees who were never managers
    - show for every employee,
        the difference between their latest salary and the max salary (all time) of their department
"""

def ex1_lucas():
    salaries_df = salaries_df.groupby('emp_no').max('salary')
    employees_df = employees_df.join(salaries_df, on='emp_no', how='left')
    employees_df.select(col('first_name'), col('last_name'), col('max(salary)')).show()


# left anti join - selects all the rows on the left table, where there IS NO MATCH on the right table
# select * from employees where NOT EXISTS (select * from dept_manager where ...)
# schema == the left table's schema

# left semi join
# select * from left where EXISTS (select * from right where ...)
def ex2():
    result_df = employees_df.join(dept_manager_df, on='emp_no', how="left_anti")
    result_df.show()


def ex3_aidar():
    # 3. Latest employee salary diff with max salary of dept
    # 3.1 Max sal of department
    max_dept_sal_df = (
        dept_emp_df.alias("dept_emp")
        .join(salaries_df.alias("sal"), "emp_no")
        .groupby("dept_no").agg(max("salary").alias("max_dept_sal"))
    )

    # 3.2 Latest employee sal
    # max_dept_sal_df.join(dept_emp_df, "dept_no").join(salaries_df, "emp_no").
    w = Window.partitionBy("sal.emp_no").orderBy(col("sal.to_date").desc())

    latest_sal_df = (
        dept_emp_df.join(salaries_df.alias("sal"), "emp_no").withColumn("rank_date", rank().over(w))
        .where(col("rank_date") == lit(1))
    )

    (
        max_dept_sal_df.alias("max_dept_sal_df")
        .join(latest_sal_df.alias("latest_sal_df"), "dept_no")
        .select(
            "latest_sal_df.emp_no",
            "latest_sal_df.dept_no",
            "latest_sal_df.salary",
            col("max_dept_sal_df.max_dept_sal") - col("latest_sal_df.salary")
        )
    ).show()
    

if __name__ == '__main__':
    employees_df.show()