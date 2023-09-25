
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master("local") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .appName("Spark Essentials") \
    .getOrCreate()

# DataFrames = distributed "table"
# structured API
movies_df = spark.read.json("../data/movies")

# "projections" = selecting & transforming columns
"""
    Exercises:
        - read the movies df and select 2 columns of your choice
        - create another column summing up the total profit of the movies, for every movie
        - select all COMEDY movies with IMDB rating above 6
"""

# spark.read.json actually does this:
movies_df_v2 = spark.read.option("inferSchema", "true").format("json").load("../data/movies")
movies_details_df = movies_df.select("Title", "IMDB_Rating")
movies_details_df_v2 = movies_df.select(col("Title"), col("IMDB_Rating"))
#                                       ^^^^^^^^^^^^ column object

# 2
movies_profits_df = movies_df.select(
    col("Title"),
    (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).alias("Total_Gross")
)

# 3
at_least_mediocre_comedies_df = movies_df \
    .select("Title", "IMDB_Rating") \
    .where((col("Major_Genre") == "Comedy") & (col("IMDB_Rating") > 6))

at_least_mediocre_comedies_df_v2 = movies_df \
    .select("Title", "IMDB_Rating") \
    .where(col("Major_Genre") == "Comedy") \
    .where(col("IMDB_Rating") > 6)

third_ex = movies_df\
    .filter((col("Major_Genre") == "Comedy") & (col("IMDB_Rating") >= 6)) \
    .select(
        col("Title"),
        col("Major_Genre"),
        (col("US_Gross") + col("Worldwide_Gross")).alias("Total_Profit")
    )

# select + expr = selectExpr
third_ex_v2 = movies_df \
    .filter(expr("Major_Genre = 'Comedy' and IMDB_Rating > 6")) \
    .selectExpr(
        "Title", # sql string here
        "Major_Genre",
        "US_Gross + Worldwide_Gross as Total_Profit"
    )

# data formats and sources
# json, csv, parquet, JDBC
cars_schema = StructType([
    StructField("Name", StringType()),
    StructField("Miles_per_Gallon", DoubleType()),
    StructField("Cylinders", LongType()),
    StructField("Displacement", DoubleType()),
    StructField("Horsepower", LongType()),
    StructField("Weight_in_lbs", LongType()),
    StructField("Acceleration", DoubleType()),
    StructField("Year", DateType()),
    StructField("Origin", StringType()),
])

# json flags: allow single quotes, compression (uncompressed, tar, zip, ...)
cars_df = spark.read \
    .schema(cars_schema) \
    .option("allowSingleQuotes", "true") \
    .option("dateFormat", "YYYY-MM-dd") \
    .json("../data/cars")

# flags to use when parsing incorrect rows:
# .option("mode", "...") -> options are PERMISSIVE (puts null), FAILFAST (crashes), DROPMALFORMED (ignores row)

# CSV
# schema, date formats (same as json), header, separator, null value
stocks_schema = StructType([
    StructField("stock", StringType()),
    StructField("date", DateType()),
    StructField("price", DoubleType())
])

stocks_df = spark.read \
    .schema(stocks_schema) \
    .option("dateFormat", "MMM d YYYY") \
    .option("header", "false") \
    .option("sep", ",") \
    .option("nullValue", "") \
    .csv("../data/stocks")

# parquet = default format, highly compressed (10x), very fast access, also contains schema

# JDBC
employees_df = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5443/rtjvm") \
    .option("user", "docker") \
    .option("password", "docker") \
    .option("dbtable", "public.employees") \
    .load()



"""
    Exercises: read the cars DF, then write it as
    - tab-separated values with a header
    - parquet
    - table "public.cars" in the db
"""

def data_formats_exercise():
    cars_df.write \
        .mode('overwrite') \
        .option("header", "true") \
        .option("sep", "\t") \
        .csv("../data/output/cars.tsv")

    cars_df.write.mode('overwrite').parquet("../data/output/cars.parquet")

    cars_df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5443/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public.cars") \
        .save()

# aggregations
"""
    Aggregations exercises
    - sum up all the money made by all the movies from the entire movies DF -> a single number
    - count how many distinct directors we have
    - compute the average IMDB rating and the average US gross revenue PER DIRECTOR
"""

def aggregations_exercises():
    movies_df_sum = movies_df.agg(sum(col("Total_Gross"))).alias("Total_Gross_Amt")
    movies_df_dir = movies_df.select("Director").distinct().count()
    movies_df_dir = movies_df.select(countDistinct("Director"))
    movies_df_avg = movies_df.groupby("Director") \
        .agg(
            round(avg(coalesce(col("IMDB_Rating"), lit(0))), 2).alias("avg_IMDB_Rating"),
#                                                            ^ rounding precision (decimals)
#                     ^^^^^^^^ gives alt value in case of nulls
            avg("US_Gross").alias("avg_US_Gross")
         )
    # df.groupBy is a different kind of DF (RelationalGroupedDataset) - doesn't have the select function

# joins

"""
    Exercises
    0. transfer all tables from postgres:
        - take the name of Docker container
        - docker exec -it (container name) psql -U docker       
        - \c rtjvm
        - \dt describes all tables
    1. show all employees and their max salary (all time)
    2. show all employees who were never managers
    3. for each employees, find the difference between
        - their own latest salary
        - the max salary of their department
"""

def transfer_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5443/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table_name) \
        .load()

def joins_exercises_shree():
    emp_df = transfer_table("employees")
    sal_df = transfer_table("salaries")
    dept_mgr_df = transfer_table("dept_manager")
    dept_emp = transfer_table("dept_emp")

    max_sal_df = sal_df.groupby(col("emp_no")).agg(max(col("salary")).alias("max_salary"))
    emp_sal_df = emp_df.join(max_sal_df, "emp_no", "inner")

    no_managers_df = emp_df.join(dept_mgr_df, "emp_no", "left").where(col("dept_no").isNull())

    # 3
    lat_sal_df = sal_df.filter(col("to_date") == '9999-01-01').select("emp_no", "salary")
    lat_dept_emp = dept_emp.filter(col("to_date") == '9999-01-01').select("emp_no", "dept_no")

    max_dep_sal = lat_dept_emp.join(lat_sal_df, "emp_no", "inner") \
        .groupby("dept_no") \
        .agg(max(col("salary")).alias(("max_dept_salary"))) \
        .select("dept_no", "max_dept_salary")

    df = emp_df.join(lat_sal_df, "emp_no", "inner") \
        .join(lat_dept_emp, "emp_no", "inner") \
        .join(max_dep_sal, "dept_no", "inner")


cars_df = transfer_table("cars")
departments_df = transfer_table("departments")
dept_emp_df = transfer_table("dept_emp")
dept_manager_df = transfer_table("dept_manager")
employees_df = transfer_table("employees")
salaries_df = transfer_table("salaries")
titles_df = transfer_table("titles")

def joins_exercises_elena():
    # Exercise 1
    window_spec = (
        Window
        .partitionBy(col("emp_no"))
        .orderBy(col("salary").desc())
    )
    max_salary = (
        salaries_df
        .withColumn("salary_rank", dense_rank().over(window_spec))
        .filter(col("salary_rank") == 1)
        .dropDuplicates(col("emp_no"))
        .select(
            col("emp_no"),
            col("salary").alias("max_salary")
        )
    )
    ex_1 = employees_df.join(max_salary, "emp_no", "left")
    ex_1.show()

    # Exercise 2
    # left-anti - select everything in the left table for which there is NO row in the right table under the join condition
    # SQL equivalent = select * from left_table where NOT EXISTS (select * from right_table where joinCondition)
    # schema is kept from the left DF
    # left-semi - select everything in the left table for which there IS a row in the right table under the join condition
    ex_2 = employees_df.join(dept_manager_df, "emp_no", "left-anti")
    ex_2.show()

    # Exercise 3
    # not quite done yet

def joins_exercise_peter():
    employee_salary = employees_df.alias("employee_df").join(salaries_df.alias("salary_df"), "emp_no", "inner") \
        .groupby("employee_df.emp_no") \
        .max("salary_df.salary")
# narrow and wide transformations
if __name__ == '__main__':
    pass