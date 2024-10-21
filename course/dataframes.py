import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-11"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Spark DataFrames") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .getOrCreate()

print(os.environ['JAVA_HOME'])

# what is a DataFrame? a distributed "table" with rows of the same structure (schema = column names + types)
# IMMUTABLE - cannot change the data inside, any change => NEW DF

# exercise: read the cars DF and compute (name of the car, weight in kg = lbs/2.2), show the DF

# transformation: DF -> DF changing DESCRIPTION => Spark has lazy computation
# action: forces the evaluation of a DF

def first_exercise():
    # Read the JSON file into a DataFrame
    original_df = spark.read.json("../data/cars/cars.json")
    cars_df = original_df
    # Divide the "Weight_in_lbs" column by 2.2
    # transformations: withColumn, select, drop, join, group ...
    cars_df = cars_df.withColumn("Weight_in_kg", col("Weight_in_lbs") / 2.2)
    #                                                    ^^^^^^^^^^^^^^^^^^^^ column object
    #                                                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^ column object
    cars_df = cars_df.select(col("Name"), col("Weight_in_kg"))

    cars_df_v2 = original_df.select(col("Name"), (col("Weight_in_lbs") / 2.2).alias("Weight_in_kg"))
    cars_df_v3 = original_df.select(col("Name"), expr("Weight_in_lbs / 2.2 as Weight_in_kg"))
    #                                                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ SQL syntax here
    cars_df_v4 = original_df.selectExpr("Name", "Weight_in_lbs / 2.2 as Weight_in_kg")
    # ^^ Spark will do expr(arg) on every argument of selectExpr
    # NO PERF DIFFERENCE!

    # Show the results
    cars_df.show()
    # actions: show, collect, take, count ...

"""
    CSV: 
        - dateFormat (e.g. "ddmmyyyy")
        - separator (can be , : ; tab, etc.)
        - null values (e.g. to differentiate vs empty strings)
        - header true/false
        
    JSON:
        - dateFormat
        - allowSingleQuotes
        
    Compression
        - uncompressed, gzip, snappy, bzip2
        
    Parquet
        - default format
        - 10x smaller than raw data
        
    JDBC 
"""

def demo_formats():
    # exercise: create a new DF out of movies DF, return all COMEDIES with IMDB rating > 6
    # store that df as a tab-separated file
    df = spark.read.json("../data/movies")
    df = df.where("IMDB_Rating > 6 and Major_Genre = 'Comedy'")
    # df.write \
    #     .option("header", "true") \
    #     .option("sep", "\t") \
    #     .format("csv") \
    #     .save("../data/movies_tsv")

    df.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public.movies") \
        .save()

# aggregations
"""
    1. Sum up all the profits of all the movies in the DF
    2. Count how many distinct directors we have
    3. Mean/standard deviation for US gross revenue
    4. Average IMDB rating and average US gross revenue PER director
"""
def demo_aggregations():
    original_df = spark.read.json("../data/movies")

    # select(sum...), countDistinct return a DF with a single value
    df = original_df.select(sum(original_df.Worldwide_Gross).alias("worldwide_gross"))
    df.show()
    
    df1 = original_df.select(countDistinct("Director").alias("distinct_director_count"))
    df1.show()
    
    df2 = original_df.select(mean(original_df.US_Gross).alias("mean_us_gross"))
    df2.show()

    # grouped DF -> compute stats per unique group
    df3 = original_df.groupBy("Director") \
        .agg(avg("IMDB_Rating").alias("avg_rating"), avg("US_Gross").alias("avg_us_gross"))
    df3.show()

# joins
# read all the tables from the database
def read_table(table_name):
    # -- reading the data
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table_name) \
        .load()

def demo_joins():
    departments_df = read_table("departments")
    dept_emp_df = read_table("dept_emp")
    dept_manager_df = read_table("dept_manager")
    employees_df = read_table("employees")
    salaries_df = read_table("salaries")
    titles_df = read_table("titles")

    """
    Exercises
        - show all employees and their max salary
        - show all employees who were never managers
    """
    employees_salaries = employees_df.join(salaries_df.groupBy("emp_no").max("salary"), on="emp_no", how="inner")
    employees_salaries.show()

    # left-ANTI join = select all rows in the left table such that there is NO row on the right table satisfying the condition
    # select * from left where NOT EXISTS (select * from right where ...)
    never_managers_df = employees_df.join(dept_manager_df, "emp_no", "left_anti")
    never_managers_df.show()

    # left-SEMI join = select all rows in the left table such that there is EXISTS a row on the right table satisfying the condition
    # select * from left where EXISTS (select * from right where ...)


if __name__ == '__main__':
    demo_aggregations()
