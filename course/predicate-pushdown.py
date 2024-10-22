from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Predicate Pushdown") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .getOrCreate()

# predicate pushdown = filter runs as early as possible
def demo_first_pushdown():
    numbers = spark.range(10000)
    evens = numbers.withColumnRenamed("id", "NUMBER") \
        .filter(col("NUMBER") % 2 == 0)
    evens.explain(True)

def demo_pushdown_on_group():
    numbers = spark.range(100000).withColumnRenamed("id", "NUMBER") \
        .withColumn("mod5", col("NUMBER") % 5)
    grouping = Window.partitionBy("mod5").orderBy("NUMBER")
    ranked = numbers.withColumn("rank", rank().over(grouping)).filter(col("mod5") > 3)

    ranked.explain(True)

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

def demo_pushdown_jdbc():
    departments_df = read_table("departments")
    dept_emp_df = read_table("dept_emp")
    dept_manager_df = read_table("dept_manager")
    employees_df = read_table("employees")
    salaries_df = read_table("salaries")
    titles_df = read_table("titles")

    employees_salaries = employees_df.join(salaries_df.groupBy("emp_no").max("salary"), on="emp_no", how="inner")
    new_employees_salaries = employees_salaries.filter("hire_date > '1999-01-01'")
    new_employees_salaries.explain(True)

if __name__ == '__main__':
    demo_pushdown_jdbc()