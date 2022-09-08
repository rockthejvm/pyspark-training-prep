############################################################
# Spark SQL Essentials
############################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# [1] add postgres jar
# [2] config with warehouse directory
spark = SparkSession \
    .builder \
    .master("local") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .config("spark.sql.warehouse.dir", "../spark-warehouse") \
    .appName("Spark Essentials") \
    .getOrCreate()

# backing DFs
cars_df = spark.read.json("../data/cars")
movies_df = spark.read.json("../data/movies")

# same values to connect to Postgres
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://localhost:5432/rtjvm"
user = "docker"
password = "docker"

# regular DF API
european_cars_df = cars_df.filter(
    col("Origin") != "USA"
)

# SQL API - need to save the DF under a local name known to the Spark cluster
cars_df.createOrReplaceTempView("cars")
european_cars_df_v2 = spark.sql("select Name from cars where Origin = 'USA'")

# we can run ANY SQL statement
spark.sql("create database rtjvm")
spark.sql("use rtjvm")

# can store existing in-memory DFs in the Spark warehouse's "database"
movies_df.write.saveAsTable("movies")

# can read a DF from a "table"
movies_df_v2 = spark.read.table("movies")  # if the "movies" table can be found in the warehouse

"""
 Exercises

 1. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
 2. Show the average salaries for the employees hired in between those dates, grouped by department.
 3. Show the name of the best-paying department for employees hired in between those dates.
"""


# utilities: transfer tables
def read_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", "public." + table_name) \
        .load()


def transfer_tables(tables):
    for table_name in tables:
        df = read_table(table_name)
        df.createOrReplaceTempView(table_name)


transfer_tables(
    [
        "employees",
        "departments",
        "titles",
        "dept_emp",
        "salaries",
        "dept_manager"
    ]
)

# 1
spark.sql(
    """
      select count(*)
      from employees
      where hire_date > '1999-01-01' and hire_date < '2000-01-01'
    """
)

# 2
spark.sql(
    """
      select de.dept_no, avg(s.salary)
      from employees e, dept_emp de, salaries s
      where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
       and e.emp_no = de.emp_no
       and e.emp_no = s.emp_no
      group by de.dept_no
    """
)

# 3
spark.sql(
    """
      select avg(s.salary) payments, d.dept_name
      from employees e, dept_emp de, salaries s, departments d
      where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
           and e.emp_no = de.emp_no
           and e.emp_no = s.emp_no
           and de.dept_no = d.dept_no
      group by d.dept_name
      order by payments desc
      limit 1
    """
)
