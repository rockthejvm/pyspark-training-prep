from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# [1] add postgres jar
spark = SparkSession \
    .builder \
    .master("local") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .config("spark.sql.warehouse.dir", "../spark-warehouse") \
    .appName("Spark Essentials") \
    .getOrCreate()

# Questions to ask:
#   - what's a DF (distributed "table")
#   - what's a Row - data structure (tuple) that conforms to a schema (description of fields/columns and types)


# expressions
# exercise: read the cars DF and compute (name of the car, weight in kg) - 1kg = 2.2lbs
cars_df = spark.read.json("../data/cars")
cars_kg = cars_df.withColumn("weight_in_kg", cars_df.Weight_in_lbs / 2.2)
#                                            ^^^^^^^^^^^^^^^^^^^^^ column object
cars_kg_2 = cars_df.select("Name", (col("Weight_in_lbs") / 2.2).alias("Weight_in_kg"))
#                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^ column object
cars_kg_3 = cars_df.select(col("Name"), expr("Weight_in_lbs / 2.2")) # expression returning a col object
cars_kg_4 = cars_df.selectExpr("Name", "Weight_in_lbs / 2.2")

# More questions
#   - how do we obtain a schema, what's schema inference
#   - what's a transformation and what's an action
#   - what is "laziness" in Spark

# exercise: create a new DF with the total profit of every movie, type COMEDY and with IMDB rating above 6
movies_df = spark.read.json("../data/movies")
comedies_profits = movies_df \
    .filter((movies_df.Major_Genre == "Comedy") & (movies_df.IMDB_Rating > 6)) \
    .withColumn("Total_Profits", movies_df.US_Gross + movies_df.Worldwide_Gross + movies_df.US_DVD_Sales) \
    .select("Title", "Total_Profits")

# data sources
# exercise: save comedies_profits as 1) TSV file, 2) parquet
def save_formats_exercise():
    comedies_profits.write \
        .option("delimiter", "\t") \
        .option("header", "true") \
        .mode("overwrite") \
        .csv("../data/movies_tsv")

    # parquet default
    comedies_profits.write.save("../data/movies_parquet")

    # dump the data as a Postgres table
    comedies_profits.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public.movies") \
        .save()

# aggregations
"""
    Exercises:
    1. sum up all the profits of all the movies in the DF
    2. count how many distinct directors we have
    3. compute mean/standard deviation of US gross revenue
    4. compute the average IMDB rating AND average US gross revenue PER every director
"""
total_profits_df = movies_df.select((sum("US_Gross") + sum("Worldwide_Gross") + sum("US_DVD_Sales")).alias("Total_MONEY"))
all_aggregations_df = movies_df.groupBy(lit("All movies")).agg(
    sum("Total_Profit"), countDistinct("Director"), mean("US_Gross"), stddev("US_Gross")
)
ratings_per_director = movies_df.groupBy("Director").mean("IMDB_Rating", "US_Gross")

# joins
# - what join types do you know?


def read_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table_name) \
        .load()

"""
    Exercises (depending on mood and level of experience): 
        1. show all employees and their max salary (all time)
        2. show all employees who were never managers
        3. for each employee, find the difference 
            between their own latest salary and the max salary of their job/department
    
    Use just exercise 2 if they're super comfortable. 
"""
# different names (camelcase) for this exercise
employees_df = read_table("employees")
salaries_df = read_table("salaries")
dept_managers_df = read_table("dept_manager")
titles_df = read_table("titles")
dept_emp_df = read_table("dept_emp")
departments_df = read_table("departments")

# -- 1
maxSalariesPerEmpNoDF = salaries_df.groupBy("emp_no").agg(max("salary").alias("maxSalary"))
employeesSalariesDF = employees_df.join(maxSalariesPerEmpNoDF, "emp_no")

# -- 2

empNeverManagersDF = employees_df.join(
    dept_managers_df,
    employees_df.emp_no == dept_managers_df.emp_no,
    "left_anti"
)

# -- 3
"""
select max(s.salary) maxs, d.dept_name dname
    from salaries s, employees e, departments d, dept_emp de
    where e.emp_no = de.emp_no
    and d.dept_no = de.dept_no
    and s.emp_no = e.emp_no
    group by d.dept_name
"""
max_salaries_df = employees_df \
    .join(dept_emp_df, "emp_no") \
    .join(departments_df, "dept_no") \
    .join(salaries_df, "emp_no") \
    .groupBy("dept_no") \
    .agg(max("salary").alias("max_salary"))

# select emp_no, max(from_date) date from salaries group by emp_no
latest_salary_dates_df = salaries_df \
    .groupBy("emp_no") \
    .agg(max("from_date").alias("from_date"))

"""
select s.emp_no, s.salary 
from salaries s, latest_salary_dates_df l 
where s.emp_no = l.emp_no and s.from_date = l.from_date
"""
latest_salaries_df = latest_salary_dates_df \
    .join(salaries_df, ["emp_no", "from_date"]) \
    .select("emp_no", "salary")

diff_in_salary_df = employees_df \
    .join(latest_salaries_df, "emp_no") \
    .join(dept_emp_df, "emp_no") \
    .join(max_salaries_df, "dept_no") \
    .selectExpr("first_name", "last_name", "dept_no", "max_salary - salary as salary_difference")

if __name__ == "__main__":
    pass