from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("DataFrames") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .getOrCreate()

# Q: what is a DataFrame? a DISTRIBUTED "table" with rows conforming to a "structure" (column names + types) = schema
# DataFrames are IMMUTABLE - DFs cannot be changed, any change => NEW DF
# Q: what is a transformation? A function DF => another DF
# transformations are LAZY - no computation is run before we call an ACTION
# job? the computation triggered by an ACTION

def demo_transformations():
    """
        Read the cars DF and show
        - name of the car as uppercase (or leave as initial)
        - weight of the car (in pounds)
        - weight of the car (in kg = lbs/2.2)
    """
    df = spark.read.json("../data/cars")  # DF reader
    df2 = df.select(upper(df.Name), df.Weight_in_lbs, (df.Weight_in_lbs/2.2).alias("Weight_in_kgs"))
    #                     ^^^^^^^ column object
    #               ^^^^^^^^^^^^^ column object
    df2.printSchema()
    df2.show() # ACTION, others: count, collect, take, ....


# data sources / sinks

def read_movies_df():
    """
        Exercise: read the movies DF, save it as a TSV file and as Parquet
    """
    df = spark.read.option("useSingleQuotes", "true").json("../data/movies")
    df.show()
    # save df as a TSV file
    df.write.option("delimiter", "\t").mode("overwrite").csv("../data/movies_tsv")
    df.write.mode("overwrite").save("../data/movies_parquet") # parquet is the default data format
    # write to JDBC
    df.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public.movies") \
        .save()

def get_stats():
    """
        Exercise - read the movies DF and
            - sum up all the profits of all the movies
            - count how many distinct directors
            - mean/stddev for US gross revenue
            - average IMDB rating and average US gross revenue PER Director
    """
    df = spark.read.json('../data/movies')
    df.printSchema()

    # DF-wide aggregations
    stats_df = df.select(
        (sum('US_Gross') + sum('Worldwide_Gross')).alias('profits_sum'),
        countDistinct('Director').alias('nunique_directors'),
        mean('US_Gross').alias('us_gross_mean'),
        std('US_Gross').alias('us_gross_std'),
    )
    stats_df.show()

    # grouped aggregations
    avg_per_dir_df = df.groupBy('Director').agg({'IMDB_Rating':'avg', 'US_Gross':'avg'})
    avg_per_dir_df_v2 = df.groupBy('Director').agg(avg('IMDB_Rating'), avg('US_Gross'))
    avg_per_dir_df.show()

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
    joins exercise - read the tables in Postgres as DFs
    - show all employees and their max salary (all time)
    - show all employees who were never managers
    - for each employee, find the difference between their own latest salary and the max salary (all time) of their department
"""
def show_employees():
    df_employees = read_table("employees")
    df_salaries = read_table("salaries")
    df_managers = read_table("dept_manager")
    df_dept_emp = read_table("dept_emp")

    # 1
    df_emp_sal = ((df_employees
                   .join(df_salaries, on="emp_no", how="left"))
                  .groupBy("emp_no")
                  .agg(max("salary").alias("max_salary")))
    df_emp_sal.show()

    # 2
    df_emp_man = (df_employees
                  .join(df_managers, on="emp_no", how="left")).filter("dept_no is NULL").distinct()
    # LEFT ANTI join = select * from left where NOT EXISTS (select * in right where ...)
    df_emp_man_v2 = df_emp_man.join(df_managers, "emp_no", "left_anti") # left anti join is faster than full/left outer + filter
    # opposite of ANTI join = SEMI join
    # select * from left where EXISTS (select * from right where ...) - faster than a full outer join + a filter

    df_emp_man.show()

def calc_joins_jakub_ex3():
    emp_df = read_table('employees')
    sal_df = read_table('salaries')
    dept_man_df = read_table('dept_manager')
    dept_emp = read_table('dept_emp')

    # max salary of the departments
    dept_emp_sal = dept_emp.join(sal_df, on='emp_no')
    dept_emp_sal = dept_emp_sal.groupBy('dept_no').agg(max('salary').alias('max_dept_salary'))
    # dept_no , max_dept_salary

    # latest salaries for every employee
    latest_salaries = sal_df.groupBy('emp_no').agg(max('from_date').alias('from_date'))
    latest_salaries = sal_df.join(latest_salaries, on=['emp_no', 'from_date'], how='left').select("emp_no", "salary")
    # emp_no, salary

    diff_in_salary = latest_salaries \
        .join(dept_emp, "emp_no") \
        .join(dept_emp_sal, "dept_no") \
        .selectExpr("emp_no", "dept_no", "max_dept_salary - salary")

    diff_in_salary.show()

if __name__ == '__main__':
    calc_joins_jakub_ex3()

# TODO - what's an RDD