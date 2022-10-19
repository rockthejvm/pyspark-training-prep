from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .appName("Predicate Pushdown demo") \
    .getOrCreate()

sc = spark.sparkContext


def demo_first_pushdown():
    numbers = spark.range(100000)
    # process the original dataframe in some way, then add a filter
    evens = numbers \
        .selectExpr("id as NUMBER") \
        .where(expr("NUMBER % 2 = 0"))

    # normally, the filter would go last
    # explain(true) to see how Spark initially starts with filter applied last, then it pushes it down
    evens.explain(True)


# Spark will try to push a filter as early as possible while still guaranteeing the correctness of complex computations
def demo_pushdown_on_group():
    numbers = spark.range(100000).withColumnRenamed("id", "NUMBER") \
        .withColumn("modulus", expr("NUMBER % 5"))
    grouping = Window.partitionBy("modulus").orderBy("NUMBER")
    ranked = numbers.withColumn("rank", rank().over(grouping)).where(expr("modulus > 3"))
    ranked.explain(True)
    # same happens with joins and groups


# many data sources support pushdown filters out of the box
# example: exercise 1 from the joins lesson ("show all employees and their max salary (all time) JUST FOR employees hired from jan 1, 1999")
def demo_pushdown_jdbc():

    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/rtjvm"
    user = "docker"
    password = "docker"

    # define this utility function so that we don't have to copy/paste all the time
    def readTable(tableName):
        return spark.read \
            .format("jdbc") \
            .option("driver", driver) \
            .option("url", url) \
            .option("user", user) \
            .option("password", password) \
            .option("dbtable", "public." + tableName) \
            .load()


    # different names (camelcase) for this exercise
    employees_df = readTable("employees")
    salaries_df = readTable("salaries")
    dept_managers_df = readTable("dept_manager")
    titles_df = readTable("titles")
    dept_emp_df = readTable("dept_emp")
    departments_df = readTable("departments")

    # -- 1
    maxSalariesPerEmpNoDF = salaries_df.groupBy("emp_no").agg(max("salary").alias("maxSalary"))
    employeesSalariesDF = employees_df.join(maxSalariesPerEmpNoDF, "emp_no")
    newEmployeesDF = employeesSalariesDF.filter("hire_date > '1999-01-01'")
    newEmployeesDF.explain(True)
    # this one is really nice because the filter first shows at the top, then right above the scan of the data source, then right in the data source scan

if __name__ == '__main__':
    demo_pushdown_jdbc()
