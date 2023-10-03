from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("RDDs") \
    .getOrCreate()


def read_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5443/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table_name) \
        .load()

# Spark Session = "entry point" for the DF API, Spark SQL API (equivalent)

# RDDs = resilient distributed dataset = distributed "list" of items of the same type
# RDDs are lazy
# all DFs are backed by RDDs

# Pro: high control, run ANY computation (not just "selects") massive optimizations
# Cons: high labor

# Spark Context = entry point for RDDs
sc = spark.sparkContext

# RDD from memory
numbers_rdd = sc.parallelize(range(1, 1000000))  # RDD[Int]
# RDD from files
jsons_rdd = sc.textFile("../data/movies/movies.json")  # RDD[String]

# RDD from a DF
movies_df = spark.read.json("../data/movies")
movies_rdd = movies_df.rdd  # RDD[Row]
# RDD[Row] to DF
movies_df_2 = spark.createDataFrame(movies_rdd, movies_df.schema)



# Transformations on RDDs
# filter
even_numbers = numbers_rdd.filter(lambda n: n % 2 == 0)  # a new RDD[Int]
# map - transform the RDD into another one, by calling the function on every element
tenx_numbers = numbers_rdd.map(lambda n: n * 10)  # RDD[Int]
# flatMap
expanded_numbers = numbers_rdd.flatMap(lambda n: range(n))  # new RDD built out of all the small ranges, combined into one
# numerical functions: min, max - only for RDDs of comparable types e.g. Int or String
biggest_number = numbers_rdd.max
# reduce - aggregates all the data into one
sum_numbers = numbers_rdd.reduce(lambda x, y: x + y)  # a single number


# grouping
even_odd_numbers = numbers_rdd.groupBy(lambda x: x % 2)  # an RDD of tuples: [(0, [2,4,6,8,...]), (1,[1,3,5,7,9,...])]

# example of how we would "find the employee with the longest name by department"
employees_rdd = read_table("employees").rdd
employees_by_dept = employees_rdd.groupBy(lambda row: row.dept_no) # [(1, [...]), (2, [...]) ... ]
def process_department(tuple):
    (dept_no, employees) = tuple
    longest_name_employee = employees[0]
    for employee in employees:
        if(len(employee.name) > len(longest_name_employee.name)):
            longest_name_employee = employee

    return (dept_no, longest_name_employee)

longest_name_by_dept = employees_by_dept.map(lambda tuple: process_department(tuple))  # RDD[(String, String)]

# 90% of cases - use DFs
# use RDDs if
#   A) you need heavy optimizations,
#   B) you have custom functions not supported by DFs,
#   C) you know the structure of your data

# DF -> RDD and RDD -> DF conversion (in PySpark) are HORRIBLE in performance

if __name__ == '__main__':
    even_numbers.foreach(lambda x: print(x))
