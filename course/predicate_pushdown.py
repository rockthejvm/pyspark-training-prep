from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .master("local") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .appName("Predicate Pushdown") \
    .getOrCreate()

# predicate pushdown = filter data as early as possible
def demo_pushdown():
    numbers = spark.range(100000) # DF with a single col "id"
    even_numbers = numbers.selectExpr("id as NUMBER").filter(F.col("NUMBER") % 2 == 0)
    even_numbers.explain(True)
    """
    == Parsed Logical Plan ==
    'Filter (('NUMBER % 2) = 0)
    +- Project [id#0L AS NUMBER#2L]
       +- Range (0, 100000, step=1, splits=Some(1))
    
    == Analyzed Logical Plan ==
    NUMBER: bigint
    Filter ((NUMBER#2L % cast(2 as bigint)) = cast(0 as bigint))
    +- Project [id#0L AS NUMBER#2L]
       +- Range (0, 100000, step=1, splits=Some(1))
    
    == Optimized Logical Plan ==
    Project [id#0L AS NUMBER#2L]  <----- THIS WAS SWAPPED!!
    +- Filter ((id#0L % 2) = 0)  <----- predicate occurs first (pushed down)
       +- Range (0, 100000, step=1, splits=Some(1))
    
    == Physical Plan ==
    *(1) Project [id#0L AS NUMBER#2L]
    +- *(1) Filter ((id#0L % 2) = 0)
       +- *(1) Range (0, 100000, step=1, splits=1)
    """


def demo_pushdown_on_group():
    numbers = spark.range(100000).withColumnRenamed("id", "NUMBER").withColumn("mod", F.col("NUMBER") % 5)
    grouping = Window.partitionBy("mod").orderBy("NUMBER")
    result = numbers.withColumn("rank", F.rank().over(grouping)).filter(F.col("mod") > 3)
    result.explain(True)
    """
    == Analyzed Logical Plan ==
    NUMBER: bigint, mod: bigint, rank: int
    Filter (mod#4L > cast(3 as bigint))
    +- Project [NUMBER#2L, mod#4L, rank#9]
       +- Project [NUMBER#2L, mod#4L, rank#9, rank#9]
          +- Window [rank(NUMBER#2L) windowspecdefinition(mod#4L, NUMBER#2L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#9], [mod#4L], [NUMBER#2L ASC NULLS FIRST]
             +- Project [NUMBER#2L, mod#4L]
                +- Project [NUMBER#2L, (NUMBER#2L % cast(5 as bigint)) AS mod#4L]
                   +- Project [id#0L AS NUMBER#2L]
                      +- Range (0, 100000, step=1, splits=Some(1))
    
    == Optimized Logical Plan ==
    Window [rank(NUMBER#2L) windowspecdefinition(mod#4L, NUMBER#2L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#9], [mod#4L], [NUMBER#2L ASC NULLS FIRST]
    +- Project [id#0L AS NUMBER#2L, (id#0L % 5) AS mod#4L]
       +- Filter ((id#0L % 5) > 3) <---- FILTER IS PUSHED DOWN, even before the column I'm filtering by was created!
          +- Range (0, 100000, step=1, splits=Some(1))
    """

# show all employees and their max salary (all time) only for employees hired after jan 1, 1999
def transfer_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5443/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table_name) \
        .load()

cars_df = transfer_table("cars")
departments_df = transfer_table("departments")
dept_emp_df = transfer_table("dept_emp")
dept_manager_df = transfer_table("dept_manager")
employees_df = transfer_table("employees")
salaries_df = transfer_table("salaries")
titles_df = transfer_table("titles")

def demo_pushdown_source():
    employees_df_2 = spark.read.load("../data/employees")
    max_salaries_df = salaries_df.groupBy("emp_no").agg(F.max("salary").alias("max_salary"))
    emp_salaries_df = employees_df_2.join(max_salaries_df, "emp_no")
    new_employees_df = emp_salaries_df.filter("hire_date > '1999-01-01'")
    new_employees_df.explain(True)
    """
    == Parsed Logical Plan ==
    'Filter ('hire_date > 1999-01-01)
    +- Project [emp_no#38, birth_date#39, first_name#40, last_name#41, gender#42, hire_date#43, max_salary#71]
       +- Join Inner, (emp_no#38 = emp_no#50)
          :- Relation [emp_no#38,birth_date#39,first_name#40,last_name#41,gender#42,hire_date#43] JDBCRelation(public.employees) [numPartitions=1]
          +- Aggregate [emp_no#50], [emp_no#50, max(salary#51) AS max_salary#71]
             +- Relation [emp_no#50,salary#51,from_date#52,to_date#53] JDBCRelation(public.salaries) [numPartitions=1]

    == Analyzed Logical Plan ==
    emp_no: int, birth_date: date, first_name: string, last_name: string, gender: string, hire_date: date, max_salary: int
    Filter (hire_date#43 > cast(1999-01-01 as date))
    +- Project [emp_no#38, birth_date#39, first_name#40, last_name#41, gender#42, hire_date#43, max_salary#71]
       +- Join Inner, (emp_no#38 = emp_no#50)
          :- Relation [emp_no#38,birth_date#39,first_name#40,last_name#41,gender#42,hire_date#43] JDBCRelation(public.employees) [numPartitions=1]
          +- Aggregate [emp_no#50], [emp_no#50, max(salary#51) AS max_salary#71]
             +- Relation [emp_no#50,salary#51,from_date#52,to_date#53] JDBCRelation(public.salaries) [numPartitions=1]

    == Optimized Logical Plan ==
    Project [emp_no#38, birth_date#39, first_name#40, last_name#41, gender#42, hire_date#43, max_salary#71]
    +- Join Inner, (emp_no#38 = emp_no#50)
       :- Filter ((isnotnull(hire_date#43) AND (hire_date#43 > 1999-01-01)) AND isnotnull(emp_no#38))
       :  +- Relation [emp_no#38,birth_date#39,first_name#40,last_name#41,gender#42,hire_date#43] JDBCRelation(public.employees) [numPartitions=1]
       +- Aggregate [emp_no#50], [emp_no#50, max(salary#51) AS max_salary#71]
          +- Project [emp_no#50, salary#51]
             +- Filter isnotnull(emp_no#50)
                +- Relation [emp_no#50,salary#51,from_date#52,to_date#53] JDBCRelation(public.salaries) [numPartitions=1]

    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [emp_no#38, birth_date#39, first_name#40, last_name#41, gender#42, hire_date#43, max_salary#71]
       +- SortMergeJoin [emp_no#38], [emp_no#50], Inner
          :- Sort [emp_no#38 ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(emp_no#38, 200), ENSURE_REQUIREMENTS, [plan_id=27]
          :     +- Scan JDBCRelation(public.employees) [numPartitions=1] [emp_no#38,birth_date#39,first_name#40,last_name#41,gender#42,hire_date#43] PushedFilters: [*IsNotNull(hire_date), *GreaterThan(hire_date,1999-01-01), *IsNotNull(emp_no)], ReadSchema: struct<emp_no:int,birth_date:date,first_name:string,last_name:string,gender:string,hire_date:date>
          +- Sort [emp_no#50 ASC NULLS FIRST], false, 0
             +- HashAggregate(keys=[emp_no#50], functions=[max(salary#51)], output=[emp_no#50, max_salary#71])
                +- Exchange hashpartitioning(emp_no#50, 200), ENSURE_REQUIREMENTS, [plan_id=23]
                   +- HashAggregate(keys=[emp_no#50], functions=[partial_max(salary#51)], output=[emp_no#50, max#82])
                      +- Scan JDBCRelation(public.salaries) [numPartitions=1] [emp_no#50,salary#51] PushedFilters: [*IsNotNull(emp_no)], ReadSchema: struct<emp_no:int,salary:int>
    """

if __name__ == '__main__':
    demo_pushdown_source()