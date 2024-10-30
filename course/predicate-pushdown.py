from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("PredicatePushdown") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .getOrCreate()

def demo_first_pushdown():
    numbers = spark.range(10**6)

    numbers_with_mod = numbers \
        .withColumn("mod", col("id") % 5) \
        .filter(col("id") > 1000)

    # filter data as early as possible
    numbers_with_mod.explain(True)
    """
    == Parsed Logical Plan ==
    'Filter ('id > 1000)
    +- Project [id#0L, (id#0L % cast(5 as bigint)) AS mod#2L]
       +- Range (0, 1000000, step=1, splits=Some(16))
    
    == Analyzed Logical Plan ==
    id: bigint, mod: bigint
    Filter (id#0L > cast(1000 as bigint))
    +- Project [id#0L, (id#0L % cast(5 as bigint)) AS mod#2L]
       +- Range (0, 1000000, step=1, splits=Some(16))
    
    == Optimized Logical Plan ==
    Project [id#0L, (id#0L % 5) AS mod#2L]
    +- Filter (id#0L > 1000) <---- HERE - predicate pushdown
       +- Range (0, 1000000, step=1, splits=Some(16))
    
    == Physical Plan ==
    *(1) Project [id#0L, (id#0L % 5) AS mod#2L]
    +- *(1) Filter (id#0L > 1000)
       +- *(1) Range (0, 1000000, step=1, splits=16)
    """

def demo_pushdown_on_group():
    numbers = spark.range(10**6)
    numbers_with_mod = numbers \
        .withColumn("mod", col("id") % 5) \

    grouping = Window.partitionBy("mod").orderBy("id")
    ranked = numbers_with_mod \
        .withColumn("rank", rank().over(grouping))\
        .filter(col("mod") > 3)
    # wide transformation then filter

    ranked.explain(True)
    """
    == Parsed Logical Plan ==
    'Filter ('mod > 3)
    +- Project [id#0L, mod#2L, rank#7]
       +- Project [id#0L, mod#2L, rank#7, rank#7]
          +- Window [rank(id#0L) windowspecdefinition(mod#2L, id#0L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#7], [mod#2L], [id#0L ASC NULLS FIRST]
             +- Project [id#0L, mod#2L]
                +- Project [id#0L, (id#0L % cast(5 as bigint)) AS mod#2L]
                   +- Range (0, 1000000, step=1, splits=Some(16))
    
    == Analyzed Logical Plan ==
    id: bigint, mod: bigint, rank: int
    Filter (mod#2L > cast(3 as bigint))
    +- Project [id#0L, mod#2L, rank#7]
       +- Project [id#0L, mod#2L, rank#7, rank#7]
          +- Window [rank(id#0L) windowspecdefinition(mod#2L, id#0L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#7], [mod#2L], [id#0L ASC NULLS FIRST]
             +- Project [id#0L, mod#2L]
                +- Project [id#0L, (id#0L % cast(5 as bigint)) AS mod#2L]
                   +- Range (0, 1000000, step=1, splits=Some(16))
    
    == Optimized Logical Plan ==
    Window [rank(id#0L) windowspecdefinition(mod#2L, id#0L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#7], [mod#2L], [id#0L ASC NULLS FIRST]
    +- Project [id#0L, (id#0L % 5) AS mod#2L]
       +- Filter ((id#0L % 5) > 3) <---- HERE predicate pushdown
          +- Range (0, 1000000, step=1, splits=Some(16))
    
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Window [rank(id#0L) windowspecdefinition(mod#2L, id#0L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#7], [mod#2L], [id#0L ASC NULLS FIRST]
       +- Sort [mod#2L ASC NULLS FIRST, id#0L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(mod#2L, 200), ENSURE_REQUIREMENTS, [plan_id=16]
             +- Project [id#0L, (id#0L % 5) AS mod#2L]
                +- Filter ((id#0L % 5) > 3)
                   +- Range (0, 1000000, step=1, splits=16)
    """

def read_table(table):
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table) \
        .load()

def demo_pushdown_on_source():
    employees_df = read_table("employees")
    salaries_df = read_table("salaries")
    df_emp_sal = (employees_df
                      .join(salaries_df, on="emp_no", how="left")
                      .groupBy("emp_no")
                      .agg(max("salary").alias("max_salary"))
                      .join(employees_df, "emp_no")
                      .filter("hire_date > '1989-01-01'")
                  )

    """
    == Parsed Logical Plan ==
    'Filter ('hire_date > 1989-01-01)
    +- Project [emp_no#0, max_salary#39, birth_date#43, first_name#44, last_name#45, gender#46, hire_date#47]
       +- Join Inner, (emp_no#0 = emp_no#42)
          :- Aggregate [emp_no#0], [emp_no#0, max(salary#13) AS max_salary#39]
          :  +- Project [emp_no#0, birth_date#1, first_name#2, last_name#3, gender#4, hire_date#5, salary#13, from_date#14, to_date#15]
          :     +- Join LeftOuter, (emp_no#0 = emp_no#12)
          :        :- Relation [emp_no#0,birth_date#1,first_name#2,last_name#3,gender#4,hire_date#5] JDBCRelation(public.employees) [numPartitions=1]
          :        +- Relation [emp_no#12,salary#13,from_date#14,to_date#15] JDBCRelation(public.salaries) [numPartitions=1]
          +- Relation [emp_no#42,birth_date#43,first_name#44,last_name#45,gender#46,hire_date#47] JDBCRelation(public.employees) [numPartitions=1]
    
    == Analyzed Logical Plan ==
    emp_no: int, max_salary: int, birth_date: date, first_name: string, last_name: string, gender: string, hire_date: date
    Filter (hire_date#47 > cast(1989-01-01 as date))
    +- Project [emp_no#0, max_salary#39, birth_date#43, first_name#44, last_name#45, gender#46, hire_date#47]
       +- Join Inner, (emp_no#0 = emp_no#42)
          :- Aggregate [emp_no#0], [emp_no#0, max(salary#13) AS max_salary#39]
          :  +- Project [emp_no#0, birth_date#1, first_name#2, last_name#3, gender#4, hire_date#5, salary#13, from_date#14, to_date#15]
          :     +- Join LeftOuter, (emp_no#0 = emp_no#12)
          :        :- Relation [emp_no#0,birth_date#1,first_name#2,last_name#3,gender#4,hire_date#5] JDBCRelation(public.employees) [numPartitions=1]
          :        +- Relation [emp_no#12,salary#13,from_date#14,to_date#15] JDBCRelation(public.salaries) [numPartitions=1]
          +- Relation [emp_no#42,birth_date#43,first_name#44,last_name#45,gender#46,hire_date#47] JDBCRelation(public.employees) [numPartitions=1]
    
    == Optimized Logical Plan ==
    Project [emp_no#0, max_salary#39, birth_date#43, first_name#44, last_name#45, gender#46, hire_date#47]
    +- Join Inner, (emp_no#0 = emp_no#42)
       :- Aggregate [emp_no#0], [emp_no#0, max(salary#13) AS max_salary#39]
       :  +- Project [emp_no#0, salary#13]
       :     +- Join LeftOuter, (emp_no#0 = emp_no#12)
       :        :- Project [emp_no#0]
       :        :  +- Filter isnotnull(emp_no#0)
       :        :     +- Relation [emp_no#0,birth_date#1,first_name#2,last_name#3,gender#4,hire_date#5] JDBCRelation(public.employees) [numPartitions=1]
       :        +- Project [emp_no#12, salary#13]
       :           +- Filter isnotnull(emp_no#12)
       :              +- Relation [emp_no#12,salary#13,from_date#14,to_date#15] JDBCRelation(public.salaries) [numPartitions=1]
       +- Filter ((isnotnull(hire_date#47) AND (hire_date#47 > 1989-01-01)) AND isnotnull(emp_no#42))
            ^----- HERE predicate pushdown!
          +- Relation [emp_no#42,birth_date#43,first_name#44,last_name#45,gender#46,hire_date#47] JDBCRelation(public.employees) [numPartitions=1]
    
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [emp_no#0, max_salary#39, birth_date#43, first_name#44, last_name#45, gender#46, hire_date#47]
       +- SortMergeJoin [emp_no#0], [emp_no#42], Inner
          :- Sort [emp_no#0 ASC NULLS FIRST], false, 0
          :  +- HashAggregate(keys=[emp_no#0], functions=[max(salary#13)], output=[emp_no#0, max_salary#39])
          :     +- HashAggregate(keys=[emp_no#0], functions=[partial_max(salary#13)], output=[emp_no#0, max#56])
          :        +- Project [emp_no#0, salary#13]
          :           +- SortMergeJoin [emp_no#0], [emp_no#12], LeftOuter
          :              :- Sort [emp_no#0 ASC NULLS FIRST], false, 0
          :              :  +- Exchange hashpartitioning(emp_no#0, 200), ENSURE_REQUIREMENTS, [plan_id=36]
          :              :     +- Scan JDBCRelation(public.employees) [numPartitions=1] [emp_no#0] PushedFilters: [*IsNotNull(emp_no)], ReadSchema: struct<emp_no:int>
          :              +- Sort [emp_no#12 ASC NULLS FIRST], false, 0
          :                 +- Exchange hashpartitioning(emp_no#12, 200), ENSURE_REQUIREMENTS, [plan_id=37]
          :                    +- Scan JDBCRelation(public.salaries) [numPartitions=1] [emp_no#12,salary#13] PushedFilters: [*IsNotNull(emp_no)], ReadSchema: struct<emp_no:int,salary:int>
          +- Sort [emp_no#42 ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(emp_no#42, 200), ENSURE_REQUIREMENTS, [plan_id=46]
                +- Scan JDBCRelation(public.employees) [numPartitions=1] [emp_no#42,birth_date#43,first_name#44,last_name#45,gender#46,hire_date#47] PushedFilters: [*IsNotNull(hire_date), *GreaterThan(hire_date,1989-01-01), *IsNotNull(emp_no)], ReadSchema: struct<emp_no:int,birth_date:date,first_name:string,last_name:string,gender:string,hire_date:date>
                WATCH HERE ------->>>>-------------------->>>--------->>>>-------------------->>>--------->>>>-------------------->>>--------->>>>-------------------->>>--------->>>>-------------------->^^^ PREDICATE PUSHDOWN!
    """
    df_emp_sal.explain(True)
    # data sources with support for pushed filters: JDBC, Parquet, CSV, JSON(?), custom data sources


if __name__ == '__main__':
    demo_pushdown_on_source()