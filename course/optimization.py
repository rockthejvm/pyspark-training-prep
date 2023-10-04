from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import sleep

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Optimization") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

sc = spark.sparkContext

def compare_spark_apis():
    df_10m = spark.range(1, 10000000)  # DF with a column called "id"
    rdd_10m = sc.parallelize(range(10000000))  # RDD of ints
    print(df_10m.count())  # 0.8s
    print(rdd_10m.count())  # 0.9s
    print(df_10m.rdd.count())  # 9s
    print(spark.createDataFrame(rdd_10m, IntegerType()).count())  # 6s


def demo_query_plans():
    simple_numbers = spark.range(1, 1000000)
    numbers_x5 = simple_numbers\
        .select(col("id"), (col("id") * 5).alias("x5"))
    """
    == Physical Plan ==
    *(1) Project [(id#0L * 5) AS x5#2L]
    +- *(1) Range (1, 1000000, step=1, splits=12)
    """
    numbers_x5.explain()

    more_numbers = spark.range(1, 100000, 2)
    split7 = more_numbers.repartition(7)  # shuffle - round robin
    split7.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=18]
       +- Range (1, 100000, step=2, splits=12)
    """

    df1 = spark.range(1, 100000000)
    df2 = spark.range(1, 50000000, 2)
    df3 = df1.repartition(7)
    df4 = df2.repartition(13)
    df5 = df3.select((col("id") * 3).alias("id"))
    joined = df5.join(df4, "id")
    sum_df = joined.select(sum(col("id")))
    sum_df.explain()
    # [1,2,3], [4,5,6], [7,8]
    # [6, 15, 15]
    # 36
    """
    == Physical Plan ==
    *(7) HashAggregate(keys=[], functions=[sum(id#17L)])
    +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=90]
       +- *(6) HashAggregate(keys=[], functions=[partial_sum(id#17L)])
          +- *(6) Project [id#17L]
             +- *(6) SortMergeJoin [id#17L], [id#11L], Inner
                :- *(3) Sort [id#17L ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(id#17L, 200), ENSURE_REQUIREMENTS, [plan_id=74]
                :     +- *(2) Project [(id#9L * 3) AS id#17L]
                :        +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=70]
                :           +- *(1) Range (1, 100000000, step=1, splits=12)
                +- *(5) Sort [id#11L ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(id#11L, 200), ENSURE_REQUIREMENTS, [plan_id=81]
                      +- Exchange RoundRobinPartitioning(13), REPARTITION_BY_NUM, [plan_id=80]
                         +- *(4) Range (1, 50000000, step=2, splits=12)
    """


def demo_pushdown():
    simple_numbers = spark.range(1, 1000000)
    numbers_x5 = simple_numbers \
        .select(col("id"), (col("id") * 5).alias("x5")) \
        .filter(col("id") < 1000)

    # predicate pushdown

    """
    == Parsed Logical Plan ==
    'Filter ('id < 1000)
    +- Project [id#0L, (id#0L * cast(5 as bigint)) AS x5#2L]
       +- Range (1, 1000000, step=1, splits=Some(12))

    == Analyzed Logical Plan ==
    id: bigint, x5: bigint
    Filter (id#0L < cast(1000 as bigint))
    +- Project [id#0L, (id#0L * cast(5 as bigint)) AS x5#2L]
       +- Range (1, 1000000, step=1, splits=Some(12))

    == Optimized Logical Plan ==
    Project [id#0L, (id#0L * 5) AS x5#2L]
    +- Filter (id#0L < 1000)
       +- Range (1, 1000000, step=1, splits=Some(12))

    == Physical Plan ==
    *(1) Project [id#0L, (id#0L * 5) AS x5#2L]
    +- *(1) Filter (id#0L < 1000)
       +- *(1) Range (1, 1000000, step=1, splits=12)
    """
    numbers_x5.explain(True)  # explain(True) will give you all 4 query plans
    # filter occurs before the select => predicate pushdown


def demo_pushdown_on_group():
    numbers = spark.range(100000).withColumn("modulus", col("id") % 5)
    grouping = Window.partitionBy("modulus").orderBy("id")
    ranked = numbers.withColumn("rank", rank().over(grouping)).filter(col("modulus") > 3)
    ranked.explain(True)
    """
    == Parsed Logical Plan ==
    'Filter ('modulus > 3)
    +- Project [id#0L, modulus#2L, rank#7]
       +- Project [id#0L, modulus#2L, rank#7, rank#7]
          +- Window [rank(id#0L) windowspecdefinition(modulus#2L, id#0L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#7], [modulus#2L], [id#0L ASC NULLS FIRST]
             +- Project [id#0L, modulus#2L]
                +- Project [id#0L, (id#0L % cast(5 as bigint)) AS modulus#2L]
                   +- Range (0, 100000, step=1, splits=Some(12))
    
    == Analyzed Logical Plan ==
    id: bigint, modulus: bigint, rank: int
    Filter (modulus#2L > cast(3 as bigint))
    +- Project [id#0L, modulus#2L, rank#7]
       +- Project [id#0L, modulus#2L, rank#7, rank#7]
          +- Window [rank(id#0L) windowspecdefinition(modulus#2L, id#0L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#7], [modulus#2L], [id#0L ASC NULLS FIRST]
             +- Project [id#0L, modulus#2L]
                +- Project [id#0L, (id#0L % cast(5 as bigint)) AS modulus#2L]
                   +- Range (0, 100000, step=1, splits=Some(12))
    
    == Optimized Logical Plan ==
    Window [rank(id#0L) windowspecdefinition(modulus#2L, id#0L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#7], [modulus#2L], [id#0L ASC NULLS FIRST]
    +- Project [id#0L, (id#0L % 5) AS modulus#2L]
       +- Filter ((id#0L % 5) > 3) <----- here Spark is very smart
          +- Range (0, 100000, step=1, splits=Some(12))
    
    == Physical Plan ==
    Window [rank(id#0L) windowspecdefinition(modulus#2L, id#0L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#7], [modulus#2L], [id#0L ASC NULLS FIRST]
    +- *(2) Sort [modulus#2L ASC NULLS FIRST, id#0L ASC NULLS FIRST], false, 0
       +- Exchange hashpartitioning(modulus#2L, 200), ENSURE_REQUIREMENTS, [plan_id=19]
          +- *(1) Project [id#0L, (id#0L % 5) AS modulus#2L]
             +- *(1) Filter ((id#0L % 5) > 3)
                +- *(1) Range (0, 100000, step=1, splits=12)
    """

def read_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5443/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table_name) \
        .load()

def demo_pushdown_jdbc():
    salaries_df = read_table("salaries")
    employees_df = read_table("employees")
    salaries_df = salaries_df.groupby('emp_no').max('salary')
    employees_df = employees_df.join(salaries_df, on='emp_no', how='left')
    max_salaries_df = employees_df\
        .select(col('first_name'), col('last_name'), col('max(salary)'))\
        .filter("hire_date > '1999-01-01'")
    max_salaries_df.explain(True)
    """
    == Parsed Logical Plan ==
    'Filter ('hire_date > 1999-01-01)
    +- Project [first_name#10, last_name#11, max(salary)#25]
       +- Project [emp_no#8, birth_date#9, first_name#10, last_name#11, gender#12, hire_date#13, max(salary)#25]
          +- Join LeftOuter, (emp_no#8 = emp_no#0)
             :- Relation [emp_no#8,birth_date#9,first_name#10,last_name#11,gender#12,hire_date#13] JDBCRelation(public.employees) [numPartitions=1]
             +- Aggregate [emp_no#0], [emp_no#0, max(salary#1) AS max(salary)#25]
                +- Relation [emp_no#0,salary#1,from_date#2,to_date#3] JDBCRelation(public.salaries) [numPartitions=1]
    
    == Analyzed Logical Plan ==
    first_name: string, last_name: string, max(salary): int
    Project [first_name#10, last_name#11, max(salary)#25]
    +- Filter (hire_date#13 > cast(1999-01-01 as date))  <--- pushdown #1
       +- Project [first_name#10, last_name#11, max(salary)#25, hire_date#13]
          +- Project [emp_no#8, birth_date#9, first_name#10, last_name#11, gender#12, hire_date#13, max(salary)#25]
             +- Join LeftOuter, (emp_no#8 = emp_no#0)
                :- Relation [emp_no#8,birth_date#9,first_name#10,last_name#11,gender#12,hire_date#13] JDBCRelation(public.employees) [numPartitions=1]
                +- Aggregate [emp_no#0], [emp_no#0, max(salary#1) AS max(salary)#25]
                   +- Relation [emp_no#0,salary#1,from_date#2,to_date#3] JDBCRelation(public.salaries) [numPartitions=1]
    
    == Optimized Logical Plan ==
    Project [first_name#10, last_name#11, max(salary)#25]
    +- Join LeftOuter, (emp_no#8 = emp_no#0)
       :- Project [emp_no#8, first_name#10, last_name#11]
       :  +- Filter (isnotnull(hire_date#13) AND (hire_date#13 > 1999-01-01)) <---- pushdown #2
       :     +- Relation [emp_no#8,birth_date#9,first_name#10,last_name#11,gender#12,hire_date#13] JDBCRelation(public.employees) [numPartitions=1]
       +- Aggregate [emp_no#0], [emp_no#0, max(salary#1) AS max(salary)#25]
          +- Project [emp_no#0, salary#1]
             +- Filter isnotnull(emp_no#0)
                +- Relation [emp_no#0,salary#1,from_date#2,to_date#3] JDBCRelation(public.salaries) [numPartitions=1]
    
    == Physical Plan ==
    *(5) Project [first_name#10, last_name#11, max(salary)#25]
    +- *(5) SortMergeJoin [emp_no#8], [emp_no#0], LeftOuter
       :- *(2) Sort [emp_no#8 ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(emp_no#8, 200), ENSURE_REQUIREMENTS, [plan_id=36]
       :     +- *(1) Scan JDBCRelation(public.employees) [numPartitions=1] [emp_no#8,first_name#10,last_name#11] PushedFilters: [*IsNotNull(hire_date), *GreaterThan(hire_date,1999-01-01)], ReadSchema: struct<emp_no:int,first_name:string,last_name:string>
                                                                                                                                                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ pushdown #3
       +- *(4) Sort [emp_no#0 ASC NULLS FIRST], false, 0
          +- *(4) HashAggregate(keys=[emp_no#0], functions=[max(salary#1)], output=[emp_no#0, max(salary)#25])
             +- Exchange hashpartitioning(emp_no#0, 200), ENSURE_REQUIREMENTS, [plan_id=42]
                +- *(3) HashAggregate(keys=[emp_no#0], functions=[partial_max(salary#1)], output=[emp_no#0, max#39])
                   +- *(3) Scan JDBCRelation(public.salaries) [numPartitions=1] [emp_no#0,salary#1] PushedFilters: [*IsNotNull(emp_no)], ReadSchema: struct<emp_no:int,salary:int>
    """
    # JDBC, parquet, CSV, Avro
    # all hash partitions are split into a finite number = spark.sql.shuffle.partitions by default 200
    employees_df.repartition(100, col("emp_no"))


# partition_(row) = hash(key of that row) % 200
# [ (99% of rows => hash(row) = 1 , 1% => hash(row) = 2] [all rows => hash(row) = 2]

if __name__ == '__main__':
    read_table("employees").repartition(100, col("emp_no")).explain()
    sleep(999999)

