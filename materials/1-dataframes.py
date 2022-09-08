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

if __name__ == "__main__":

    ###################################################################################################################
    # Dataframes basics
    #
    # Jump straight to exercises - if something's missing, go back and explain.
    ###################################################################################################################

    # load first DF

    movies_df = spark.read \
        .option("inferSchema", "true") \
        .format("json") \
        .option("path", "../data/movies") \
        .load()
    # show

    # -- schemas

    movies_df.printSchema()

    # enforce a schema
    from pyspark.sql.types import *

    stocksSchema = StructType([
        StructField("symbol", StringType()),
        StructField("date", DateType()),
        StructField("price", DoubleType())
    ])

    stocks_df = spark.read.format("csv").schema(stocksSchema).load("../data/stocks")

    # shorthand loading
    cars_df = spark.read.json(
        "../data/cars")  # shorthand for .option("inferSchema" ...).format("json").option("path" ...).load()

    ############################################################
    # Columns and expressions
    #
    # Jump straight to exercises. Fill in the gaps that arise:
    #   - aliases
    #   - column objects operators
    #   - selectExpr
    ############################################################

    # -- columns and expressions
    firstColumn = col("Name")  # calling the function returns a column object
    car_names = cars_df.select(firstColumn)
    # show
    car_names_hp = cars_df.select("Name", "Horsepower")
    # show
    car_weights = cars_df.select(firstColumn, col("Weight_in_lbs") / 2.2)
    # show

    # multiple ways of computing new columns
    # watch the expr thing
    # watch the alias method - this will give a proper column name to the resulting column
    cars_with_weights = cars_df.select(
        col("Name"),
        col("Weight_in_lbs"),
        (col("Weight_in_lbs") / 2.2).alias("Weight_in_kg"),
        expr("Weight_in_lbs / 2.2").alias("Weight_in_kg_2"),
        expr("Weight_in_lbs / 2.2 as Weight_in_kg_3"),
    )
    # show
    # also notice that the results of Weight_in_kg columns are slightly different;
    #      there are different implementations (col operators vs SQL operators)

    cars_with_weights_v2 = cars_df.selectExpr(
        "Name",
        "Weight_in_lbs",
        "Weight_in_lbs / 2.2"
    )

    # just mention (or write in the comments as you write the code):
    #   - withColumn, withColumnRenamed
    #   - column names with backticks, e.g. `Weight in pounds`
    #   - drop

    # filtering
    europeanCarsDF = cars_df.filter(
        col("Origin") != "USA"
    )  # col("Origin") != "USA" is also a column object; we filter by the truth of that column

    """
        Exercises

        1. Read the movies DF and select 2 columns of your choice
        2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
        3. Select all COMEDY movies with IMDB rating above 6

        Use as many versions as possible
    """

    # -- load
    movies_df = spark.read.option("inferSchema", "true").json("../data/movies")
    # show

    # -- 1

    # show each in turn
    movies_release_df = movies_df.select("Title", "Release_Date")
    movies_release_df2 = movies_df.select(col("Title"), col("Release_Date"))
    movies_release_df3 = movies_df.select(expr("Title"), expr("Release_Date"))
    movies_release_df4 = movies_df.selectExpr("Title", "Release_Date")

    # -- 2

    movies_profit_df = movies_df.select(
        col("Title"),
        col("US_Gross"),
        col("Worldwide_Gross"),
        col("US_DVD_Sales"),
        (col("US_Gross") + col("Worldwide_Gross")).alias("Total_Gross")
    )

    movies_profit_df2 = movies_df.selectExpr(
        "Title",
        "US_Gross",
        "Worldwide_Gross",
        "US_Gross + Worldwide_Gross as Total_Gross"
    )

    movies_profit_df3 = movies_df \
        .select("Title", "US_Gross", "Worldwide_Gross") \
        .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross")) \
 \
        # -- 3

    at_least_mediocre_comedies_df = movies_df \
        .select("Title", "IMDB_Rating") \
        .where((col("Major_Genre") == "Comedy") & (col("IMDB_Rating") > 6))

    at_least_mediocre_comedies_df2 = movies_df \
        .select("Title", "IMDB_Rating") \
        .where(col("Major_Genre") == "Comedy") \
        .where(col("IMDB_Rating") > 6)

    at_least_mediocre_comedies_df3 = movies_df \
        .select("Title", "IMDB_Rating") \
        .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

    ############################################################
    # Data sources and formats
    #
    # Show the flags step by step, as people might only work with Foundry.
    ############################################################

    # JSON flags

    # this one enforces a schema, make sure to bring the schema here
    # paste the column names, then do the IntelliJ trickery:
    # "Name", "Miles_per_Gallon", "Cylinders", "Displacement", "Horsepower", "Weight_in_lbs", "Acceleration", "Year", "Origin"
    cars_schema = StructType([
        StructField("Name", StringType()),
        StructField("Miles_per_Gallon", DoubleType()),
        StructField("Cylinders", LongType()),
        StructField("Displacement", DoubleType()),
        StructField("Horsepower", LongType()),
        StructField("Weight_in_lbs", LongType()),
        StructField("Acceleration", DoubleType()),
        StructField("Year", StringType()),
        StructField("Origin", StringType())
    ])

    spark.read \
        .schema(cars_schema) \
        .option("dateFormat", "YYYY-MM-dd") \
        .option("allowSingleQuotes", "true") \
        .option("compression", "uncompressed") \
        .json("../data/cars")

    # -- CSV flags

    """
    CSV flags:
      - schema
      - date formats: same as for JSON
      - header: true/false if the first row is the column names
      - separator: can be any character e.g. for TSV (tab-separated)
      - nullValue: empty values usually treated as nulls, but can also be valid values (empty string)
    """

    stocksSchema = StructType([
        StructField("symbol", StringType()),
        StructField("date", DateType()),
        StructField("price", DoubleType())
    ])

    stocksDF = spark.read \
        .schema(stocksSchema) \
        .option("dateFormat", "MMM d yyyy") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("nullValue", "") \
        .csv("../data/stocks")

    # reading a DF from a JDBC table
    # [1] add the postgres jar in the SparkSession
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/rtjvm"
    user = "docker"
    password = "docker"

    employees_df = spark.read \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", "public.employees") \
        .load()

    """
       Writing DFs
       - format
       - save mode = overwrite, append, ignore, errorIfExists
       - path
       - zero or more options
    """

    """
    Exercise: read the movies DF, then write it as
      - tab-separated values file with header
      - Parquet
      - table "public.movies" in the Postgres DB
    """

    # TSV
    movies_df.write \
        .format("csv") \
        .option("header", "true") \
        .option("sep", "\t")
    # .save("../data/movies-tab-csv") \

    # Parquet
    # movies_df.write.save("../data/movies-parquet")

    # save to table
    movies_df.write \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", "public.movies")
    # .save()

    ############################################################
    # Aggregations
    #
    # Jump straight to the exercises
    ############################################################

    # basic aggregation
    genresCountDF = movies_df.select(count(col("Major_Genre")))  # how many values the column Major_Genre has
    # show
    genresCountExpr = movies_df.selectExpr("count(Major_Genre)")
    # show
    # the aggregation function returns another column object

    # alternative: select the column and then count
    #   this will return the number of rows in the DF (therefore including all nulls)
    #   the count() function includes null just once (we'll deal with null in a dedicated section/lesson)

    # also available:
    #   - countDistinct
    #   - min, max
    #   - sum, avg (for numerical columns)
    #   - mean, stddev (for numerical columns, data analysis)

    # grouping
    count_by_genre_df = movies_df \
        .groupBy(col("Major_Genre")) \
        .count()  # call the aggregation function on the grouped dataset; can also use min(), max(), avg(),

    # multiple aggregations (students probably didn't know that)
    aggregations_by_genre_df = movies_df \
        .groupBy(col("Major_Genre")) \
        .agg(
        count("*").alias("N_Movies"),
        avg("IMDB_Rating").alias("Avg_Rating")
    )

    """
      Exercises
        1. Sum up ALL the profits of ALL the movies in the DF
        2. Count how many distinct directors we have
        3. Show the mean and standard deviation of US gross revenue for the movies
        4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
    """

    # -- 1
    # option 1: summing the 3 columns for every row
    # problem: if one is null the sum is null

    all_profits = movies_df \
        .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).alias("Total_Gross")) \
        .select(sum("Total_Gross"))

    # option 2: summing per column, then summing the results (better)
    all_profits_v2 = movies_df.select(
        (sum(col("US_Gross")) + sum(col("Worldwide_Gross")) + sum(col("US_DVD_Sales"))).alias("Total_Gross"))

    # -- 2

    nDirectors = movies_df.select(countDistinct(col("Director")))

    # -- 3

    profit_metrics = movies_df.select(mean("US_Gross"), stddev("US_Gross"))
    # 40 million avg, 60 million deviation - talk about distribution lol

    # -- 4
    averages_per_director = movies_df \
        .groupBy("Director") \
        .agg(
        avg("IMDB_Rating").alias("Avg_Rating"),
        sum("US_Gross").alias("Total_US_Gross")
    ) \
        .orderBy(col("Avg_Rating").desc_nulls_last())

    ############################################################
    # Joins
    #
    # Jump straight to exercises, show the left-semi/left-anti joins with exercise #2
    ############################################################

    # how many different kinds of joins do you know?

    guitarsDF = spark.read \
        .option("inferSchema", "true") \
        .json("../data/guitars")

    guitaristsDF = spark.read \
        .option("inferSchema", "true") \
        .json("../data/guitarPlayers")

    bandsDF = spark.read \
        .option("inferSchema", "true") \
        .json("../data/bands")

    # -- inner joins = all rows from the left and right DF for which the condition is true

    joinCondition = guitaristsDF.band == bandsDF.id  # DF columns can be accessed as fields: we'll talk more about datasets
    guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

    # outer joins
    # left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
    guitaristsDF.join(bandsDF, joinCondition, "left_outer")

    # right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
    guitaristsDF.join(bandsDF, joinCondition, "right_outer")

    # outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
    guitaristsDF.join(bandsDF, joinCondition, "outer")

    # semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
    guitaristsDF.join(bandsDF, joinCondition, "left_semi")

    # anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
    guitaristsDF.join(bandsDF, joinCondition, "left_anti")

    """
    Exercises
        1. show all employees and their max salary (all time)
        2. show all employees who were never managers
        3. for each employee, find the difference 
            between their own latest salary and the max salary of their job/department
    """


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
    salaries_df = readTable("salaries")
    dept_managers_df = readTable("dept_manager")
    titles_df = readTable("titles")
    dept_emp_df = readTable("dept_emp")
    departments_df = readTable("departments")

    # -- 1
    maxSalariesPerEmpNoDF = salaries_df.groupBy("emp_no").agg(max("salary").alias("maxSalary"))
    employeesSalariesDF = employees_df.join(maxSalariesPerEmpNoDF, "emp_no")
    employeesSalariesDF

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

    # show


