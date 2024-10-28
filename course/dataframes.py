from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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
    df2 = df.select(F.upper(df.Name), df.Weight_in_lbs, (df.Weight_in_lbs/2.2).alias("Weight_in_kgs"))
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

"""
    Exercise - read the movies DF and
    - sum up all the profits of all the movies
    - count how many distinct directors
    - mean/stddev for US gross revenue
    - average IMDB rating and average US gross revenue PER Director
"""

if __name__ == '__main__':
    read_movies_df()

# TODO - what's an RDD