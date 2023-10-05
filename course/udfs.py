from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.jars", "../jars/swissre_spark_udf_example_jar/swissre-spark-udf-example.jar") \
    .appName("UDFs") \
    .getOrCreate()

# UDFs = custom functions for DataFrames

cars_df = spark.read.json("../data/cars")

# step 1 - write a python function
def convert_case(name):
    words = name.split(" ")
    words_upper = [word[0].upper() + word[1:] for word in words if len(word) > 0]
    return " ".join(words_upper)

# step 2 - register this function as a PySpark UDF
convert_case_udf = udf(lambda x: convert_case(x), StringType())

# step 3 - use the new "function" on a column object
car_names_converted_df = cars_df.select(convert_case_udf(col("Name")).alias("New_name"))


# UDFs are NARROW transformations, because the function is invoked on every row independently
# UDFs are impossible to optimize (by Spark) - UDFs are applied PER ROW, one at a time
# JVM UDFs (Java/Scala) are almost always faster than PySpark UDFs

# for every movie, show the diff between its IMDB rating and the avg rating of its genre
def demo_udaf():
    # step 1 - define an aggregate function on Pandas - this is applied on a group of rows (as a Pandas DF)
    def sub_mean(pandas_df):
        return pandas_df.assign(Rating_Diff=pandas_df.IMDB_Rating - pandas_df.IMDB_Rating.mean())

    movies_df = spark.read.json("../data/movies") \
        .filter(col("IMDB_Rating").isNotNull() & col("Major_Genre").isNotNull()) \
        .select("Title", "Major_Genre", "IMDB_Rating") \
        .withColumn("Rating_Diff", lit(0.0))

    # step 2 - register the aggregate function as a pandas_udf
    # subtract_mean = pandas_udf(sub_mean, movies_df.schema, PandasUDFType.GROUPED_MAP)
    # step 3 - apply the UDAF on a group
    diff_vs_mean_df = movies_df.groupby("Major_Genre").applyInPandas(sub_mean, movies_df.schema)
    diff_vs_mean_df.show()

def demo_udf_from_scala():
    df = spark.read.text("../data/stupid_text")  # DF with a single column called "value"
    spark.udf.registerJavaFunction("rockthejvm_occurrences", "com.rockthejvm.Occurrences", IntegerType())
    processed_df = df.selectExpr("value", "rockthejvm_occurrences(value)")
    processed_df.show()
    # JVM UDFs (defined in Scala/Java) will always be faster than PySpark UDFs

if __name__ == '__main__':
    demo_udf_from_scala()

