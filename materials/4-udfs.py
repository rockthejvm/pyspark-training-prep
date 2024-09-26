############################################################
# UDFs
############################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# [1] add postgres jar
spark = SparkSession \
    .builder \
    .master("local") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .config("spark.jars", "../jars/swissre_spark_udf_example_jar/swissre-spark-udf-example.jar") \
    .config("spark.sql.warehouse.dir", "../spark-warehouse") \
    .appName("Spark Essentials") \
    .getOrCreate()

# backing DFs
cars_df = spark.read.json("../data/cars")

# used for executing transformations that the native Spark DF/SQL API doesn't support

# example:
# car names are all lowercase, we'd like the names to be "official", i.e. capital letters on all words
def convert_case(name):
    arr = name.split(" ")
    return " ".join([x[0].upper() + x[1:] for x in arr if len(x) > 0])

convert_case_udf = udf(lambda x: convert_case(x), StringType())

cars_names_formatted_df = cars_df.select(convert_case_udf(col("Name")))
cars_names_formatted_df.show()

# question: what kind of transformation is a UDF (narrow or wide)?

"""
    Exercise:
    1. Write a UDF that approximates the total profits of a with more human-readable forms, up to 3 significant digits
        - 12345 = 12.3k
        - 123456 = 123k
        - 1234567 = 1.23m
        - 12345678 = 12.3m
        Use the suffixes "" (less than thousands), "k" for thousands, "m" for millions, "b" for billions
        Then print all movies with their corresponding profits formatted in this way.
    2. Use the same UDF on the purchases DF, count how many items were sold of each category.
"""


def format_profit(number):
    if number is None:
        return ""
    profit_number = str(number)
    fnumber = float(number)
    body = ""
    suffix = ""
    if len(profit_number) < 4:
        suffix = ""
        body = round(fnumber, 2)
    elif len(profit_number) >= 4 and len(profit_number) < 7:
        suffix = "k"
        body = round(fnumber/1000, 2)
    elif len(profit_number) >= 7 and len(profit_number) < 10:
        suffix = "m"
        body = round(fnumber/1000000, 2)
    elif len(profit_number) >= 10:
        suffix = "b"
        body = round(fnumber/1000000000, 2)
    formatted_number = f"{body}{suffix}"
    return formatted_number

format_profit_udf = udf(format_profit, StringType())
movies_df = spark.read.json("../data/movies") \
    .filter(col("US_Gross").isNotNull()) \
    .select(col("Title"), format_profit_udf(col("US_Gross")).alias("Profit_Sci"))

############################################################
# UDAFs
############################################################

# !!!!!!!!! VERY IMPORTANT - install pandas, pyarrow before running this example !!!!!!!!!!
from pyspark.sql.functions import pandas_udf, PandasUDFType

def demo_udaf():
    def sub_mean(pandas_df):
        return pandas_df.assign(Rating_Diff=pandas_df.IMDB_Rating - pandas_df.IMDB_Rating.mean())

    movies_df_2 = spark.read.json("../data/movies") \
        .filter(col("Major_Genre").isNotNull() & col("IMDB_Rating").isNotNull()) \
        .select("Title", "Major_Genre", "IMDB_Rating") \
        .withColumn("Rating_Diff", lit(0.0))

    subtract_mean = pandas_udf(sub_mean, movies_df_2.schema, PandasUDFType.GROUPED_MAP)
    diff_vs_mean_by_genre_df = movies_df_2.groupBy("Major_Genre").apply(subtract_mean)
    diff_vs_mean_by_genre_df.show()

def demo_with_numerical_df():
    df = spark.range(0, 10 * 1000 * 1000).withColumn('id', (col('id') / 10000).cast('integer')).withColumn('v', rand())

    # Input/output are both a pandas.DataFrame
    def stm(pdf):
        return pdf.assign(v=pdf.v - pdf.v.mean())

    subtract_mean = pandas_udf(stm, df.schema, PandasUDFType.GROUPED_MAP)
    # or add the @pandas_udf(df.schema, PandasUDFType.GROUPED_MAP) at the top of stm

    df.groupBy('id').apply(subtract_mean).show()


def demo_udf_from_scala():
    df = spark.read.text("../data/text.txt")
    spark.udf.registerJavaFunction("rtjvm_count", "com.rockthejvm.Occurrences", IntegerType())
    df.selectExpr("value", "rtjvm_count(value)").show()


if __name__ == '__main__':
    demo_udf_from_scala()