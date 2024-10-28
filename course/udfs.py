from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("UDFs") \
    .getOrCreate()

# your own DF function (user-defined function aka UDF)
# UDFs are narrow transformations
def convert_case(text):
    words = text.split(" ")
    return " ".join([word[0].upper() + word[1:] for word in words if len(word) > 0])

def demo_udf():
    cars_df = spark.read.json("../data/cars")
    convert_case_udf = udf(convert_case, StringType())
    cars_formatted_df = cars_df.select(col("Name"), convert_case_udf(col("Name")).alias("Name_Formatted"), col("Miles_per_Gallon"))
    cars_formatted_df.show()

# user-defined aggregate function (UDAF)
# pandas, pyarrow

# for every movie, show the diff between their IMDB rating and the avg rating of their genre
def demo_udaf():
    # step 1 - define the function working on Pandas
    def diff_vs_mean(pandas_df):
        return pandas_df.assign(Rating_Diff=pandas_df.IMDB_Rating - pandas_df.IMDB_Rating.mean())

    # step 2 - prepare your DF (include the output column)
    movies_df = spark.read.json("../data/movies") \
        .filter(col("Major_Genre").isNotNull() & col("IMDB_Rating").isNotNull()) \
        .select("Title", "Major_Genre", "IMDB_Rating")\
        .withColumn("Rating_Diff", lit(0))

    # step 3 - register the UDAF with pandas_udf
    diff_mean_udaf = pandas_udf(diff_vs_mean, movies_df.schema, PandasUDFType.GROUPED_MAP)
    # step 4 - do your thing
    movies_with_diff_df = movies_df.groupBy("Major_Genre").apply(diff_mean_udaf)
    movies_with_diff_df.show()

# user-defined table functions, aka UDTF
# example: a function that for every number, computes the square
@udtf(returnType="num: int, squared: int")
class SquareNumbers:
    def eval(self, start, end):
        for num in range(start, end + 1):
            yield (num, num * num)

def demo_udtf():
    # square_udtf = udtf(SquareNumbers, returnType="num: int, squared: int")
    df = SquareNumbers(lit(1), lit(100)) # returns a DataFrame
    df.show()

if __name__ == '__main__':
    demo_udtf()