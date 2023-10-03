from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
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

# TODO: call UDFs from the JVM
# TODO: demo UDAFs

if __name__ == '__main__':
    car_names_converted_df.show()

