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

if __name__ == '__main__':
    demo_udf()