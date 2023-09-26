from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Spark Essentials") \
    .getOrCreate()

# step 0 - get a DF
cars_df = spark.read.json("../data/cars")

# step 1 - write you python function
def convert_case(name):
    words = name.split(" ")
    return " ".join([word[0].upper() + word[1:] for word in words if len(word) > 0])

# step 2 - declare custom function to work on every row
# needs to declare the type returned
convert_case_udf = udf(lambda x: convert_case(x), StringType())

# step 3 - use it just as you would on any other pyspark function
car_names_formatted_df = cars_df.select(convert_case_udf(col("Name")).alias("Better_Name"))

#  UDFs are narrow transformations!

if __name__ == '__main__':
    car_names_formatted_df.show()


