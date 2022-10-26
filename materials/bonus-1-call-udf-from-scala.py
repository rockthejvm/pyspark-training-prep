from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("spark://localhost:6066") \
    .config("spark.sql.warehouse.dir", "../spark-warehouse") \
    .appName("Spark Essentials") \
    .getOrCreate()


if __name__ == '__main__':
    spark.range(100).show()