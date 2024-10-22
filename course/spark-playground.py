from pyspark.sql import SparkSession
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("SparkPlayground") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .getOrCreate()


def demo_app():
    df = spark.read.json("../data/cars")
    df.show()


def demo_db():
    # -- reading the data
    employeesDF = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public.employees") \
        .load()

    employeesDF.show()


if __name__ == "__main__":
    demo_app()
