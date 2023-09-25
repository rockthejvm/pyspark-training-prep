from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("SparkPlayground") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .getOrCreate()


def demo_app():
    df = spark.read.json("../data/cars")
    df.show()

# using 5443 for port if you have other databases running
def demo_db():
    # -- reading the data
    employeesDF = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5443/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public.employees") \
        .load()

    employeesDF.show()

if __name__ == "__main__":
    demo_app()
