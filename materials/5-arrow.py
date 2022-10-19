from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
import numpy as np
import pandas as pd

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Arrow Demo") \
    .getOrCreate()

sc = spark.sparkContext

def demo_arrow():
    # Enable Arrow-based columnar data transfers
    # spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # Generate a Pandas DataFrame
    pdf = pd.DataFrame(np.random.rand(1000000, 3))

    # Create a Spark DataFrame from a Pandas DataFrame using Arrow
    df = spark.createDataFrame(pdf)
    df.show()

    # # Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
    # result_pdf = df.select("*").toPandas()
    #
    # print("Pandas DataFrame result statistics:\n%s\n" % str(result_pdf.describe()))

# performance demo only visible in the real app, not in the Spark UI
from time import sleep
from datetime import datetime

if __name__ == '__main__':
    print(datetime.now())
    demo_arrow()
    print(datetime.now()) # ~3s with arrow,
    sleep(1000)
