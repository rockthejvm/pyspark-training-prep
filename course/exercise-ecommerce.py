from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import time
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.sql.adaptive.enabled", "false") \
    .appName("Spark Exercise - eCommerce") \
    .getOrCreate()

"""
    Exercise - fictitious eCommerce platform
    Tasks
        - total revenue by customer id, in descending order
        - total revenue by product category in descending order
        
    Write the simplest code that does the job, then optimize it.
"""

def basic():
    orders_df = spark.read.option("header", "true").csv("../data/shopping/orders")
    customers_df = spark.read.option("header", "true").csv("../data/shopping/customers")
    products_df = spark.read.option("header", "true").csv("../data/shopping/products")
    categories_df = spark.read.option("header", "true").csv("../data/shopping/categories")

    revenue_by_customer = (
        orders_df
        .join(customers_df, 'customer_id')
        .join(products_df, 'product_id')
        .selectExpr('customer_id', 'order_amount * price as total_amount')
        .groupBy('customer_id')
        .agg(sum('total_amount').alias('total_revenue'))
        .orderBy(col('total_revenue').desc())
    )

    revenue_by_category = (
        orders_df
        .join(products_df, 'product_id')
        .join(categories_df, 'category_id')
        .selectExpr('category_id', 'category_name', 'order_amount * price as total_amount')
        .groupBy('category_id', 'category_name')
        .agg(sum('total_amount').alias('total_revenue'))
        .orderBy(col('total_revenue').desc())
    )

    start_time = time()
    revenue_by_customer.show()
    revenue_by_category.show()
    print(f"Total time: {time() - start_time} seconds")
    # 13s total (Daniel's machine)



if __name__ == '__main__':
    basic()