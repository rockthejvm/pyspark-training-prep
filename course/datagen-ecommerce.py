import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataGeneration") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Define schemas
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_timestamp", StringType(), False),
    StructField("order_amount", IntegerType(), False)
])

products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("category_id", IntegerType(), False),
    StructField("price", IntegerType(), False)
])

customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("location", StringType(), False),
    StructField("registration_date", StringType(), False)
])

categories_schema = StructType([
    StructField("category_id", IntegerType(), False),
    StructField("category_name", StringType(), False)
])

# Step 1: Generate Products Data
def generate_products(n):
    products = [(i, random.randint(1, 1000), random.randint(10, 500)) for i in range(1, n+1)]
    return spark.createDataFrame(products, schema=products_schema)

# Step 2: Generate Categories Data
def generate_categories(n):
    categories = [(i, f"Category_{i}") for i in range(1, n+1)]
    return spark.createDataFrame(categories, schema=categories_schema)

# Step 3: Generate Customers Data with Regional Skew
def generate_customers(n, power_law_factor=0.7):
    customers = []
    for i in range(1, n+1):
        if random.random() < power_law_factor:
            location = random.choice(["Region_1", "Region_2"])
        else:
            location = random.choice([f"Region_{i % 10 + 3}" for i in range(1, 10)])
        customers.append((i, location, f"2020-01-{random.randint(1, 30)}"))
    return spark.createDataFrame(customers, schema=customers_schema)

# Step 4: Generate Orders Data
def generate_orders(n_orders, n_customers, n_products, power_law_factor=0.1):
    customers_count = int(n_customers * power_law_factor)
    customers = [random.randint(1, customers_count) for _ in range(customers_count)]

    orders = []
    for i in range(1, n_orders+1):
        if random.random() < power_law_factor:
            customer_id = random.choice(customers)
        else:
            customer_id = random.randint(customers_count+1, n_customers)
        orders.append((i, random.randint(1, n_products), customer_id, f"2023-05-{random.randint(1, 31)}", random.randint(20, 500)))

    return spark.createDataFrame(orders, schema=orders_schema)

# Generate the datasets
n_orders = 10000000
n_products = 10000
n_customers = 100000
n_categories = 100
# with 10000000 orders, 10000 products, 100000 customers and 100 categories, we get unoptimized code 13s, optimized 7s

products_df = generate_products(n_products)
categories_df = generate_categories(n_categories)
customers_df = generate_customers(n_customers, power_law_factor=0.7)
orders_df = generate_orders(n_orders, n_customers, n_products, power_law_factor=0.1)

# Save datasets to disk for later use
orders_df.write.csv("../data/shopping/orders", mode="overwrite", header=True)
products_df.write.csv("../data/shopping/products", mode="overwrite", header=True)
customers_df.write.csv("../data/shopping/customers", mode="overwrite", header=True)
categories_df.write.csv("../data/shopping/categories", mode="overwrite", header=True)

print("Data generation complete.")
