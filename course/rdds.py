from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.types import Row

import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# SparkSession = "entry point" to the DF/SQL API
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("SparkPlayground") \
    .getOrCreate()

cars_df = spark.read.json("../data/cars")
cars_df.createOrReplaceTempView("cars")
car_names_df = spark.sql("select Name from cars")
car_names_df_v2 = cars_df.select("Name")

# SparkContext = "entry point" for RDDs
sc = spark.sparkContext

# create an RDD of numbers
numbers = range(1000000)
numbers_rdd = sc.parallelize(numbers) # RDD of numbers

# process an RDD by using FP
even_numbers_rdd = numbers_rdd.filter(lambda x: x % 2 == 0) # RDD of ints, with the numbers satisfying the lambda
tenx_numbers_rdd = numbers_rdd.map(lambda x: x * 10)
expanded_numbers_rdd = numbers_rdd.flatMap(lambda x: range(x)) # every element is transformed to a collection, all collections are "concatenated"
sum_numbers = numbers_rdd.reduce(lambda x,y: x + y)
# [1,2,3,4,5] => [(0, [2,4]), (1, [1,3,5])]
grouped_numbers_rdd = numbers_rdd.groupBy(lambda x: x % 2) # RDD[(key, [values for that group])]
# [(0, [2,4]), (1, [1,3,5])] => [(0, 6), (1, 9)]
sum_by_group = grouped_numbers_rdd.reduceByKey(lambda x,y: x + y) # RDD[(key, result)]

# partitioning, transformations (lazy), actions (triggers) (take, collect, max, ...)

# DataFrames <-> RDDs
# DF -> RDD of Spark Rows
cars_rdd = cars_df.rdd # disastrous for perf!
# RDD (must be of Spark Rows) -> DF
cars_df_v2 = spark.createDataFrame(cars_rdd) # bad

"""
    - read the movies file as an RDD
    - average rating of movies by genre
"""

def computeAvgRating(group): # group: (genre, [movies])
    genre = group[0]
    movies = group[1]
    ratings = [movie.IMDB_Rating for movie in movies if movie.IMDB_Rating is not None]
    avgRating = reduce(lambda a,b: a + b, ratings) / len(ratings)
    return Row(genre=genre, rating=avgRating)

def demo_groupBy_df_equivalent():
    movies_df = spark.read.json("../data/movies")
    movies_rdd = movies_df.rdd # for convenience

    grouped_by_genre_rdd = movies_rdd.groupBy(lambda movie: movie.Major_Genre)
    ratings_by_genre_rdd = grouped_by_genre_rdd.map(computeAvgRating) # RDD[Row]
    ratings_by_genre_df = spark.createDataFrame(ratings_by_genre_rdd)
    ratings_by_genre_df.show()

if __name__ == '__main__':
    demo_groupBy_df_equivalent()