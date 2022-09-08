from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# RDD Essentials

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Spark Essentials - RDDs") \
    .getOrCreate()

# entry point: spark context
sc = spark.sparkContext

# create an RDD from local memory
numbers = range(1, 1000000)
numbersRDD = sc.parallelize(numbers)

# create RDD from text lines
sc.textFile("../data/stocks/stocks.csv")

# read from a DF
cars_df = spark.read.json("../data/cars")
carsRDD = cars_df.rdd  # converted to an RDD of rows = tuples with named fields

""" 
    Transformations on RDDs
    - filter(lambda which returns a boolean)
    - map(lambda from row to something else)
    - min, max: only for RDDs of comparable values
    - reduce(lambda x,y: associative operation)
    - groupBy(lambda from item to a key) - returns a grouped RDD
"""

"""

  /**
    * Exercises
    * 0. Read the movies file as an RDD.
    * 1. Select all the movies in the Drama genre with IMDB rating > 6.
    * 2. Show the average rating of movies by genre.
    */
"""

movies_df = spark.read.json("../data/movies")
moviesRDD = movies_df \
    .select(col("Title").alias("title"), col("Major_Genre").alias("genre"), col("IMDB_Rating").alias("rating")) \
    .where(col("genre").isNotNull() & col("rating").isNotNull()) \
    .rdd

# 3
goodDramasRDD = moviesRDD.filter(lambda movie: (movie.genre == "Drama") & (movie.rating > 6))
goodDramasRDD.take(5)

# 4: RDDs of tuples
# if you want to work with the Row data structure, use this import
# WARNING: make sure you import pyspark.sql.functions as an alias,
#   so as not to confuse the array sum() with the pyspark sum()

from pyspark.sql import Row
from functools import reduce


def computeAvgRating(group):
    genre = group[0]
    movies = group[1]
    ratings = [movie.rating for movie in movies]
    # the line below can be run with the Python sum function, but there's a name collision with PySpark sum
    avgRating = reduce(lambda a, b: a + b, ratings) / len(ratings)
    return Row(genre=genre, rating=avgRating)


groupedByGenreRDD = moviesRDD.groupBy(lambda x: x.genre)
avgRatingByGenreRDD = groupedByGenreRDD.map(lambda x: computeAvgRating(x)).collect()

# test:
# avgRatingByGenreRDD.collect() or
avgRatingByGenreDF = spark.createDataFrame(avgRatingByGenreRDD)
# show

# the DF option
avgRatingByGenreDF2 = spark.createDataFrame(moviesRDD).groupBy(col("genre")).avg("rating")
# show

"""
  reference:
    +-------------------+------------------+
    |              genre|       avg(rating)|
    +-------------------+------------------+
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |        Documentary| 6.997297297297298|
    |       Black Comedy|6.8187500000000005|
    |  Thriller/Suspense| 6.360944206008582|
    |            Musical|             6.448|
    |    Romantic Comedy| 5.873076923076922|
    |Concert/Performance|             6.325|
    |             Horror|5.6760765550239185|
    |            Western| 6.842857142857142|
    |             Comedy| 5.853858267716529|
    |             Action| 6.114795918367349|
    +-------------------+------------------+

  RDD:
    +-------------------+------------------+
    |              genre|            rating|
    +-------------------+------------------+
    |Concert/Performance|             6.325|
    |            Western| 6.842857142857142|
    |            Musical|             6.448|
    |             Horror|5.6760765550239185|
    |    Romantic Comedy| 5.873076923076922|
    |             Comedy| 5.853858267716529|
    |       Black Comedy|6.8187500000000005|
    |        Documentary| 6.997297297297298|
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |  Thriller/Suspense| 6.360944206008582|
    |             Action| 6.114795918367349|
    +-------------------+------------------+
"""