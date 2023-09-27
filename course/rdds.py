
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("RDDs") \
    .getOrCreate()

# RDD = resilient distributed dataset

# low-level entry point to the RDD API
# Spark Context
sc = spark.sparkContext

# create an RDD off a local collection
numbers = range(1, 1000000)
numbers_rdd = sc.parallelize(numbers) # RDD[Int]

# create an RDD off text files
stocks_rdd_text = sc.textFile("../data/stocks/aapl.csv")

"""
    Transformations are lazy
    - filter - takes a lambda that returns a boolean
    - map - takes a lambda on an element
    - min, max: only for RDDs of comparable types
    - reduce - collapses an RDD to a single value by a combiner function
    - groupBy - takes a lambda from item to a "key"
    - groupByKey, aggregateByKey, reduceByKey -  applies to an RDD of tuples
"""
even_numbers_rdd = numbers_rdd.filter(lambda x: x % 2 == 0) # returns a new RDD
tenx_numbers_rdd = numbers_rdd.map(lambda x: x * 10)
# [1,2,3].map(x * 10) => [10,20,30]
expanded_numbers_rdd = numbers_rdd.flatMap(lambda x: [x, x * 10])
# [1,2,3].flatMap([x,x*10]) => [[1,10],[2,20],[3,30]] => [1,10,2,20,3,30]
top_number = numbers_rdd.max()
sum_numbers = numbers_rdd.reduce(lambda x, y: x + y)
even_odd_numbers_rdd = numbers_rdd.groupBy(lambda x: x % 2)

laptops_rdd = sc.parallelize([
    ("work", "MacBook Pro"),
    ("gaming", "HP Omen"),
    ("work", "Lenonvo ThinkPad"),
    ("gaming", "Alienware X16")
])

laptop_list_rdd = laptops_rdd.groupByKey() # RDD [("work", [macbook, thinkpad]), (gaming, [HP, alienware]))
def print_laptop_groups():
    for (kind, result) in laptop_list_rdd.collect():
        print(kind + ":")
        print(list(result))


# convert between a DF and RDD (expensive in Python)
cars_df = spark.read.json("../data/cars")
cars_rdd = cars_df.rdd  # RDD of Spark Rows
car_names_rdd = cars_rdd.map(lambda row: row.Name)
cars_df_from_rdd = spark.createDataFrame(cars_rdd)  # returns a DF, only works for RDD of Rows!

# advice: stick to ONE layer (either DFs or RDDs), do not convert

"""
    Exercises:
    - read the movies file as an RDD of Strings (sc.textFile)
    - read the movies file as an RDD of Rows (after reading the DF)
    - using the movies RDD, show the average rating of movies by genre
"""
from functools import reduce

def compute_avg_rating(group):
    genre = group[0]
    movies = group[1]
    ratings = [movie.IMDB_Rating for movie in movies]
    avg_rating = reduce(lambda a,b: a + b, ratings) / len(ratings)
    return Row(genre=genre, average=avg_rating)

def solutions_shree():
    movies_rdd_text = sc.textFile("../data/movies/movies.json") # RDD of strings

    movies_df = spark.read.json("../data/movies").filter(F.col("IMDB_Rating").isNotNull() & F.col("Major_Genre").isNotNull())
    movies_rdd = movies_df.rdd
    movies_df_from_rdd = spark.createDataFrame(movies_rdd)

    # DF API for avg raging by genre
    movies_avg_df = movies_df \
        .groupBy("Major_Genre") \
        .avg("IMDB_Rating")

    grouped_by_genre_rdd = movies_rdd.groupBy(lambda row: row.Major_Genre) # RDD[(String, List[Row])
    avg_rating_rdd = grouped_by_genre_rdd.map(lambda tuple: compute_avg_rating(tuple))
    movies_avg_df_2 = spark.createDataFrame(avg_rating_rdd)

    movies_avg_df.show()
    movies_avg_df_2.show()

if __name__ == '__main__':
    solutions_shree()


"""
    financial_performance(dept_no, profit)
    
    departments
    dept_emp
    
    count_by_dept = deparments.join(dept_emp, "dept_no") => employee numbers for every department
        .groupBy("dept_no")
        .count() // (dept_no, count_of_employees)
        
    count_by_dept.join(financial_performance, "dept_no")
    |||||||||||||      ^^^^^^^^^^^^^^^^^^^^^ was not partitioned by dept_no, it will be shuffled!
    ^^^^^^^^^^^^^ already partitioned by dept_no, not to be shuffled again!
"""