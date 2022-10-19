from pyspark.sql import SparkSession
from time import sleep

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Spark Optimization") \
    .config("spark.sql.warehouse.dir", "../spark-warehouse") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

sc = spark.sparkContext

############################################################
# custom partitioners
############################################################

from pyspark.rdd import Partitioner, portable_hash

def demo_custom_partitioners():
    # partitioners
    numbers = sc.parallelize(range(1, 10000))
    print(numbers.partitioner)

    numbers_3 = numbers.repartition(3)  # uniform data redistribution
    print(numbers_3.partitioner)

    keyed_numbers = numbers.map(lambda n: (n % 10, n))  # RDD[(Int, Int)]
    hashed_numbers = keyed_numbers.partitionBy(3, portable_hash)
    print(hashed_numbers.partitioner)
    """
        Keys with the same hash stay on the same partition - function is hash % n_partitions
        Prerequisite for
          - combineByKey
          - groupByKey
          - aggregateByKey
          - foldByKey
          - reduceByKey

        Prerequisite for joins, when neither RDD has a known partitioner.
    """

    # in order to sort an RDD, Spark needs to partition it by a "range" partitioner: all keys under a range will be on the same partition
    # the partitioner object can't be identified in PySpark (can be in Scala and Java)
    sorted_numbers = keyed_numbers.sortByKey()

    # define your own partitioner
    import string
    import random
    def generate_random_words(n_words, max_length):
        letters = string.ascii_lowercase
        return [''.join(random.choice(letters) for _ in range(random.randint(1, max_length))) for _ in range(n_words)]

    random_words_rdd = sc.parallelize(generate_random_words(1000, 100))
    # repartition this RDD by the words length == two words of the same length will be on the same partition
    # custom computation = counting the occurrences of 'z' in every word
    z_words_rdd = random_words_rdd.map(lambda word: (word, word.count("z")))  # RDD[(String, Int)]

    # repartitioning by a custom by-length partitioner: all keys with the same length will be on the same partition
    by_length_z_words_rdd = z_words_rdd.partitionBy(100, lambda k: len(str(k)) % 100)

    def print_partition(partition):
        for tuple in partition:
            print(tuple)

    by_length_z_words_rdd.foreachPartition(print_partition)
    # some intermingled prints, but generally prints are grouped, which means that all strings with the same length are on the same partition

############################################################
# co-grouping RDDs
############################################################

"""
    Example: a database with student exams. A student is considered to pass if their max attempt is > 9.0.
    The database has 3 "tables":
    - ids
    - emails
    - scores

    In order to find which student is passed and email them, we need to do a 3-way join between these 3 "tables".
    Normal joins can only be performed one at a time.
    RDDs can be co-partitioned/co-grouped all at once, i.e. partitioned in the same way.
    After co-grouping, RDDs can be traversed in parallel for the relevant data.
    This is more powerful than what the DF API can offer.
"""


def read_ids():
    def line2tuple(line):
        tokens = line.split(" ")
        return int(tokens[0]), tokens[1]

    return sc.textFile("../data/exams/examIds.txt").map(line2tuple)


def read_exam_scores():
    def line2tuple(line):
        tokens = line.split(" ")
        return int(tokens[0]), float(tokens[1])

    return sc.textFile("../data/exams/examScores.txt").map(line2tuple)


def read_exam_emails():
    def line2tuple(line):
        tokens = line.split(" ")
        return int(tokens[0]), tokens[1]

    return sc.textFile("../data/exams/examEmails.txt").map(line2tuple)


def demo_plain_join():
    def tuple2finalMark(tuple):
        (_, maxAttempt), email = tuple
        if maxAttempt >= 9.0:
            return email, "PASSED"
        else:
            return email, "FAILED"

    # reduce by key: find the max exam attempt for every student id
    scores = read_exam_scores().reduceByKey(
        lambda a, b: True if a > b else False)  # reverse if-expression to be compatible with one-liner in lambda
    candidates = read_ids()
    emails = read_exam_emails()
    # 3-way join is expensive
    results = candidates.join(scores).join(emails).mapValues(tuple2finalMark)
    results.count()  # ~20s


def coGroupedJoin():
    scores = read_exam_scores().reduceByKey(
        lambda a, b: True if a > b else False)  # reverse if-expression to be compatible with one-liner in lambda
    candidates = read_ids()
    emails = read_exam_emails()

    # automatic repartitioning of the 3 RDDs by the same key
    result = candidates.groupWith(scores, emails)  # co-partitioning the 3 RDDs

    def iterable2FinalMark(iterables):
        _, maxAttemptIterable, emailIterable = iterables
        maxScore = next(iter(maxAttemptIterable))
        email = next(iter(emailIterable))
        if maxScore > 9.0:
            return email, "PASSED"
        else:
            return email, "FAILED"

    # each RDD has an iterable of values for the same ID (for our data, just one value)
    result.mapValues(iterable2FinalMark).count()  # 15s

############################################################
# RDD broadcast joins
############################################################

def demo_rdd_broadcast_joins():
    # small lookup table
    prizes = sc.parallelize([
        (1, "gold"),
        (2, "silver"),
        (3, "bronze")
    ])

    # the competition has ended - the leaderboard is known
    import string, random
    def generate_competitor_id():
        letters = string.ascii_lowercase
        return [''.join(random.choice(letters) for _ in range(8))]

    leaderboard = sc.parallelize(range(10000000)).map(lambda i: (i, generate_competitor_id()))
    medalists = leaderboard.join(prizes)
    # print(medalists.collect()) # 45s!

    """
        We know from SQL joins that the small RDD can be broadcast so that we can avoid the shuffle on the big RDD.
        However, for the RDD API, we'll have to do this manually.
        This lesson is more about how to actually implement the broadcasting technique on RDDs.
    """

    # need to collect the RDD locally, so that we can broadcast to the executors
    medals_map = prizes.collectAsMap()
    # after we do this, all executors can refer to the medalsMap locally
    sc.broadcast(medals_map)

    # need to avoid shuffles by manually going through the partitions of the big RDD
    def transform_medalists(partition):
        for competitor in partition:
            index, name = competitor
            medal = medals_map.get(index)
            if medal is not None:
                yield name, medal

    improved_medalists = leaderboard.mapPartitions(transform_medalists)
    print(improved_medalists.collect())  # 16s
    # just to brag: the Scala native RDD broadcasts takes this down to 2s


if __name__ == '__main__':
    sleep(10000)
