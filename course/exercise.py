from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import sleep

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .appName("Spark Essentials") \
    .getOrCreate()

"""
    We have a online store selling used gaming laptops.
    Every laptop "state"/config has
    - an id
    - make, model
    - perf score ("procSpeed") 
    
    We have a "marketplace" with people selling actual gaming laptops.
    - make, model
    - perf score ("procSpeed")
    - price (USD)
    
    We want to make a search tool, so that when someone clicks on a "laptop" (make, model, state),
    we would like to show "similar laptops" (items in the "offers" DF with same make/model, with perf score <= +/-0.1 difference)
    
    1. For every item in "laptops", we want the average price of "similar laptops"
    2. Optimize the most out of that job.
"""

def solution_szabi():
    laptop_df = spark.read.format('csv').option("header", "true").load("../data/laptops")
    offer_df = spark.read.format('csv').option("header", "true").load("../data/offers")

    combined = (laptop_df.join(
        offer_df,
        on=((laptop_df.model == offer_df.model) & (laptop_df.make == offer_df.make) & (
                    laptop_df.procSpeed >= offer_df.procSpeed - 0.1) & (
                        laptop_df.procSpeed <= offer_df.procSpeed + 0.1)),
        how="left"))

    avg_price = combined.groupBy(col('registration')).agg(avg("salePrice"))
    avg_price.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[registration#17], functions=[avg(cast(salePrice#45 as double))])
       +- Exchange hashpartitioning(registration#17, 200), ENSURE_REQUIREMENTS, [plan_id=74]
          +- HashAggregate(keys=[registration#17], functions=[partial_avg(cast(salePrice#45 as double))])
             +- Project [registration#17, salePrice#45]
                +- SortMergeJoin [model#19, make#18], [offerModel#51, offerMake#50], LeftOuter, ((cast(procSpeed#20 as double) >= (cast(offerprocSpeed#52 as double) - 0.1)) AND (cast(procSpeed#20 as double) <= (cast(offerprocSpeed#52 as double) + 0.1)))
                   :- Sort [model#19 ASC NULLS FIRST, make#18 ASC NULLS FIRST], false, 0
                   :  +- Exchange hashpartitioning(model#19, make#18, 200), ENSURE_REQUIREMENTS, [plan_id=66]
                   :     +- FileScan csv [registration#17,make#18,model#19,procSpeed#20] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<registration:string,make:string,model:string,procSpeed:string>
                   +- Sort [offerModel#51 ASC NULLS FIRST, offerMake#50 ASC NULLS FIRST], false, 0
                      +- Exchange hashpartitioning(offerModel#51, offerMake#50, 200), ENSURE_REQUIREMENTS, [plan_id=67]
                         +- Project [make#42 AS offerMake#50, model#43 AS offerModel#51, procSpeed#44 AS offerprocSpeed#52, salePrice#45]
                            +- Filter ((isnotnull(model#43) AND isnotnull(make#42)) AND isnotnull(procSpeed#44))
                               +- FileScan csv [make#42,model#43,procSpeed#44,salePrice#45] Batched: false, DataFilters: [isnotnull(model#43), isnotnull(make#42), isnotnull(procSpeed#44)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(model), IsNotNull(make), IsNotNull(procSpeed)], ReadSchema: struct<make:string,model:string,procSpeed:string,salePrice:string>
    """
    # avg_price.show() # takes forever! (15 mins!)

    # data skew! Razer Blade occurs in a disproportionate amount!
    # hashpartition(make, model) =>
    #   HP ... => 1000 rows => task 1
    #   Lenovo => 1500 rows => task 2
    #   Alienware => 1300 rows => task 3
    #   Razer => 200000 rows => task 4 (straggler task) -> causes the running time!


    # techinque to optimize non-equi-joins
    laptops2 = laptop_df.withColumn("procSpeed", explode(array(col("procSpeed"), col("procSpeed") - 0.1, col("procSpeed") + 0.1)))
    joined2 = laptops2.join(offer_df, ["make", "model", "procSpeed"]) \
        .groupBy("registration") \
        .agg(avg("salePrice").alias("RESULT"))

    # hashpartition(make, model, procSpeed)
    #   HP ... procSpeed 4.4 => task 1
    #   HP ... procSpeed 4.3 => task 2...
    #   Razer 4.3 => task 13
    #   Razer 4.5 => ...
    # straggler tasks are now split in multiple smaller ones

    joined2.explain()
    # joined2.show() # 3 mins

    # Salting - add a fictitious column in an interval (e.g. 1-10)
    #   - one DF needs to have ALL salt values => explode the DF with all possible values
    #   - the other DF needs to have ONE of the salt values => generate randomly (in the interval)
    # then do a join with (...) + the salt => the straggler task will be split in (in this case 10) smaller tasks
    # then do your computations later
    laptops3 = laptop_df \
        .withColumn("procSpeed", explode(array(col("procSpeed"), col("procSpeed") - 0.1, col("procSpeed") + 0.1))) \
        .withColumn("salt", explode(array(lit(1),lit(2),lit(3),lit(4),lit(5),lit(6),lit(7))))
    laptops3.show()
    offers3 = offer_df.withColumn("salt", ceil(rand() * 7))
    offers3.show()
    joined3 = laptops3.join(offers3, ["make", "model", "procSpeed", "salt"]) \
        .groupBy("registration") \
        .agg(avg("salePrice").alias("RESULT"))
    joined3.show()


def szabi_solution_2():
    laptop_df = spark.read.format('csv').option("header", "true").load(
        "../data/laptops") \
        .repartition("make")
    offer_df = spark.read.format('csv').option("header", "true").load(
        "../data/offers") \
        .groupBy(
        col("make").alias("offerMake"),
        col("model").alias("offerModel"),
        col("procSpeed").alias("offerProcSpeed")
    ).agg(
        avg("salePrice").alias("salePrice"),
        count("salePrice").alias("count")
    )

    combined = (laptop_df.join(
        offer_df,
        on=((laptop_df.model == offer_df.offerModel) & (laptop_df.make == offer_df.offerMake) & (
                    laptop_df.procSpeed >= offer_df.offerProcSpeed - 0.1) & (
                        laptop_df.procSpeed <= offer_df.offerProcSpeed + 0.1)),
        how="left"))

    avg_price = combined.groupBy(col('registration')).agg(sum(col("salePrice") * col("count")) / sum("count"))
    avg_price.show() # takes < 1m!


if __name__ == '__main__':
    solution_szabi()
    sleep(99999999)
