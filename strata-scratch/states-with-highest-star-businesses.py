# Source: https://platform.stratascratch.com/coding/10046-top-5-states-with-5-star-businesses?code_type=6

# Title: Top 5 States With 5 Star Businesses

"""
Find the top 5 states with the most 5-star businesses.
Return the state name and the number of 5-star businesses.
Rank states by the number of 5-star businesses in descending order.
States with the same total should share the same rank, and the next rank should increase
by one (without skipping numbers). If multiple states are tied at the same rank, include all of them in the output.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.col("n_businesses").desc())

yelp_business = (
    yelp_business
    .filter(F.col("stars") == 5)
    .select("state", "stars")
    .groupBy("state")
    .agg(
        F.count("stars").alias("n_businesses")
    )
    .withColumn(
        "rn",
        F.dense_rank().over(window_spec)
    )
    .filter(F.col("rn") < 6)
    .select("state", "n_businesses")
)

yelp_business.toPandas()