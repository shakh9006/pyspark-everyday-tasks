# Source: https://platform.stratascratch.com/coding/10060-top-cool-votes?code_type=6

# Title: Top Cool Votes

"""
Find the review_text that received the highest number of  cool votes.
Output the business name along with the review text with the highest number of cool votes.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.col("cool").desc())

yelp_reviews = (
    yelp_reviews
    .withColumn("rn", F.rank().over(window_spec))
    .filter(F.col("rn") == 1)
    .select("business_name", "review_text")
)

yelp_reviews.toPandas()

