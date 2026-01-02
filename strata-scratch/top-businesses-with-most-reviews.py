# Source: https://platform.stratascratch.com/coding/10048-top-businesses-with-most-reviews?code_type=6

# Title: Top Businesses With Most Reviews

"""
Find the top 5 businesses with most reviews. Assume that each row has a unique business_id such that the total reviews for each business is listed on each row.
Output the business name along with the total number of reviews and order your results by the total reviews in descending order.


If there are ties in review counts, businesses with the same number of reviews receive the same rank, and subsequent ranks are skipped accordingly
 (e.g., if two businesses tie for rank 4, the next business receives rank 6, skipping rank 5).
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.col("review_count").desc())

yelp_business = (
    yelp_business
    .groupBy("name")
    .agg(
        F.sum("review_count").alias("review_count")
    )
    .withColumn("rn", F.rank().over(window_spec))
    .filter(F.col("rn") <= 5)
    .select("name", "review_count")
)

yelp_business.toPandas()