# Source: https://platform.stratascratch.com/coding/10049-reviews-of-categories?code_type=6

# Title: Reviews of Categories

"""
Calculate number of reviews for every business category.
Output the category along with the total number of reviews.
Order by total reviews in descending order.
"""


import pyspark
import pyspark.sql.functions as F

yelp_business = (
    yelp_business
    .withColumn(
        "category",
        F.explode(
            F.split(F.col("categories"), ';')
        )
    )
    .groupBy("category")
    .agg(
        F.sum("review_count").alias("review_cnt")
    )
    .orderBy(F.col("review_cnt").desc())
)

yelp_business.toPandas()