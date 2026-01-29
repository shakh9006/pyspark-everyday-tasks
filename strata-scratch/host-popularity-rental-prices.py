# Source: https://platform.stratascratch.com/coding/9632-host-popularity-rental-prices?code_type=6

# Title: Host Popularity Rental Prices

"""
You are given a table named airbnb_host_searches that contains listings shown to users during Airbnb property searches.
Each record represents a property listing (not the user's search query).
Determine the minimum, average, and maximum rental prices for each host popularity rating based on the property's number_of_reviews.


The host’s popularity rating is defined as below:
•   0 reviews: "New"
•   1 to 5 reviews: "Rising"
•   6 to 15 reviews: "Trending Up"
•   16 to 40 reviews: "Popular"
•   More than 40 reviews: "Hot"


Tip: The id column in the table refers to the listing ID.


Output host popularity rating and their minimum, average and maximum rental prices. Order the solution by the minimum price.
"""


import pyspark
import pyspark.sql.functions as F

airbnb_host_searches = (
    airbnb_host_searches
    .withColumn(
        "host_popularity",
        F.when(F.col("number_of_reviews") == 0, 'New')
        .when(F.col("number_of_reviews") < 6, 'Rising')
        .when(F.col("number_of_reviews") < 16, 'Trending Up')
        .when(F.col("number_of_reviews") < 41, 'Popular')
        .otherwise('Hot')
    )
    .groupBy("host_popularity")
    .agg(
        F.min("price").alias("min_price"),
        F.avg("price").alias("avg_price"),
        F.max("price").alias("max_price")
    )
    .orderBy(F.col("min_price"))
)

airbnb_host_searches.toPandas()