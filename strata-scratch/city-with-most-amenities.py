# Source: https://platform.stratascratch.com/coding/10572-city-with-most-amenities?code_type=6

# Title: City With Most Amenities

"""
You're given a dataset of searches for properties on Airbnb. For simplicity, each row represents a unique host.


Your task is to find the city whose hosts collectively list the greatest total number of amenities across all their properties.


Treat amenities as a comma-separated list and count each listed entry as-is, even if the same amenities appear multiple times within
the same property's amenities, count each occurrence (do not deduplicate).


If multiple cities tie for the highest total, return return all of those cities. Output the name of the city/cities.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.desc("total"))

airbnb_search_details = airbnb_search_details.withColumn(
    "ct",
    F.size(
        F.split(
            F.regexp_replace(F.col("amenities"), r"[{}]", ""),
            ","
        )
    )
)

result = (
    airbnb_search_details
    .groupBy("city")
    .agg(
        F.sum("ct").alias("total")
    )
    .withColumn("rn", F.rank().over(window_spec))
    .filter(F.col("rn") == 1)
    .select("city")
)


result.toPandas()
