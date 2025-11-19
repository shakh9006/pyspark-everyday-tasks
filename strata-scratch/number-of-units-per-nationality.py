# Source: https://platform.stratascratch.com/coding/10156-number-of-units-per-nationality?code_type=6

# Title: Number Of Units Per Nationality

"""
We have data on rental properties and their owners.
Write a query that figures out how many different
apartments (use unit_id) are owned by people under 30,
broken down by their nationality. We want to see which
nationality owns the most apartments, so make sure to sort the results accordingly.
"""


import pyspark
import pyspark.sql.functions as F

result = (
    airbnb_units
    .join(airbnb_hosts, airbnb_units.host_id == airbnb_hosts.host_id, "inner")
    .filter((F.col("age") < 30) & (F.col("unit_type") == "Apartment"))
    .groupBy("nationality")
    .agg(
        F.countDistinct("unit_id").alias("apartment_count")
    )
)

result.toPandas()
