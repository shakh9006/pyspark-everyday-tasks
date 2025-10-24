# Source: https://platform.stratascratch.com/coding/2056-number-of-shipments-per-month?code_type=6

# Title: Number of Shipments Per Month

"""
Write a query that will calculate the number of shipments per month.
The unique key for one shipment is a combination of shipment_id and sub_id.
Output the year_month in format YYYY-MM and the number of shipments in that month.
"""

import pyspark
import pyspark.sql.functions as F

amazon_shipment = amazon_shipment.withColumn(
    "year_month",
    F.date_format(F.col("shipment_date"), 'yyyy-MM')
)

amazon_shipment = (
    amazon_shipment
    .groupBy(F.col("year_month"))
    .agg(
        F.countDistinct(F.col("shipment_id"), F.col("sub_id")).alias("count")
    )
)

amazon_shipment.toPandas()