# Source: https://platform.stratascratch.com/coding/10090-find-the-percentage-of-shipable-orders?code_type=6

# Title: Find the percentage of shipable orders

"""
Find the percentage of shipable orders.
Consider an order is shipable if the customer's address is known.
"""


import pyspark
from pyspark.sql.functions import col, when, count, sum

result = (
    orders.alias("o")
    .join(
        customers.alias("c"),
        on=col("o.cust_id") == col("c.id"),
        how="inner"
    )
    .select(col("o.id"), col("o.cust_id"), col("c.address"))
    .agg(
        (100 * sum(when(col("address") != "", 1).otherwise(0)) / count("*")).alias("percent_shipable")
    )
)

result.toPandas()