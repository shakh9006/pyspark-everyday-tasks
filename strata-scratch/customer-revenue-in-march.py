# Source: https://platform.stratascratch.com/coding/9782-customer-revenue-in-march?code_type=6

# Title: Customer Revenue In March

"""
Calculate the total revenue from each customer in March 2019. Include only customers who were active in March 2019.
An active user is a customer who made at least one transaction in March 2019.

Output the revenue along with the customer id and sort the results based on the revenue in descending order.
"""


import pyspark
import pyspark.sql.functions as F

orders = (
    orders
    .filter((F.month(F.col("order_date")) == 3) & (F.year(F.col("order_date")) == 2019))
    .groupBy("cust_id")
    .agg(
        F.sum("total_order_cost").alias("revenue")
    )
    .orderBy(F.col("revenue").desc())
)

orders.toPandas()