# Source: https://platform.stratascratch.com/coding/9913-order-details?code_type=6

# Title: Order Details

"""
Find order details made by Jill and Eva.
Consider the Jill and Eva as first names of customers.
Output the order date, details and cost along with the first name.
Order records based on the customer id in ascending order.
"""


import pyspark
import pyspark.sql.functions as F

customers = customers.filter(F.col("first_name").isin(["Eva", "Jill"]))

orders = (
    orders
    .join(
        customers,
        on=orders.cust_id == customers.id,
        how="inner"
    )
)

orders = (
    orders
    .select("first_name", "order_date", "order_details", "total_order_cost")
    .orderBy(F.col("cust_id"))
)

orders.toPandas()
