# Source: https://platform.stratascratch.com/coding/9891-customer-details?code_type=6

# Title: Customer Details

"""
Find the details of each customer regardless of whether the customer made an order.
 Output the customer's first name, last name, and the city along with the order details.
Sort records based on the customer's first name and the order details in ascending order.
"""

import pyspark
import pyspark.sql.functions as F

# Start writing code
customers = customers.select(F.col("id").alias("cust_id"), "first_name", "last_name", "city")

result = customers.join(orders, on="cust_id", how="left")

result = (
    result
    .select("first_name", "last_name", "city", "order_details")
    .orderBy("first_name", "order_details")
)

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()