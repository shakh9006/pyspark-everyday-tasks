# Source: https://platform.stratascratch.com/coding/10142-no-order-customers?code_type=6

# Title: No Order Customers

"""
Identify customers who did not place an order between 2019-02-01 and 2019-03-01.

Include:
    • Customers who placed orders only outside this date range.
    • Customers who never placed any orders.

Output the customers' first names.
"""


import pyspark
import pyspark.sql.functions as F

start = '2019-02-01'
end = '2019-03-01'

customer_not_in_period = (
    customers.alias("c")
    .join(
        orders
        .filter(F.col("order_date").between(start, end))
        .select("cust_id")
        .dropDuplicates()
        .alias("o"),
        on=F.col("c.id") == F.col("o.cust_id"),
        how="left_anti"
    )
    .select("first_name")
)

customer_not_in_period.toPandas()