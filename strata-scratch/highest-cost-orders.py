# Source: https://platform.stratascratch.com/coding/9915-highest-cost-orders?code_type=6

# Title: Highest Cost Orders

"""
Find the customers with the highest daily total order cost between 2019-02-01 and 2019-05-01.
If a customer had more than one order on a certain day, sum the order costs on a daily basis.
Output each customer's first name, total cost of their items, and the date.
If multiple customers tie for the highest daily total on the same date, return all of them.


For simplicity, you can assume that every first name in the dataset is unique.
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

orders_df = orders.filter(
    (F.col("order_date") >= '2019-02-01') &
    (F.col("order_date") <= '2019-05-01')
)

daily_totals = (
    orders_df
    .groupBy("cust_id", "order_date")
    .agg(
        F.sum("total_order_cost").alias("total_daily_cost")
    )
)

window_spec = Window.partitionBy("order_date").orderBy(F.desc("total_daily_cost"))

ranked_orders = daily_totals.withColumn("rn", F.rank().over(window_spec))

top_daily = ranked_orders.filter(F.col("rn") == 1)

result = (
    top_daily
    .alias("d")
    .join(
        customers.alias("c"),
        on=F.col("d.cust_id") == F.col("c.id"),
        how="left"
    )
    .select("c.first_name", "d.order_date", "d.total_daily_cost")
)

result.toPandas()

