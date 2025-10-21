# Source: https://platform.stratascratch.com/coding/10172-best-selling-item?code_type=6

# Title: Best Selling Item

"""
Find the best-selling item for each month (no need to separate months by year).
The best-selling item is determined by the highest total sales amount,
calculated as: total_paid = unitprice * quantity.
Output the month, description of the item, and the total amount paid.
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

online_retail = (
    online_retail
    .withColumn("month", F.month(F.col("invoicedate")))
    .withColumn("total_paid", F.col("unitprice") * F.col("quantity"))
)

online_retail = (
    online_retail
    .groupBy(F.col("month"), F.col("description"))
    .agg(
        F.sum(F.col("total_paid")).alias("total_paid")
    )
)

window_spec = Window.partitionBy(F.col("month")).orderBy(F.col("total_paid").desc())

online_retail = online_retail.withColumn(
    "rn",
    F.row_number().over(window_spec)
)

online_retail = (
    online_retail
    .select("month", "description", "total_paid")
    .filter(F.col("rn") == 1)
    .orderBy(F.col("month"))
)

online_retail.toPandas()