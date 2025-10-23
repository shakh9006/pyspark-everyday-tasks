# Source: https://platform.stratascratch.com/coding/2136-customer-tracking?code_type=6

# Title: Customer Tracking

"""
Given the users' sessions logs on a particular day,
calculate how many hours each user was active that day.

Note: The session starts when state=1 and ends when state=0.
"""

import pyspark
import pyspark.sql.functions as F

cust_tracking = cust_tracking.withColumn(
    "sort_ts",
    F.when(F.col("state") == 1, F.col("timestamp").cast("long"))
    .otherwise(-F.col("timestamp").cast("long"))
)

cust_tracking = (
    cust_tracking
    .groupBy(F.col("cust_id"))
    .agg(
        F.abs(F.sum(F.col("sort_ts")) / 3600).alias("sum(time_diff)")
    )
    .orderBy(F.col("cust_id"))
)

cust_tracking.toPandas()