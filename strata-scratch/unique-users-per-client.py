# Source: https://platform.stratascratch.com/coding/2024-unique-users-per-client-per-month?code_type=6

# Title: Unique Users Per Client Per Month

"""
Write a query that returns the number of unique users per client for each month.
Assume all events occur within the same year, so only month
needs to be be in the output as a number from 1 to 12.
"""

import pyspark
import pyspark.sql.functions as F

fact_events = fact_events.withColumn("month", F.month(F.col("time_id")))

fact_events = (
    fact_events
    .groupBy("client_id", "month")
    .agg(F.countDistinct(F.col("user_id")).alias("users_num"))
)

fact_events.toPandas()