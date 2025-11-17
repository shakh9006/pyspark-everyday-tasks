# Source: https://platform.stratascratch.com/coding/10296-facebook-accounts?code_type=6

# Title: Meta/Facebook Accounts

"""
Of all accounts with status records on January 10th, 2020, calculate the ratio of those with 'closed' status.
"""


import pyspark
import pyspark.sql.functions as F

fb_account_status = (
    fb_account_status
    .groupBy("status_date")
    .agg(
        F.count(F.col("acc_id")).alias("all_records"),
        F.sum(F.when(F.col("status") == "closed", 1).otherwise(0)).alias("closed_records")
    )
    .filter(F.col("status_date") == "2020-01-10")
    .select(F.round(F.col("closed_records") / F.col("all_records"), 2).alias("temp"))
)

fb_account_status.toPandas()
