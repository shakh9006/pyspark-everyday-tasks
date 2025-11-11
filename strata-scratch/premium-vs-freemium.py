# Source: https://platform.stratascratch.com/coding/10300-premium-vs-freemium?code_type=6

# Title: Premium vs Freemium

"""
Find the total number of downloads for paying and non-paying users by date.
Include only records where non-paying customers have more downloads than paying customers.
The output should be sorted by earliest date first and contain 3 columns date, non-paying downloads, paying downloads.
"""

import pyspark
import pyspark.sql.functions as F

paying_acc_ids = (
    ms_acc_dimension
    .filter(F.col("paying_customer") == "yes")
    .select(F.col("acc_id").alias("paying_acc_id"))
)

users_with_flag = (
    ms_user_dimension
    .join(paying_acc_ids, ms_user_dimension.acc_id == paying_acc_ids.paying_acc_id, how="left")
    .withColumn(
        "is_paying",
        F.when(F.col("paying_acc_id").isNotNull(), F.lit("yes")).otherwise(F.lit("no"))
    )
    .select("user_id", "is_paying")
)

result = (
    ms_download_facts
    .join(users_with_flag, how="left", on="user_id")
    .groupBy(F.col("date").alias("download_date"))
    .agg(
        F.sum(F.when(F.col("is_paying") == "no", F.col("downloads")).otherwise(0)).alias("none_paying"),
        F.sum(F.when(F.col("is_paying") == "yes", F.col("downloads")).otherwise(0)).alias("paying"),
    )
    .filter(F.col("none_paying") > F.col("paying"))
    .orderBy(F.col("download_date"))
)

result.toPandas()