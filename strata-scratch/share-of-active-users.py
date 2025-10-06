# Source: https://platform.stratascratch.com/coding/2005-share-of-active-users?code_type=6

# Title: Share of Active Users

"""
Calculate the percentage of users who are both from the US and have an 'open' status,
as indicated in the fb_active_users table.

"""

import pyspark
import pyspark.sql.functions as F

agg_df = fb_active_users.agg(
    F.count(F.when((F.col('status') == 'open') & (F.col('country') == 'USA'), 1)).alias("us_open"),
    F.count("*").alias("total")
)

result = agg_df\
    .withColumn("us_active_share", F.round(F.col("us_open") / F.col("total") * 100, 2))\
    .select("us_active_share")

result.toPandas()