# Source: https://platform.stratascratch.com/coding/10322-finding-user-purchases?code_type=6

# Title: Finding User Purchases

"""
Identify returning active users by finding users who made a second purchase within 1 to 7 days after their first purchase.
Ignore same-day purchases. Output a list of these user_ids.
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col('user_id')).orderBy(F.col('created_at').asc())

amazon_transactions = (
    amazon_transactions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") < 3)
)

amazon_transactions = (
    amazon_transactions
    .groupBy(F.col("user_id"))
    .agg(
        F.count(F.col("rn")).alias("cnt"),
        (
            (F.max(F.unix_timestamp("created_at")) - F.min(F.unix_timestamp("created_at"))) / 60 / 60 / 24
        ).alias("diff")
    )
    .filter(F.col("cnt") == 2)
    .filter((F.col("diff") < 8) & (F.col("diff") > 0))
    .select(F.col("user_id"))
)

amazon_transactions.toPandas()