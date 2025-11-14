# Source: https://platform.stratascratch.com/coding/10553-finding-purchases?code_type=6

# Title: Finding Purchases

"""
Identify returning active users by finding users who made a second purchase
within 7 days or less of any previous transaction,
excluding same-day purchases. Output a list of these user_id.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy("user_id").orderBy(F.col("created_at").asc())

amazon_transactions = (
    amazon_transactions
    .withColumn("next_purchase", F.lead('created_at', 1).over(window_spec))
    .filter((F.datediff("next_purchase", "created_at") < 7) & (F.datediff("next_purchase", "created_at") > 0))
    .select("user_id")
    .distinct()
)

amazon_transactions.toPandas()