# Source: https://platform.stratascratch.com/coding/2097-premium-acounts?code_type=6

# Title: Premium Accounts

"""
You have a dataset that records daily active users for each premium account.
A premium account appears in the data every day as long as it remains premium. However,
some premium accounts may be temporarily discounted, meaning they are not actively paying — this is indicated by a final_price of 0.


For each date, count the number of premium accounts that were actively paying on that day.
Then, track how many of those same accounts are still premium and actively paying exactly 7 days later,
if that later date exists in the dataset. Return results for the first 7 dates in the dataset.


Output three columns:
•   The date of initial calculation.
•   The number of premium accounts that were actively paying on that day.
•   The number of those accounts that remain premium and are still paying after 7 days.
"""

import pyspark
import pyspark.sql.functions as F

filtered_acc = premium_accounts_by_day.filter(F.col("final_price") != 0)
filtered_acc = filtered_acc.withColumn("check_date", F.date_add("entry_date", 7))

source = (
    filtered_acc
    .alias("f1")
    .join(
        filtered_acc.alias("f2"),
        on=((F.col("f1.account_id") == F.col("f2.account_id")) & (F.col("f1.check_date") == F.col("f2.entry_date"))),
        how="left"
    )

)

result = (
    source
    .groupBy("f1.entry_date")
    .agg(
        F.countDistinct(F.col("f1.account_id")).alias("premium_paid_accounts"),
        F.countDistinct(F.col("f2.account_id")).alias("premium_paid_accounts_after_7d")
    )
    .orderBy("f1.entry_date")
    .limit(7)
)

result.toPandas()
