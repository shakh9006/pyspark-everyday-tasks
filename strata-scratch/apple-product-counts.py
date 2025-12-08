# Source: https://platform.stratascratch.com/coding/10141-apple-product-counts?code_type=6

# Title: Apple Product Counts

"""
We’re analyzing user data to understand how popular
Apple devices are among users who have performed at least one event on the platform.
Specifically, we want to measure this popularity across different languages.
Count the number of distinct users using Apple devices —limited to
"macbook pro", "iphone 5s", and "ipad air" — and compare it to the total number of users per language.

Present the results with the language, the number of Apple users, and the total number of users for each language. Finally, sort the results so that languages with the highest total user count appear first.
"""

import pyspark
import pyspark.sql.functions as F

devices = ["macbook pro", "ipad air", "iphone 5s"]
merged = playbook_events.join(playbook_users, on="user_id")

df = (
    merged
    .filter(F.col("device").isin(devices))
    .groupBy("language")
    .agg(
        F.countDistinct("user_id").alias("n_apple_users")
    )
)

result = (
    merged
    .groupBy("language")
    .agg(
        F.countDistinct("user_id").alias("n_total_users")
    )
    .join(df, on="language", how="left")
    .fillna(0)
    .select("language", "n_apple_users", "n_total_users")
    .orderBy(F.col("n_total_users").desc())
)

result.toPandas()