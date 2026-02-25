# Source: https://platform.stratascratch.com/coding/2007-rank-variance-per-country?code_type=6

# Title: Rank Variance Per Country

"""
Compare the total number of comments made by users in each country during December 2019 and January 2020.


For each month, rank countries by their total number of comments in descending order.
Countries with the same total should share the same rank, and the next rank should increase by one (without skipping numbers).


Return the names of the countries whose rank improved from December to January (that is, their rank number became smaller).
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

comments = fb_comments_count.filter(
    (F.col("created_at") >= '2019-12-01') &
    (F.col("created_at") < '2020-02-01')
)

result = (
    comments
    .alias("c")
    .join(
        fb_active_users
        .alias("u"),
        on=F.col("u.user_id") == F.col("c.user_id"),
        how="left"
    )
    .filter(F.col("country") != "")
    .select("u.country", "c.created_at", "c.number_of_comments")
    .withColumn("month", F.date_format(F.col("created_at"), "MM"))
    .groupBy("country", "month")
    .agg(
        F.sum("number_of_comments").alias("monthly_comments")
    )
)

window_spec = Window.partitionBy("month").orderBy(F.desc("monthly_comments"))

result = result.withColumn("rn", F.dense_rank().over(window_spec))

december_comments = result.filter(F.col("month") == "12")
january_comments = result.filter(F.col("month") == "01")

result = (
    december_comments
    .alias("d")
    .join(
        january_comments
        .alias("j"),
        on=(F.col("d.country") == F.col("j.country")) & (F.col("d.rn") > F.col("j.rn")),
        how="inner"
    )
    .select("d.country")
)


result.toPandas()

