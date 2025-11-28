# Source: https://platform.stratascratch.com/coding/2131-user-streaks?code_type=6

# Title: User Streaks

"""
Provided a table with user id and the dates they visited the platform,
find the top 3 users with the longest continuous streak of visiting
the platform as of August 10, 2022. Output the user ID and the length of the streak.


In case of a tie, display all users with the top three longest streaks.
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window

w = Window.partitionBy(F.col("user_id")).orderBy("visit_date")

user_streaks = (
    user_streaks
    .filter(F.col("date_visited") <= '2022-08-10')
    .distinct()
    .withColumn("visit_date", F.to_date("date_visited"))
    .withColumn("rn", F.rank().over(w))
    .withColumn("group_date", F.date_sub(F.col("visit_date"), F.col("rn")))
    .groupBy(F.col("user_id"), F.col("group_date"))
    .agg(
        F.count(F.col("visit_date")).alias("dq")
    )
)

user_streaks = (
    user_streaks
    .groupBy(F.col("user_id"))
    .agg(
        F.max(F.col("dq")).alias("dq")
    )
    .filter(F.col("dq") > 2)
    .orderBy(F.col("dq").desc(), F.col("dq"))
)

user_streaks.toPandas()