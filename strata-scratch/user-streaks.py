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

user_streaks = (
    user_streaks
    .filter(F.col("date_visited") <= '2022-08-10')
    .distinct()
    .orderBy(F.col("user_id"), F.col("date_visited"))
    .withColumn("visit_date", F.to_date("date_visited"))
)

w = Window.partitionBy(F.col("user_id")).orderBy(F.col("visit_date"))

user_streaks = (
    user_streaks
    .withColumn("prev_date", F.lag(F.col("visit_date")).over(w))
    .withColumn(
        "is_new_streak",
        F.when(
            (F.col("prev_date").isNull()) |
            (F.datediff("visit_date", "prev_date") != 1),
            1
        ).otherwise(0)
    )
    .withColumn("streak_id", F.sum("is_new_streak").over(w))
    # .withColumn("rn", F.dense_rank().over(w2))
)

streaks = (
    user_streaks.groupBy("user_id", "streak_id")
        .agg(F.count("*").alias("streak_length"))
)


streaks.toPandas()