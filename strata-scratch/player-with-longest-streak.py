# Source: https://platform.stratascratch.com/coding/2059-player-with-longest-streak?code_type=6

# Title: Player with Longest Streak

"""
You are given a table of tennis players and their matches that they could either win (W) or lose (L).
Find the longest streak of wins. A streak is a set of consecutive won matches of one player.
The streak ends once a player loses their next match. Output the ID of the player or players and the length of the streak.
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

w1 = (
    Window
    .partitionBy("player_id")
    .orderBy("match_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

players_results = players_results.withColumn(
    "loss_flag",
    F.when(F.col("match_result") == "L", 1).otherwise(0)
)

players_results = players_results.withColumn(
    "streak_group",
    F.sum(F.col("loss_flag")).over(w1)
)

w2 = (
    Window
    .partitionBy("player_id", "streak_group")
    .orderBy("match_date")
)

players_results = (
    players_results
    .groupBy(F.col("player_id"), F.col("streak_group"))
    .agg(
        F.count(
            F.when(F.col("match_result") == "W", 1)
        ).alias("win_streak")
    )
)

players_results = (
    players_results
    .groupBy(F.col("player_id"))
    .agg(F.max(F.col("win_streak")).alias("max_win_streak"))
)

w2 = Window.orderBy(F.desc("max_win_streak"))

players_results = players_results.withColumn(
    "rn",
    F.dense_rank().over(w2)
)

players_results = (
    players_results
    .select("player_id", "max_win_streak")
    .where("rn == 1")
    .orderBy("player_id")
)

players_results.toPandas()