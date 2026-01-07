# Source: https://platform.stratascratch.com/coding/9991-top-ranked-songs?code_type=6

# Title: Top Ranked Songs

"""
Find songs that have ranked in the top position.
Output the track name and the number of times it ranked at the top.
Sort your records by the number of times the song was in the top position in descending order.
"""


import pyspark
import pyspark.sql.functions as F

spotify_worldwide_daily_song_ranking = (
    spotify_worldwide_daily_song_ranking
    .filter(F.col("position") == 1)
    .groupBy("trackname")
    .agg(
        F.count("id").alias("times_top1")
    )
)

spotify_worldwide_daily_song_ranking.toPandas()