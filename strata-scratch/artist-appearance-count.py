# Source: https://platform.stratascratch.com/coding/9992-find-artists-that-have-been-on-spotify-the-most-number-of-times?code_type=6

# Title: Artist Appearance Count

"""
Find how many times each artist appeared on the Spotify ranking list.
Output the artist name along with the corresponding number of occurrences.
Order records by the number of occurrences in descending order.
"""


import pyspark
import pyspark.sql.functions as F

spotify_worldwide_daily_song_ranking = (
    spotify_worldwide_daily_song_ranking
    .groupBy("artist")
    .agg(
        F.count("streams").alias("n_occurences")
    )
    .orderBy(F.col("n_occurences").desc())
)

spotify_worldwide_daily_song_ranking.toPandas()