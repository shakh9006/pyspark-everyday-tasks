# Source: https://platform.stratascratch.com/coding/9942-largest-olympics?code_type=6

# Title: Largest Olympics

"""
Find the Olympics with the highest number of unique athletes.
The Olympics game is a combination of the year and the season, and is found in the games column.
Output the Olympics along with the corresponding number of athletes.
The id column uniquely identifies an athlete.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.col("athletes_count").desc())

olympics_athletes_events = (
    olympics_athletes_events
    .groupBy("games")
    .agg(
        F.countDistinct("name").alias("athletes_count")
    )
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .select("games", "athletes_count")
)

olympics_athletes_events.toPandas()
