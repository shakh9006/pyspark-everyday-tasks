# Source: https://platform.stratascratch.com/coding/9650-find-the-top-10-ranked-songs-in-2010?code_type=6

# Title: Top 10 Songs 2010

"""
Find the top 10 ranked songs in 2010. Output the rank, group name, and song name,
but do not show the same song twice. Sort the result based on the rank in ascending order.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec_rn = Window.partitionBy("group_name", "song_name").orderBy("id")
window_spec_id = Window.orderBy(F.col("year_rank").asc())

billboard_top_100_year_end = (
    billboard_top_100_year_end
    .filter(F.col("year") == 2010)
    .withColumn("rn", F.row_number().over(window_spec_rn))
    .filter(F.col("rn") == 1)
    .withColumn("id", F.row_number().over(window_spec_id))
    .select("id", "group_name", "song_name")
    .filter(F.col("id") < 11)
)

billboard_top_100_year_end.toPandas()