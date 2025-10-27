# Source: https://platform.stratascratch.com/coding/2103-reviewed-flags-of-top-videos?code_type=6

# Title: Reviewed flags of top videos

"""
For the video (or videos) that received the most user flags,
how many of these flags were reviewed by YouTube?
Output the video ID and the corresponding number of reviewed flags.
Ignore flags that do not have a corresponding flag_id.
"""

import pyspark
import pyspark.sql.functions as F

user_flags = user_flags.filter(F.col("flag_id").isNotNull())

video_flag_counts = (
    user_flags
    .groupBy("video_id")
    .agg(F.count("flag_id").alias("total_flags"))
)

max_flags = video_flag_counts.agg(F.max("total_flags").alias("max_flags")).collect()[0]["max_flags"]

most_flagged_videos = (
    video_flag_counts
    .filter(F.col("total_flags") == max_flags)
    .select("video_id")
)

result = (
    user_flags
    .join(most_flagged_videos, on="video_id", how="inner")
    .join(flag_review.filter(F.col("reviewed_by_yt") == True), on="flag_id", how="inner")
    .groupBy("video_id")
    .agg(F.count("flag_id").alias("reviewed_flags"))
)

result.toPandas()