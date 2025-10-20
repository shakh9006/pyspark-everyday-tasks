# Source: https://platform.stratascratch.com/coding/10352-users-by-avg-session-time?code_type=6

# Users By Average Session Time

"""
Calculate each user's average session time, where a session is defined as the time
difference between a page_load and a page_exit. Assume each user has only one session per day.
If there are multiple page_load or page_exit events on the same day,
use only the latest page_load and the earliest page_exit.
Only consider sessions where the page_load occurs before the page_exit on the same day.
 Output the user_id and their average session time.

"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

facebook_web_log = facebook_web_log.withColumn(
    "sort_ts",
    F.when(F.col("action") == "page_load", -F.col("timestamp").cast("long"))
    .otherwise(F.col("timestamp").cast("long"))
)

wp = Window.partitionBy("user_id", "timestamp", "action").orderBy("sort_ts")

facebook_web_log = facebook_web_log.withColumn("timestamp", F.col("timestamp").cast("date"))

facebook_web_log = facebook_web_log.withColumn(
    "rn",
    F.row_number().over(wp)
)

facebook_web_log = (
    facebook_web_log
    .filter((F.col("rn") == 1) & ((F.col("action") == 'page_exit') | (F.col("action") == 'page_load')))
)

facebook_web_log = (
    facebook_web_log
    .groupBy("user_id", "action")
    .agg(
        F.sum(F.col("sort_ts")).alias("timestamp"),
        F.count(F.col("action")).alias("ct")
    )
    .filter(F.col("ct") > 1)
)

facebook_web_log = (
    facebook_web_log
    .groupBy("user_id")
    .agg(
        F.avg(F.col("timestamp")).alias("avg_session_duration")
    )
)

facebook_web_log.toPandas()