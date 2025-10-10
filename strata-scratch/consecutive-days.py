# Source: https://platform.stratascratch.com/coding/2054-consecutive-days?code_type=6

# Title: Consecutive Days

"""
Find all the users who were active for 3 consecutive days or more.

"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy(F.col("user_id")).orderBy(F.col("record_date"))

sf_events = (
    sf_events
    .withColumn("prev_date", F.lag("record_date").over(window_spec))
    .withColumn("next_date", F.lead("record_date").over(window_spec))
)

sf_events = (
    sf_events
    .where(
        (F.col("next_date") == F.date_add(F.col("record_date"), 1))
        & (F.col("prev_date") == F.date_sub(F.col("record_date"), 1))
    )
    .select("user_id")
    .distinct()
)

sf_events.toPandas()