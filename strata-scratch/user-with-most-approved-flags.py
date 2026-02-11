# Source: https://platform.stratascratch.com/coding/2104-user-with-most-approved-flags?code_type=6

# Title: User with Most Approved Flags

"""
Which user flagged the most distinct videos that ended up approved by YouTube?
Output, in one column, their full name or names in case of a tie.
In the user's full name, include a space between the first and the last name.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.col("ct").desc())

user_flags = (
   user_flags
   .groupBy("user_firstname", "user_lastname")
   .agg(F.countDistinct("video_id").alias("ct"))
   .withColumn("rn", F.rank().over(window_spec))
   .filter(F.col("rn") == 1)
   .select(F.concat(F.col("user_firstname"), F.lit(" "), F.col("user_lastname")).alias("user"))
)

user_flags.toPandas()