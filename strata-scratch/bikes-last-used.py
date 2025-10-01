# Source: https://platform.stratascratch.com/coding/10176-bikes-last-used?code_type=6

# Title: Bikes Last Used

"""
Find the last time each bike was in use.
Output both the bike number and the date-timestamp of the bike's last use (i.e., the date-time the bike was returned).
Order the results by bikes that were most recently used.
"""


import pyspark
import pyspark.sql.functions as F

result = (
    dc_bikeshare_q1_2012\
        .groupBy('bike_number')\
        .agg(F.max('end_time').alias('last_time'))
)

result.toPandas()