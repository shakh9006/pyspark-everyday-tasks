# Source: https://platform.stratascratch.com/coding/10003-lyft-driver-wages?code_type=6

# Title: Lyft Driver Wages

"""
For the video (or videos) that received the most user flags,
how many of these flags were reviewed by YouTube?
Output the video ID and the corresponding number of reviewed flags.
Ignore flags that do not have a corresponding flag_id.
"""

import pyspark
import pyspark.sql.functions as F

lyft_drivers = lyft_drivers.filter((F.col('yearly_salary') <= 30000) | (F.col('yearly_salary') >= 70000))

lyft_drivers.toPandas()