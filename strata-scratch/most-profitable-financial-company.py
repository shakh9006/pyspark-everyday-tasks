# Source: https://platform.stratascratch.com/coding/9663-find-the-most-profitable-company-in-the-financial-sector-of-the-entire-world-along-with-its-continent?code_type=6

# Title: Most Profitable Financial Company

"""
Find the most profitable company from the financial sector. Output the result along with the continent.
"""

import pyspark
import pyspark.sql.functions as F

forbes_global_2010_2014 = (
    forbes_global_2010_2014
    .orderBy(F.col("profits").desc())
    .select("company", "continent")
    .limit(1)
)

forbes_global_2010_2014.toPandas()
