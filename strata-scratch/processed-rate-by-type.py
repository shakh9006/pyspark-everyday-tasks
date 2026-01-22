# Source: https://platform.stratascratch.com/coding/9781-find-the-rate-of-processed-tickets-for-each-type?code_type=6

# Title: Processed Ticket Rate By Type

"""
Find the processed rate of tickets for each `type`.
The processed rate is defined as the number of processed tickets
divided by the total number of tickets for that type. Round this result to two decimal places.
"""


import pyspark
import pyspark.sql.functions as F

facebook_complaints = (
    facebook_complaints
    .groupBy("type")
    .agg(
        (F.round(
            F.sum(F.when(F.col("processed") == "TRUE", 1).otherwise(0)) / F.count("processed"),
            2
        )).alias("processed_rate")
    )
)

facebook_complaints.toPandas()