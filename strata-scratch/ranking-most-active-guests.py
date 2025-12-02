# Source: https://platform.stratascratch.com/coding/10299-finding-updated-records?code_type=6

# Title: Finding Updated Records

"""
Identify the most engaged guests by ranking them according to their overall messaging activity.
The most active guest, meaning the one who has exchanged the most messages with hosts, should have the highest rank.
If two or more guests have the same number of messages,
they should have the same rank. Importantly, the ranking shouldn't skip any numbers,
even if many guests share the same rank. Present your results in a clear format,
showing the rank, guest identifier, and total number of messages for each guest,
ordered from the most to least active.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window

window_spec = Window.orderBy(F.col("sum_n_messages").desc())

airbnb_contacts = (
    airbnb_contacts
    .groupBy(F.col("id_guest"))
    .agg(
        F.sum(F.col("n_messages")).alias("sum_n_messages")
    )
)

airbnb_contacts = airbnb_contacts.withColumn(
    "ranking",
    F.dense_rank().over(window_spec)
)

airbnb_contacts.toPandas()
