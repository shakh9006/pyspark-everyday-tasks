# Source: https://platform.stratascratch.com/coding/10078-find-matching-hosts-and-guests-in-a-way-that-they-are-both-of-the-same-gender-and-nationality?code_type=6

# Title: Matching Similar Hosts and Guests

"""
Find matching hosts and guests pairs in a way that they are both of the same gender and nationality.
Output the host id and the guest id of matched pair.
"""


import pyspark
import pyspark.sql.functions as F

result = (
    airbnb_hosts
    .alias("h")
    .join(
        airbnb_guests
        .alias("g"),
        on=(F.col("h.nationality") == F.col("g.nationality")) & (F.col("h.gender") == F.col("g.gender")),
        how="inner"
    )
    .select(F.col("h.host_id"), F.col("g.guest_id"))
    .distinct()
)

result.toPandas()
