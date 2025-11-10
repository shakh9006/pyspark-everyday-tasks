# Source: https://platform.stratascratch.com/coding/10285-acceptance-rate-by-date?code_type=6

# Title: Acceptance Rate By Date

"""
Calculate the friend acceptance rate for each date when friend requests were sent.
A request is sent if action = sent and accepted if action = accepted.
If a request is not accepted, there is no record of it being accepted in the table.


The output will only include dates where requests were sent and at least one of them was accepted
(acceptance can occur on any date after the request is sent).
"""

import pyspark
import pyspark.sql.functions as F

result = (
    fb_friend_requests
    .groupBy(F.col("user_id_sender"), F.col("user_id_receiver"))
    .agg(
        F.min("date").alias("request_date"),
        F.lit(1).alias("sent_count"),
    )
)

result = (
    result.join(
        fb_friend_requests,
        (result.user_id_sender == fb_friend_requests.user_id_sender)
            & (result.user_id_receiver == fb_friend_requests.user_id_receiver)
            & (result.request_date <= fb_friend_requests.date)
            & (fb_friend_requests.action == "accepted"),
        "left"
    )
    .withColumn("accepted_count", F.when(F.col("action") == "accepted", 1).otherwise(0))
    .select("request_date", "sent_count", "accepted_count")
)

result = (
    result
    .groupBy(F.col("request_date"))
    .agg(
        F.round(F.sum(F.col("accepted_count")) / F.sum(F.col("sent_count")), 2).alias("percentage_acceptance")
    )
)

result.toPandas()