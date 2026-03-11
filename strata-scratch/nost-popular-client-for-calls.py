# Source: https://platform.stratascratch.com/coding/2029-the-most-popular-client_id-among-users-using-video-and-voice-calls?code_type=6

# Title: Most Popular Client For Calls

"""
Select the most popular client_id based on the number of users who individually have at least 50% of
their events from the following list: 'video call received', 'video call sent', 'voice call received', 'voice call sent'.
"""


import pyspark
import pyspark.sql.functions as F

events = ['video call received', 'video call sent',  'voice call received', 'voice call sent']

fact_events = (
    fact_events
    .groupBy("client_id", "user_id")
    .agg(
        F.count("*").alias("all"),
        F.sum(
            F.when(F.col("event_type").isin(events), 1)
            .otherwise(0)
        ).alias("video_and_voice_ct")
    )
    .select("*", (
        F.coalesce((F.lit(100.0) * F.col("video_and_voice_ct")) / F.col("all"),
        F.lit(0)
    )).alias("temp"))
    .filter(F.col("temp") >= 50)
    .groupBy("client_id")
    .agg(
        F.count(F.col("temp")).alias("ct")
    )
    .orderBy(F.desc(F.col("ct")))
    .limit(1)
    .select(F.col("client_id").alias("CLIENT_ID"))
)

fact_events.toPandas()

