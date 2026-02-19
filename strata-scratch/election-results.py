# Source: https://platform.stratascratch.com/coding/2099-election-results?code_type=6

# Title: Finding Updated Records

"""
The election is conducted in a city and everyone can vote for one or more candidates,
or choose not to vote at all. Each person has 1 vote so if they vote for multiple candidates,
their vote gets equally split across these candidates. For example, if a person votes for 2 candidates,
these candidates receive an equivalent of 0.5 vote each. Some voters have chosen not to vote, which explains the blank entries in the dataset.

Find out who got the most votes and won the election. Output the name of the candidate or multiple names in case of a tie.
To avoid issues with a floating-point error you can round the number of votes received by a candidate to 3 decimal places.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

point_spec = Window.partitionBy("voter")
rank_spec = Window.orderBy(F.col("total_votes").desc())

voting_results = (
    voting_results
    .distinct()
    .withColumn(
        "vote_point",
        F.format_number(1 / F.count("candidate").over(point_spec), 3)
    )
    .groupBy("candidate")
    .agg(
        F.format_number(F.sum(F.col("vote_point")), 3).alias("total_votes")
    )
    .withColumn("rn", F.rank().over(rank_spec))
    .filter(F.col("rn") == 1)
    .select("candidate")
)

voting_results.toPandas()
