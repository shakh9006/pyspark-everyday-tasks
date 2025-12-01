# Source: https://platform.stratascratch.com/coding/10303-top-percentile-fraud?code_type=6

# Title: Top Percentile Fraud

"""
We want to identify the most suspicious claims in each state.
We'll consider the top 5% of claims
(those at or above the 95th percentile of fraud scores) in each state as potentially fraudulent.


Your output should include the policy number, state, claim cost, and fraud score.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window

window_spec = Window.partitionBy(F.col("state")).orderBy(F.col("fraud_score"))

fraud_score = fraud_score.withColumn(
    "rn",
    F.percent_rank().over(window_spec)
)

fraud_score = (
    fraud_score
    .filter(F.col("rn") >= 0.95)
    .select("policy_num", "state", "claim_cost", "fraud_score")
)

fraud_score.toPandas()