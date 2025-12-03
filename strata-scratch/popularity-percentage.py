# Source: https://platform.stratascratch.com/coding/10284-popularity-percentage?code_type=6

# Title: Popularity Percentage

"""
Find the popularity percentage for each user on Meta/Facebook.
The dataset contains two columns, user1 and user2, which represent pairs of friends.
Each row indicates a mutual friendship between user1 and user2, meaning both users are friends with each other.
A user's popularity percentage is calculated as the total number of friends
they have (counting connections from both user1 and user2 columns) divided by the total
 number of unique users on the platform.
 Multiply this value by 100 to express it as a percentage.

Output each user along with their calculated popularity percentage.
he results should be ordered by user ID in ascending order.
"""


import pyspark
import pyspark.sql.functions as F

left_user = (
    facebook_friends
    .groupBy(F.col("user1").alias("user"))
    .agg(
        F.count("user2").alias("count")
    )
)

right_user = (
    facebook_friends
    .groupBy(F.col("user2").alias("user"))
    .agg(
        F.count("user1").alias("count")
    )
)

result = left_user.union(right_user)

result = (
    result
    .groupBy("user")
    .agg(
        F.sum(F.col("count")).alias("count")
    )
    .orderBy(F.col("count").desc())
)

count = result.count() * 1.0

result = (
    result
    .withColumn(
        "temp",
        F.col("count") / count * 100
    )
    .select("user", "temp")
)

result.toPandas()