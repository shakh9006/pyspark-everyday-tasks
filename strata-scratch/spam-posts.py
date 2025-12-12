# Source: https://platform.stratascratch.com/coding/10134-spam-posts?code_type=6

# Title: Spam Posts

"""
Calculate the percentage of spam posts in all viewed posts by day.
 A post is considered a spam if a string "spam" is inside keywords of the post.
 Note that the facebook_posts table stores all posts posted by users.
 The facebook_post_views table is an action table denoting if a user has viewed a post.
"""

import pyspark
import pyspark.sql.functions as F

result = (
    facebook_post_views
    .alias("v")
    .join(
        facebook_posts
        .alias("p"),
        on="post_id",
        how="inner"
    )
    .select(F.col("v.post_id"), F.col("v.viewer_id"), F.col("p.post_date"), F.col("p.post_keywords"))
)

result = (
    result
    .groupBy("post_date")
    .agg(
        F.sum(
            F.when(F.col("post_keywords").like("%spam%"), 1).otherwise(0)
        ).alias("spam_count"),
        F.count("post_id").alias("total_count")
    )
    .select(F.col("post_date"), (F.col("spam_count") / F.col("total_count") * 100).alias("spam_percentage"))
)

result.toPandas()