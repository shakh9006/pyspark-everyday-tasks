# Source: https://platform.stratascratch.com/coding/10547-actor-rating-difference-analysis?code_type=6

# Title: Actor Rating Difference Analysis

"""
Management wants to analyze only employees with official job titles.
Find the job titles of the employees with the highest salary.
If multiple employees have the same highest salary, include all their job titles.

"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

w1 = Window.partitionBy(F.col("actor_name")).orderBy(F.desc(F.col("release_date")))

actor_rating_shift = actor_rating_shift.withColumn(
    "rn",
    F.rank().over(w1)
)

actors = (
    actor_rating_shift
    .where(F.col("rn") == 1)
    .select(F.col("actor_name"), F.col("film_rating").alias("latest_rating"))
)

actor_rating_shift = actor_rating_shift.filter(F.col("rn") != 1)

actor_rating_shift = (
    actor_rating_shift
    .where(F.col("rn") != 1)
    .groupBy(F.col("actor_name"))
    .agg(
        F.round(F.avg(F.col("film_rating")), 2).alias("avg_rating")
    )
)

result = actor_rating_shift.join(actors, on="actor_name", how="left_outer")

result = (
    result
    .select(F.col("actor_name"), F.col("latest_rating"), F.col("avg_rating"))
    .withColumn(
        "rating_difference",
        F.when(
            F.round(F.col("avg_rating"), 2) != F.round(F.col("latest_rating"), 2),
            F.round(F.col("latest_rating") - F.col("avg_rating"), 2)
        ).otherwise(0)
    )
)

# actor_name
# latest_rating
# avg_rating
# rating_difference

result.toPandas()