# Source: https://platform.stratascratch.com/coding/9610-find-students-with-a-median-writing-score?code_type=6

# Title: Find Students At Median Writing

"""
Identify the IDs of students who scored exactly at the median for the SAT writing section.

"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

w = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

sat_scores = (
    sat_scores.withColumn(
        "median",
        F.expr("percentile_approx(sat_writing, 0.5)").over(w)
    )
    .filter(F.col("sat_writing") == F.col("median"))
    .select("student_id")
    .distinct()
)

sat_scores.toPandas()