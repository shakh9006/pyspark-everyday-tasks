# Source: https://platform.stratascratch.com/coding/10319-monthly-percentage-difference?code_type=6

# Title: Monthly Percentage Difference

"""
Given a table of purchases by date, calculate the month-over-month percentage change in revenue.
The output should include the year-month date (YYYY-MM) and percentage change,
rounded to the 2nd decimal point, and sorted from the beginning of the year to the end of the year.

The percentage change column will be populated from the 2nd month forward and can be
calculated as ((this month's revenue - last month's revenue) / last month's revenue)*100.
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sf_transactions = sf_transactions.withColumn(
    "year_month",
    F.date_format(F.col("created_at"), "yyyy-MM")
)

sf_transactions = (
    sf_transactions
    .groupBy(F.col("year_month"))
    .agg(
        F.sum(F.col("value")).alias("total_amount")
    )
)

w_spec = Window.orderBy("year_month")

sf_transactions = sf_transactions.withColumn(
    "prev_month",
    F.lag("total_amount").over(w_spec)
)

sf_transactions = (
    sf_transactions
    .withColumn(
        "revenue_diff_pct",
        F.round(F.when(F.col("prev_month").isNull(), "").otherwise((F.col("total_amount") - F.col("prev_month")) / F.col("prev_month") * 100), 2)
    )
    .select("year_month", "revenue_diff_pct")
)


sf_transactions.toPandas()
