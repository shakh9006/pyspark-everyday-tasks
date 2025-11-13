# Source: https://platform.stratascratch.com/coding/10351-activity-rank?code_type=6

# Title: Activity Rank

"""
Find the email activity rank for each user.
Email activity rank is defined by the total number of emails sent.
The user with the highest number of emails sent will have a rank of 1,
and so on. Output the user, total emails, and their activity rank.

• Order records first by the total emails in descending order.
• Then, sort users with the same number of emails in alphabetical order by their username.
• In your rankings, return a unique value (i.e., a unique rank) even if multiple users have the same number of emails.
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.col("total_emails").desc(), F.col("user_id"))

google_gmail_emails = (
    google_gmail_emails
    .groupBy(F.col("from_user").alias("user_id"))
    .agg(
        F.count(F.col("to_user")).alias("total_emails")
    )
)

google_gmail_emails = google_gmail_emails.withColumn(
    "activity_rank",
    F.rank().over(window_spec)
)

google_gmail_emails.toPandas()