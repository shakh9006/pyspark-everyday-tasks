# Source: https://platform.stratascratch.com/coding/9653-count-the-number-of-user-events-performed-by-macbookpro-users?code_type=6

# Title: MacBookPro User Event Count

"""
Count the number of user events performed by MacBookPro users.
Output the result along with the event name.
Sort the result based on the event count in the descending order.
"""

import pyspark
import pyspark.sql.functions as F

playbook_events = (
    playbook_events
    .filter(F.col("device") == "macbook pro")
    .groupBy("event_name")
    .agg(
        F.count("user_id").alias("users")
    )
    .orderBy(F.col("users").desc())
)

playbook_events.toPandas()
