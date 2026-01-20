# Source: https://platform.stratascratch.com/coding/9845-find-the-number-of-employees-working-in-the-admin-department?code_type=6

# Title: April Admin Employees

"""
Find the number of employees working in the Admin department that joined in April or later, in any year.
"""


import pyspark
import pyspark.sql.functions as F

worker = (
    worker
    .filter((F.month(F.col("joining_date")) >= 4) & (F.col("department") == 'Admin'))
    .agg(
        F.count("worker_id").alias("n_admins")
    )
)

worker.toPandas()