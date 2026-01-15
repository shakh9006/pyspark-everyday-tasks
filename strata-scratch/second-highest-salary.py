# Source: https://platform.stratascratch.com/coding/9892-second-highest-salary?code_type=6

# Title: Second Highest Salary

"""
Find the second highest salary of employees.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.col("salary").desc())

employee = (
    employee
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 2)
    .select("salary")
)

employee.toPandas()