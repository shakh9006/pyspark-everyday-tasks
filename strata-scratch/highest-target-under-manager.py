# Source: https://platform.stratascratch.com/coding/9905-highest-target-under-manager?code_type=6

# Title: Highest Target Under Manager

"""
Identify the employee(s) working under manager manager_id=13 who have achieved the highest target.
Return each such employee’s first name alongside the target value.
The goal is to display the maximum target among all employees under manager_id=13
and show which employee(s) reached that top value.
"""



import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.col("target").desc(), F.col("id").asc())

salesforce_employees = salesforce_employees.filter(F.col("manager_id") == 13)

salesforce_employees = (
    salesforce_employees
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") < 4)
    .select("first_name", "target")
)

salesforce_employees.toPandas()
