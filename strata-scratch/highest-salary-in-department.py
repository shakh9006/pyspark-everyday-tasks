# Source: https://platform.stratascratch.com/coding/9897-highest-salary-in-department?code_type=6

# Title: Highest Salary In Department

"""
Find the employee with the highest salary per department.
Output the department name, employee's first name along with the corresponding salary.
"""


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy("department").orderBy(F.col("salary").desc())

employee = (
    employee
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .select("department", "first_name", "salary")
)

employee.toPandas()