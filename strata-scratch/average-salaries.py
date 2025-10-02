# Source: https://platform.stratascratch.com/coding/9917-average-salaries?code_type=6

# Title: Average Salaries

"""
Compare each employee's salary with the average salary of the corresponding department.
Output the department, first name, and salary of employees along with the average salary of that department.
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window

window_spec = Window.partitionBy('department')

avg_emp_salary = employee.withColumn('average_salary', F.avg('salary').over(window_spec))

employee = avg_emp_salary.select('department', 'first_name', 'salary', 'average_salary')

employee.toPandas()