# Source: https://platform.stratascratch.com/coding/10299-finding-updated-records?code_type=6

# Title: Finding Updated Records

"""
We have a table with employees and their salaries, however, some of the records are old and contain outdated salary information.
Find the current salary of each employee assuming that salaries increase each year.
Output their id, first name, last name, department ID, and current salary. Order your list by employee ID in ascending order.
"""


import pyspark
from pyspark.sql.window import Window
import pyspark.sql.functions as F

w = Window.partitionBy("id").orderBy(F.col("salary").desc())
rows_with_rank = ms_employee_salary.withColumn('rn', F.rank().over(w))

result = (
    rows_with_rank.select(
        'id',
        'first_name',
        'last_name',
        'department_id',
        'salary'
    ).where(F.col('rn') == 1)
)

result.show()

result.toPandas()