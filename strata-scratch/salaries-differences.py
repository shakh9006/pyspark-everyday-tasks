# Source: https://platform.stratascratch.com/coding/10308-salaries-differences?code_type=6

# Title: Salaries Differences

"""
Calculates the difference between the highest salaries in the marketing and engineering departments.
Output just the absolute difference in salaries.
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

db_dept = db_dept.withColumnRenamed("id", "department_id")
db_employee = db_employee.join(db_dept, how='inner', on="department_id")

window_spec = Window.partitionBy("department").orderBy(F.desc(F.col("salary")))
result = db_employee.withColumn("rn", F.rank().over(window_spec))

result = result.agg(
        F.max(F.when(F.col("department") == "engineering", F.col("salary"))).alias("eng_highest"),
        F.max(F.when(F.col("department") == "marketing", F.col("salary"))).alias("mark_highest"),
       )

result = (
    result
    .withColumn("salary_diff", F.col("mark_highest") - F.col("eng_highest"))
    .select("salary_diff")
)

result.toPandas()