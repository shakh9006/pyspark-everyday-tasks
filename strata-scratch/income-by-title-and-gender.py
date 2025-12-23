# Source: https://platform.stratascratch.com/coding/10077-income-by-title-and-gender?code_type=6

# Title: Income By Title and Gender

"""
Find the average total compensation based on employee titles and gender.
 Total compensation is calculated by adding both the salary and bonus of each employee.
 However, not every employee receives a bonus so disregard employees without bonuses in your calculation.
 Employee can receive more than one bonus.
Output the employee title, gender (i.e., sex), along with the average total compensation.
"""


import pyspark
import pyspark.sql.functions as F

result = (
    sf_employee
    .join(
        sf_bonus,
        on=F.col("worker_ref_id") == F.col("id"),
        how="left"
    )
    .select(F.col("id"), F.col("employee_title"), F.col("sex"), F.col("bonus"), F.col("salary"))
    .filter(F.col("bonus").isNotNull())
    .groupBy("id", "employee_title", "sex", "salary")
    .agg(
        F.sum("bonus").alias("bonus")
    )
    .groupBy("employee_title", "sex")
    .agg(
        F.avg(F.col("bonus") + F.col("salary")).alias("bonus_avg")
    )
)


result.toPandas()