# Source: https://platform.stratascratch.com/coding/10353-workers-with-the-highest-salaries?code_type=6

# Title: Workers With The Highest Salaries

"""
Management wants to analyze only employees with official job titles.
Find the job titles of the employees with the highest salary.
If multiple employees have the same highest salary, include all their job titles.

"""

import pyspark
import pyspark.sql.functions as F

data = worker.join(title, worker.worker_id == title.worker_ref_id, 'inner')
max_salary = data.select(F.max('salary')).first()[0]

result = (
    data\
        .select(F.col('worker_title').alias('best_paid_title'))\
        .where(F.col('salary') == max_salary)
)

result.toPandas()