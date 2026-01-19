# Source: https://platform.stratascratch.com/coding/9881-make-a-report-showing-the-number-of-survivors-and-non-survivors-by-passenger-class?code_type=6

# Title: Titanic Survivors and Non-Survivors

"""
Make a report showing the number of survivors and non-survivors by passenger class. Classes are categorized based on the pclass value as:

•	First class: pclass = 1
•	Second class: pclass = 2
•	Third class: pclass = 3


Output the number of survivors and non-survivors by each class.
"""


import pyspark
import pyspark.sql.functions as F

titanic = (
    titanic
    .groupBy("survived")
    .agg(
        F.sum(F.when(F.col("pclass") == 1, 1).otherwise(0)).alias("first_class"),
        F.sum(F.when(F.col("pclass") == 2, 1).otherwise(0)).alias("second_class"),
        F.sum(F.when(F.col("pclass") == 3, 1).otherwise(0)).alias("third_class"),
    )
)

titanic.toPandas()