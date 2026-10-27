# Source: https://www.sparkplayground.com/pyspark-coding-interview-questions/running_payroll

# Title: Running Payroll


"""
You work in the payroll department and are asked to process the payroll data of a company. You are given two DataFrames: employees and payroll. 
employeescontains employee information, andpayrollcontains the payroll data.

Write a function that returns the calculated pay for each employee.

File Path: /datasets/customer_purchases.csv

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

payroll = (
  payroll.withColumn(
    "pay",  
    F.when(F.col("hours_worked") <= 40.0, F.col("hours_worked") * F.col("hourly_rate"))
    .otherwise((40.0 * F.col("hourly_rate")) + ((F.col("hours_worked") - 40) * (F.col("hourly_rate") * 1.5)))
  )
)


df_result = (
  payroll
  .join(
    employees,
    on="employee_id",
    how="inner"
  )
  .select("employee_id", "name", "pay", "position")
)

display(df_result)
    