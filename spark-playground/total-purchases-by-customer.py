# Source: https://www.sparkplayground.com/pyspark-coding-interview-questions/total-customer-purchases

# Title: Total Purchases by Customer

"""
Given a dataset of customer purchases, your task is to group the data by customer and calculate the total purchase amount for each customer. 
You will need to group by customer_id and sum up the purchase_amount for each individual.
Order the result by customer_id
Use display(df) to show the final DataFrame.

File Path: /datasets/customer_purchases.csv

"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

df = spark.read.csv("/datasets/customer_purchases.csv", header=True, inferSchema=True)

df_result = (
  df
    .groupBy(F.col("customer_id"))
    .agg(
      F.sum(F.col("purchase_amount")).alias("total_purchase")
    )
    .orderBy(F.col("customer_id").asc())
)

display(df_result)