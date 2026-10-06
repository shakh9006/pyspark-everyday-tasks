# Source: https://www.sparkplayground.com/pyspark-coding-interview-questions/handling-null-values

# Title: Handling Null Values

"""
You are provided with a dataset containing customer information. 
The dataset may have missing values in the customer_id or email columns. 
Your task is to filter out any rows where either customer_id or email is null.

File Path: /datasets/customers_raw.csv

"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

df = spark.read.csv("/datasets/customers_raw.csv", header=True, inferSchema=True)

df_result = df.filter((F.col("customer_id").isNotNull()) & (F.col("email").isNotNull()))

display(df_result)