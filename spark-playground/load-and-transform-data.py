# Source: https://www.sparkplayground.com/pyspark-coding-interview-questions/load-and-transform-data

# Title: Load and Transform Data

"""
You need to process a customer dataset to identify high-value customers. Specifically, you will:

1. Read data from a CSV file with inferSchema option as true.
2. Filter customers with a purchase amount more than 100 USD.
3. Further filter to include only customers aged 30 or above.
4. Use display(df) to show the final DataFrame.

File Path:
/datasets/customers.csv

"""

import pyspark
import pyspark.sql.functions as F

result = (
    fb_friend_requests
    .groupBy(F.col("user_id_sender"), F.col("user_id_receiver"))
    .agg(
        F.min("date").alias("request_date"),
        F.lit(1).alias("sent_count"),
    )
)

result = (
    result.join(# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

df = spark.read.csv("/datasets/customers.csv", header=True, inferSchema=True)

df_result = (
  df
  .filter((df.purchase_amount > 100) & (df.age >= 30))
  .select("customer_id", "name", "purchase_amount")
)

display(df_result)