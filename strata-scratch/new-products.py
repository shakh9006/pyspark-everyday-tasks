# Source: https://platform.stratascratch.com/coding/10318-new-products?code_type=6

# Title: New Products

"""
Calculate the net change in the number of products launched by companies in 2020 compared to 2019.
Your output should include the company names and the net difference.
(Net difference = Number of products launched in 2020 - The number launched in 2019.)

"""

import pyspark
import pyspark.sql.functions as F

result = (
    car_launches
    .groupBy('company_name')
    .agg(
        F.count(F.when(F.col('year') == '2019', 1)).alias('total_2019'),
        F.count(F.when(F.col('year') == '2020', 1)).alias('total_2020')
    )
)

result = (
    result
    .withColumn("net_products", F.col("total_2020") - F.col("total_2019"))
    .select("company_name", "net_products")
)

result.toPandas()