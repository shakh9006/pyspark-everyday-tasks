# Source: https://platform.stratascratch.com/coding/10182-number-of-streets-per-zip-code?code_type=6

# Title: Number of Streets Per Zip Code

"""
Count the number of unique street names for each postal code in the business dataset.
Use only the first word of the street name, case insensitive (e.g., "FOLSOM" and "Folsom" are the same).
If the structure is reversed (e.g., "Pier 39" and "39 Pier"),
count them as the same street. Output the results with postal codes,
ordered by the number of streets (descending) and postal code (ascending).
"""


import pyspark
import pyspark.sql.functions as F

df = (
    sf_restaurant_health_violations
    .withColumn("temp", F.split(sf_restaurant_health_violations['business_address'], ' '))
)

df = (
    df.withColumn(
        "stname",
        F.when(df["temp"][0].rlike('[0-9]'), df["temp"][1]).otherwise(df["temp"][0])
    )
    .filter(F.col("business_postal_code").isNotNull())
    .select("business_postal_code", F.lower("stname").alias("st_name")).distinct()
)

sf_restaurant_health_violations = (
    df
    .groupBy(F.col("business_postal_code").alias("postal_code"))
    .agg(
        F.count("st_name").alias("n_streets")
    )
    .orderBy(F.col("n_streets").desc())
)

sf_restaurant_health_violations.toPandas()