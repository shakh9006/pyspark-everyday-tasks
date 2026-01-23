# Source: https://platform.stratascratch.com/coding/9728-inspections-that-resulted-in-violations?code_type=6

# Title: Number of violations

"""
You are given a dataset of health inspections that includes details about violations.
Each row represents an inspection, and if an inspection resulted in a violation, the violation_id column will contain a value.


Count the total number of violations that occurred at 'Roxanne Cafe' for each year, based on the inspection date.
 Output the year and the corresponding number of violations in ascending order of the year.
"""


import pyspark
import pyspark.sql.functions as F

sf_restaurant_health_violations = (
    sf_restaurant_health_violations
    .filter(F.col("business_name") == "Roxanne Cafe")
    .groupBy(F.year(F.col("inspection_date")))
    .agg(
        F.count("violation_id").alias("n_violations")
    )
)

sf_restaurant_health_violations.toPandas()