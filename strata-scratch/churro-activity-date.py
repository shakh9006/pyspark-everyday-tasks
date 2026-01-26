# Source: https://platform.stratascratch.com/coding/9688-churro-activity-date?code_type=6

# Title: Churro Activity Date

"""
Find the inspection date and risk category (pe_description) of facilities named 'STREET CHURROS' that received a score below 95.
"""

import pyspark
import pyspark.sql.functions as F

los_angeles_restaurant_health_inspections = (
    los_angeles_restaurant_health_inspections
    .filter(F.col("facility_name") == "STREET CHURROS")
    .select("activity_date", "pe_description")
)

los_angeles_restaurant_health_inspections.toPandas()
