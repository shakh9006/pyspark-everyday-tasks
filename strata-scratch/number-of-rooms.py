# Source: https://platform.stratascratch.com/coding/9622-number-of-bathrooms-and-bedrooms?code_type=6

# Title: Number Of Bathrooms And Bedrooms

"""
Find the average number of bathrooms and bedrooms for each city’s property types.
Output the result along with the city name and the property type.

"""

import pyspark
import pyspark.sql.functions as F

result = (
    airbnb_search_details
    .groupBy('city', 'property_type')
    .agg(F.avg(F.col('bedrooms')).alias('n_bedrooms_avg'), F.avg(F.col('bathrooms')).alias('n_bathrooms_avg'))
)

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()