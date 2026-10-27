# Source: https://www.sparkplayground.com/pyspark-coding-interview-questions/word-count-program-pyspark-rdd

# Title: Word Count Program in PySpark

"""
Write a PySpark program to count the occurrences of each word in a given text file. 
The solution must utilize RDD transformations and actions for processing, and then convert the final 
RDD into a DataFrame for output. Sort DataFrame by count in descending order.

File Path: /datasets/notes.txt

"""
# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

sc = spark.sparkContext

lines_rdd = sc.textFile("/datasets/notes.txt")

words_rdd = lines_rdd.flatMap(lambda line: line.split())

pairs_rdd = words_rdd.map(lambda word: (word, 1))

counts_rdd = pairs_rdd.reduceByKey(lambda a, b: a + b)

sorted_rdd = counts_rdd.sortByKey()

df = spark.createDataFrame(sorted_rdd, schema=["word", "count"])

df = df.orderBy(col("count").desc())

display(df)