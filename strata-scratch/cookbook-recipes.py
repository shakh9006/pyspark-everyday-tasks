# Source: https://platform.stratascratch.com/coding/2089-cookbook-recipes?code_type=6

# Title: Cookbook Recipes

"""
You are given a table containing recipe titles and their corresponding page numbers from a cookbook.
Your task is to format the data to represent how recipes are distributed across double-page spreads in the book.


Each spread consists of two pages:


⦁   The left page (even-numbered) and its corresponding recipe title (if any).
⦁   The right page (odd-numbered) and its corresponding recipe title (if any).


The output table should contain the following three columns:


⦁   left_page_number – The even-numbered page that starts each double-page spread.
⦁   left_title – The title of the recipe on the left page (if available).
⦁   right_title – The title of the recipe on the right page (if available).


For the  k-th  row (starting from 0):


⦁   The  left_page_number  should be 2 × k.
⦁   The  left_title  should be the title from page 2 × k, or NULL if there is no recipe on that page.
⦁   The  right_title  should be the title from page 2 × k + 1, or NULL if there is no recipe on that page.


Each page contains at most one recipe and  if a page does not contain a recipe, the corresponding title should be NULL.
Page 0 (the inside cover) is always empty and included in the output. Only include spreads where at least one of the two pages has a recipe.
"""

import pyspark
import pyspark.sql.functions as F

pages = (
    cookbook_titles
    .withColumn(
        "page",
        F.when(F.col("page_number") % 2 == 0, F.col("page_number"))
        .otherwise(F.col("page_number") - 1)
    )
    .select("page")
    .dropDuplicates()
    .orderBy("page")
)


result = (
    pages
    .alias("p")
    .join(
        cookbook_titles
        .alias("l"),
        on=F.col("p.page") == F.col("l.page_number"),
        how="left"
    )
    .join(
        cookbook_titles
        .alias("r"),
        on=F.col("p.page") + 1 == F.col("r.page_number"),
        how="left"
    )
    .select(
        F.col("page").alias("left_page_number"),
        F.col("l.title").alias("left_title"),
        F.col("r.title").alias("right_title")
    )
    .orderBy("left_page_number")
)

result.toPandas()
