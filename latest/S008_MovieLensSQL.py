# Databricks notebook source
"""
/FileStore/tables/deloitte-aug-2021/movies.csv
/FileStore/tables/deloitte-aug-2021/ratings.csv

/FileStore/tables/deloitte-aug-2021/links.csv
/FileStore/tables/deloitte-aug-2021/tags.csv
"""

# COMMAND ----------

# Data Frame: Structured Data, where we have columns (column name, column type) and then data
# data frame: meta data (Schema) + data rows [RDD] + APIs [filter, where, join, agg, etc]

# load csv file using data frame
# spark - Spark Session, Entry point for Spark SQL [DataFrame etc]
# for each driver program, there will be ONLY ONE SPARK CONTEXT
# for each driver program, there can be ONE or MORE SPARK SESSION

# we don't have schema, but spark dataframe csv driver will build one schema for us
# default csv will fix all columns with string type
tagsDf = spark.read.format("csv")\
         .option("header", True)\
         .load('/FileStore/tables/deloitte-aug-2021/tags.csv')

# ***DON't USE THIS WAY
tagsDf.printSchema()
# access schema using schema
tagsDf.show(5) # shows 5 records

# COMMAND ----------

# to access schema attached to dataframe
tagsDf.schema

# to access the data rows
# dataframe has rdd, data frame/sql is API, where RDD is core
print(tagsDf.rdd.getNumPartitions())
print(tagsDf.rdd.take(5))


# COMMAND ----------

# load data using inferSchema, request Spark itself to analyse and build a schema with proper data types
# ***DON't USE THIS WAY for LARGE FILES, but OK for small files
linksDf = spark.read.format("csv")\
         .option("header", True)\
         .option("inferSchema", True)\
         .load('/FileStore/tables/deloitte-aug-2021/links.csv')

linksDf.printSchema()
linksDf.show(5)

# COMMAND ----------

# define the schema ourself, better approach
from pyspark.sql.types import StructType, LongType,StringType, IntegerType, DoubleType

movieSchema = StructType()\
              .add("movieId", IntegerType(), True)\
              .add("title", StringType(), True)\
              .add("genres", StringType(), True)


movieDf = spark.read.format("csv")\
         .option("header", True)\
         .schema(movieSchema) \
         .load('/FileStore/tables/deloitte-aug-2021/movies.csv')

movieDf.printSchema()
movieDf.show(4)
 

# COMMAND ----------

# ratingDf usign schema
# how to avoid using \ for new line continuation with paranthesis

ratingSchema = (StructType()
         .add("userId", IntegerType(), True)
         .add("movieId", IntegerType(), True)
         .add("rating", DoubleType(), True)
         .add("timestamp", LongType(), True))


ratingDf = (spark.read.format("csv")
         .option("header", True)
         .schema(ratingSchema)
         .load('/FileStore/tables/deloitte-aug-2021/ratings.csv'))
 
ratingDf.printSchema()
ratingDf.show(4)

# COMMAND ----------

# Spark has database [NOT ATOMIC, NO INTEGRITY etc], based on Files
# Database [Meta data (database name, table names, table schmea, actual data location) + actual data]
# 4 types tables
#  1. Temp Table [View]
#  2. Global Temp Table [Global Temp view]
#  3. Managed Table [Persisted, managed by Spark, insert/update/delete/select statements]
#  4. External table [Schema is managed by spark, the content is located elsewhere, managed by other  tools like ETL, Glue etc]

# COMMAND ----------

spark.sql("SHOW DATABASES").show()
spark.sql("SHOW TABLES").show()

# COMMAND ----------

# create a temp view
# temporary table, available within spark session
# it will be removed once spark session/driver terminated
# SQL API, internally DataSet/RDD, Dataframe
ratingDf.createOrReplaceTempView("ratings")

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

# spark.sql returns data frames
df = spark.sql("SELECT * FROM ratings")
df.printSchema()
df.show(2)

# COMMAND ----------

df = spark.sql("SELECT * FROM ratings")
df.show(5) # ASCII FORMAT PRINT
display(df) # data bricks only, html output, interactive, html table, charts

# COMMAND ----------

# MAGIC Function availabel in notebooks jupyter, databricks has one
"""
magic functions starts with %sql %pthon %scala etc 

%sql
 SELECT movieId, rating from ratings
 
" SELECT movieId, rating from ratings" shall be taken as input, put into 
spark.sql ( SELECT movieId, rating from ratings ) and execute and get the result
then displayed using display function (HTML based one)
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL comment, this sql block executed inside spark.sql() and displayed using display
# MAGIC SELECT * FROM ratings

# COMMAND ----------

# store the result of the SQL SELECT statement into another temp view
# CTAS CREATE TEMP TABLE AS SELECT **** from ..
popularMovies = spark.sql("""
CREATE OR REPLACE TEMP VIEW popular_movies AS
SELECT movieId, 
       count(userId) AS total_ratings, 
       avg(rating) AS avg_rating        
FROM
       ratings
GROUP BY movieId
HAVING    total_ratings >= 100 AND     avg_rating >= 3.5
ORDER BY avg_rating DESC
""")

display(popularMovies)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES

# COMMAND ----------

# how to get the sql table /temp table as data frame
popularMoviesDf = spark.table("popular_movies") # returns popular_movies dataframe
popularMoviesDf.printSchema()

# COMMAND ----------

movieDf.createOrReplaceTempView('movies')

# COMMAND ----------

spark.sql ("""
 CREATE OR REPLACE TEMP VIEW most_popular_movies AS
 SELECT  pm.movieId, movies.title, pm.avg_rating, pm.total_ratings, movies.genres
 FROM   popular_movies pm
 INNER JOIN  movies ON pm.movieId == movies.movieId
""")

# COMMAND ----------

most_popular_movies = spark.table('most_popular_movies')
most_popular_movies.show(10)
# write to CSV, JSON etc

# COMMAND ----------

