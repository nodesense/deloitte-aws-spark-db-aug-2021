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

print("ratingDf partitions", ratingDf.rdd.getNumPartitions())
ratingDf = ratingDf.repartition(16)
print("ratingDf repartitioned ", ratingDf.rdd.getNumPartitions())


# COMMAND ----------

print (ratingDf.count())
print (ratingDf.columns)


# COMMAND ----------

# select shall project specific columns
# DATA FRAME is IMMUTABLE, WE CANNOT ADD, REMOVE, UPDATE content on original dataframe
# we can create/derive new data frame from existing
# every dataframe even if it is derived has its own schema, dataset
df = ratingDf.select("movieId", "rating")
df.printSchema()
df.show(4)

# COMMAND ----------

# derive a new column, value from existing dataframe
df = ratingDf.withColumn("rating_adjusted", ratingDf.rating + 0.2)
df.show(2)

# the original dataframe is there as is, READ only
ratingDf.show(2)

# COMMAND ----------

ratingDf.withColumnRenamed("rating", "ratings").show(3)

# COMMAND ----------

ratingDf.select('*').show(2)

# alias 
ratingDf.select(ratingDf.rating, (ratingDf.rating + 0.2).alias("adjusted") ).show(4)

# COMMAND ----------

# filter, where are same, just alias function
ratingDf.filter(ratingDf.rating > 4).show(2)
ratingDf.where(ratingDf.rating > 4).show(2)


# COMMAND ----------

# multiple conditions
ratingDf.filter ( (ratingDf.rating >3) & (ratingDf.rating <= 4) ).show(5)

# COMMAND ----------

from pyspark.sql.functions import col 

print("Col", col("rating"))
print("col Data", ratingDf.rating)
// print (col("rating") == ratingDf.rating)

# COMMAND ----------

ratingDf.filter ( col("rating") > 4.0).show()

# COMMAND ----------

from pyspark.sql.functions import asc, desc

# ascending order
ratingDf.sort("rating").show(5)

# ascending order
ratingDf.sort(asc("rating")).show(5)

ratingDf.sort(desc("rating")).show(5)

# COMMAND ----------

# module import alias name
# importing all with alias name
import pyspark.sql.functions as F
 
print(ratingDf.rdd.getNumPartitions())

# the most popular movies, at least rated by 100 users, avg rating should be > 3.5
mostPopularDf = ratingDf\
                .groupBy("movieId")\
                .agg(F.count("userId"), F.avg('rating').alias("avg_rating"))\
                .withColumnRenamed("count(userId)", "total_ratings")\
                .filter( (F.col("total_ratings") >= 100) & (F.col("avg_rating") >= 3.5) )\
                .sort (desc("avg_rating"))


mostPopularDf.printSchema()
mostPopularDf.show(50)


# COMMAND ----------

# joins, inner join

mostPopularMoviesDf = mostPopularDf\
                      .join(movieDf, movieDf.movieId == mostPopularDf.movieId)\
                      .select(mostPopularDf.movieId, 'title', 'total_ratings', 'avg_rating', 'genres')

mostPopularMoviesDf.show(10)

# COMMAND ----------

# mostPopularMoviesDf.cache()

mostPopularMoviesDf.write.mode('overwrite')\
                   .csv('/FileStore/tables/deloitte-aug-2021/most-popular-movies-csv')

# COMMAND ----------

# reduce to single partition and write to file system
mostPopularMoviesDf.coalesce(1)\
                   .write\
                    .option('header', True)\
                    .mode('overwrite')\
                   .csv('/FileStore/tables/deloitte-aug-2021/most-popular-movies-single-csv')

# COMMAND ----------


resultFile = sc.textFile('/FileStore/tables/deloitte-aug-2021/most-popular-movies-single-csv')

resultFile.take(5)

# COMMAND ----------

popularMoviesFromResultDf = spark.read.format("csv")\
         .option("header", True)\
         .option("inferSchema", True)\
         .load('/FileStore/tables/deloitte-aug-2021/most-popular-movies-single-csv')

popularMoviesFromResultDf.printSchema()
popularMoviesFromResultDf.show(10)

# COMMAND ----------


