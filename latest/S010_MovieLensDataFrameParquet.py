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

# write dataframe as parquet
# it also maintain the schema, column, data types
ratingDf.coalesce(1).write.mode("overwrite").parquet("/FileStore/tables/deloitte-aug-2021/ratings-parquet")

# COMMAND ----------

# read parquet file format
# it construct schema from parquet [not by using inferSchema]
ratingDfPar = spark.read.format("parquet").load("/FileStore/tables/deloitte-aug-2021/ratings-parquet")
ratingDfPar.printSchema()
ratingDfPar.show(3)

# COMMAND ----------

