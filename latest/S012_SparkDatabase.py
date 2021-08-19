# Databricks notebook source
spark.sql("SHOW DATABASES").show()

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS moviedb").show()

# COMMAND ----------

spark.sql("SHOW DATABASES").show()

# COMMAND ----------

# show tables in moviedb
spark.sql("SHOW TABLES IN moviedb").show()

# COMMAND ----------

# creating a permanent table, can be used by as many spark applications
# we are create a managed table, it means we need use spark to insert/update/delete records instead of accessing and updating in directory
# META Data: Database name, table names, columns, column type and actual location where data stored
# data is stored in flat files
spark.sql("""
   CREATE TABLE IF NOT EXISTS moviedb.bookings(
       userId INT,
       movieId INT,
       seats INT,
       amount DOUBLE
   )
   """)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS moviedb.bookings(
# MAGIC        userId INT,
# MAGIC        movieId INT,
# MAGIC        seats INT,
# MAGIC        amount DOUBLE
# MAGIC    )

# COMMAND ----------

# show tables in moviedb
spark.sql("SHOW TABLES IN moviedb").show()

# COMMAND ----------

# FIXME: why no spark job at very first time for table without data
spark.sql("SELECT * FROM moviedb.bookings").show()

# COMMAND ----------

spark.sql("""
INSERT INTO moviedb.bookings  VALUES (1,1,5,500)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO moviedb.bookings  VALUES (1,23,3,300) 

# COMMAND ----------

spark.sql("SELECT * FROM moviedb.bookings").show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- update, not supported in normal tables, we need DELTA table
# MAGIC UPDATE moviedb.bookings SET seats = 6 WHERE movieId=3

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- DELETE, not supported in normal tables, we need DELTA table
# MAGIC DELETE from moviedb.bookings  WHERE movieId=3

# COMMAND ----------

