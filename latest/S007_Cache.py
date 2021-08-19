# Databricks notebook source
import pyspark
# Cache - cache RDD data/DataFrame

# cache is used to minimize the IO workload that is performed repeatly again and again
# cache is used to improve CPU workload instead of doing same tasks again and again
# whenever we reuse RDD, you may look over the cache feature
#   output is ready, now write a csv file, also write results to OLTP / Postgres, OLAP / Redshift, also write to S3
# \ line continuation

def show(line):
  print("processing ", line)
  return line

# lazy part, no action
wordCountRdd = sc.textFile('/FileStore/tables/wordcount/shakespeare.txt') \
  .repartition(20) \
  .map(lambda line: show(line)) \
  .map ( lambda line: line.lower()) \
  .flatMap(lambda line: line.split(" "))\
  .map (lambda word: (word, 1) )\
  .reduceByKey(lambda acc, value: acc + value)

# cache - MEMORY_AND_DISK first store in memory, if not enough in memory, it spills to disk
wordCountRdd.cache() # worker cache, cached when the first execution take place, lazy method
#wordCountRdd.persist(pyspark.StorageLevel.DISK_ONLY) #- takes options, DISK_ONLY, MEMORY_ONLY
wordCountRdd.count() # now wordCountRdd is cached

# COMMAND ----------

wordCountRdd.count()

# COMMAND ----------

wordCountRdd.saveAsTextFile('/FileStore/tables/wordcount/to-s3-batch2')

# COMMAND ----------

wordCountRdd.saveAsTextFile('/FileStore/tables/wordcount/to-azure-batch2')

# COMMAND ----------

wordCountRdd.saveAsTextFile('/FileStore/tables/wordcount/to-hdfs-batch2')

# COMMAND ----------

wordCountRdd.count()

# COMMAND ----------

