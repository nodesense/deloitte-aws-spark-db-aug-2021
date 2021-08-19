# Databricks notebook source
# THIS IS DRIVER application

# use TAB key for intellisense
#sc.pythonVer

# RDD - Resillient Distributed DataSet
# data distributed across cluster
# data SHOULD be immutable

# loading static/hardcoded/data in list/collection into spark memory
data = [1,2,3,4,5]
rdd = sc.parallelize(data) # lazy create on need basic


# COMMAND ----------

# this odd function is executed in executor/worker node, not on the driver node
# this python function is shipped to executor/worker node
def odd(n):
  print("Odd func", n)
  if (n % 2 == 1):
    return True
  else:
    return False
  
# we pass odd function into filter function
# Transformation method, lazy method
oddRdd = rdd.filter (odd)


# COMMAND ----------


# action 
# collect func, bring the result data back to the driver program
# collect is an action function, it loads data, execute the transformation
# Every action create a Spark JOB, the job is executed in the  cluster 
# A Job execution involve,
#  1. Driver: Convert RDD [Tree model] into DAG [Graph Model] - Directed Acylic Graph [Flatten model]
#  2. Driver: DAG is tranformed into Stages - A set of tasks
#  3. Worker: Task is executed on worker node [VM/System]
#     3.0  The data is loaded into spark as parition
#     3.1 Task needs data in forms of partition [sub of your dataset]
#  4. The result is collected, brought back to the driver progrmam [this notebook]
result = oddRdd.collect()

print(result)


# COMMAND ----------

# min is action method
min = rdd2.min() # return min output 1, 3, 5
print("Min", min)

# COMMAND ----------

max = rdd2.max()
print("Max", max)

# COMMAND ----------

rdd3 = rdd.filter (lambda n: n % 2 == 0) # pick only even number
print(rdd3.collect())

# COMMAND ----------

