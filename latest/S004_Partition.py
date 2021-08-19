# Databricks notebook source
data = range(10)

# 8 parallism [community edition]
print("Default min partition", sc.defaultMinPartitions)
print("Default min parallism", sc.defaultParallelism)

rdd = sc.parallelize(data, 4) # takes default partitions from sc.defaultParallelism

# for each v.core, we can have minimum 3 to 4 tasks at a time 

# COMMAND ----------

# Partition
# A Sub set of data of the whole data set
# parition is used for spark task to execute the subset of data as one task

print ("partitions ", rdd.getNumPartitions())

# collect data from parititions
# returns list of list
# glom() collect data from parititon as is
# blom() is an action method
partitionData = rdd.glom().collect()

for  pd in partitionData:
  print("partition ",   pd)

# COMMAND ----------

def odd(n):
  print("odd", n)
  if (n % 2 == 1):
    return True
  else:
    return False
  
# filter, accept lambda, that calls odd function is basically part of task
rdd2 = rdd.filter (lambda n: odd(n))
# multiply all the odd numbers by 10
# map is transformation function
# map, lambda inside is part of the task
rdd3 = rdd2.map (lambda n: n * 10)

partData = rdd3.glom().collect()
for pd in partData:
  print(pd)

# COMMAND ----------

