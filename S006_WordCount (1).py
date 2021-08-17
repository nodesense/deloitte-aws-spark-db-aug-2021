# Databricks notebook source
# loading data, Lazy method
#'/FileStore/tables/wordcount/book.txt'
fileRdd = sc.textFile('/FileStore/tables/wordcount/shakespeare.txt')
print("partitions ", fileRdd.getNumPartitions())

fileRdd = fileRdd.repartition(20) # creates 20 partitions, distribute the data to 20 partitions
print("partitions ", fileRdd.getNumPartitions())


# COMMAND ----------

fileRdd.count() # action method, line count

# COMMAND ----------

fileRdd.collect()

# COMMAND ----------

lowerCaseRdd = fileRdd.map ( lambda line: line.lower())
# take fetch the data from first partition, first 2 records, better than collect
lowerCaseRdd.take(2) # action method

# COMMAND ----------

# split the line to list of words
wordsListRdd = lowerCaseRdd.map (lambda line: line.split(" "))
wordsListRdd.take(10) # returns list of list of words

# COMMAND ----------

# convert list of list of words/elements into words list
# flatMap, flatten an array/list into individual element
wordsRdd = wordsListRdd.flatMap(lambda list: list) # converts into words list/flatten
wordsRdd.take(5)

# COMMAND ----------

# software: 1 but as tuple (software, 1)
# hardware: 1 but as tuple (hardware, 1)

wordsPairRdd = wordsRdd.map (lambda word: (word, 1) ) # return tuple  (<<word>>, 1) (software, 1)
wordsPairRdd.take(5)

# COMMAND ----------

# word count now, using reduceByKey

"""
 ('paradigms', 1) - paired RDD , key and a value (key = paradigms, value = 1)
 acc = accumulator where the results will be stored, managed by reduceByKey internally
 
 Input: wordsPairRdd
 
 ('artificial', 1),     -- reduceByKJey is not called
  ('in', 1),   -- reduceByKJey is not called
  ('paradigms', 1) -- reduceByKJey is not called
 ('artificial', 1), <- calls reducebyKey, as artifical already there in temp result
                       calls reducebyKey(accomulatorValue, value) => accomulatorValue + value     (1=acc, 1=value ) => 1 + 1 (2)
                       then it updates the  temp result with new value 2
('artificial', 1), <- calls reducebyKey (2 = acc, 1 = value) =>2 + 1 (3)

reduceByKey shall not be called for the first entry 
A temp data structure within reduceByKey

    key,      Accumulator
('artificial',   3)                
 ('in'         , 1)                
('paradigms'    , 1)                    


"""

wordCountRdd = wordsPairRdd.reduceByKey(lambda acc, value: acc + value)
wordCountRdd.take(20)

# COMMAND ----------

# write the result to text file with default partitions
print ("partitions ", wordCountRdd.getNumPartitions())
# prints 2 parition, now we write to text file, 1 partion = 1 task, 2 tasks  total we have.
# this means that, two files shall be created for output based on partition
wordCountRdd.saveAsTextFile('/FileStore/tables/wordcount/results2')

# COMMAND ----------

print ("partitions ", wordCountRdd.getNumPartitions())
# prints 2 parition, now we write to text file, 1 partion = 1 task, 2 tasks  total we have.
# this means that, two files shall be created for output based on partition
wordCountRdd = wordCountRdd.coalesce(1)
print ("partitions ", wordCountRdd.getNumPartitions()) # 1
wordCountRdd.saveAsTextFile('/FileStore/tables/wordcount/results3')

# COMMAND ----------


