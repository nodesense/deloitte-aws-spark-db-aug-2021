# Databricks notebook source
# Python lambda is annonymous function wtihout name
# works same like function, no name to be given, no need to use def keyword

# add is method/function with name
# reusable
def add(a, b):
  return a + b

#lambda is annonymous, no name function
#sub is just a variable
# MUST be single line
sub = lambda a, b: a - b

print(add(10, 20))
print(sub(10, 20))

print(add) # <function add at 0x7f18f23834d0>
print(sub) # <function <lambda> at 0x7f18f23835f0> no name

# COMMAND ----------

