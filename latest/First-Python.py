# Databricks notebook source
# Shift + Enter to run it
print("Hello World")

# COMMAND ----------

name = "Python"
print(name)

# COMMAND ----------

# data structure
# list using [], elements accessed by using index starting from 0

names = ['spark', 'python', 'databricks', 'aws']
# append an item, mutable, not good for spark env
names.append ('azure')
names.append('google')

print(names)
print(len(names)) # size of the list


# COMMAND ----------

# how to access list members, left to right
print(names[0]) # list index
print (names[0: 2]) # range using :
print (names[2:]) # from 2
print (names[:4]) # upto 4 - 1

# Negative index, right to left
print (names[-1]) # last one  or first from right
print (names[2: -2]) # -2 is upper bound, -3

# COMMAND ----------

# list member check
print ('python' in names) # True
print ('glue' in names) # False
print ('glue' not in names) # True

# COMMAND ----------

# Dictoinary / Map, Key Value pair , {}, dict, : as delimitter

states = {'KA': 'Karnataka', 
          'KL': 'Kerela'
         }

states['TN'] = 'Tamil Nadu'

# to access object, use same [] with key
print(states['KA'])

# COMMAND ----------

# Tuple represented using ( ), Similar to List but Immutable
# once created tuples cannot be changed, updates, delete, add will not be allowed
# use for Spark

# sequence of elements, pairs of elements, 2 tuple, 3 or more
employee1 = ("John", "M", 4567)
# use all list accessors like :, we cannot mutate the tuple
print(employee1[1])
print (employee1[-1])
print (employee1[0:2])

# employee1[1] = "Mary" # error


# COMMAND ----------

# block statements are done using identation WHITE SPACE, TAB or SPACE, SPACE is recommended over TAB
# if statement
n = 11
if (n % 2 == 0):
  print("Even ", n)
else:
  print("Odd", n)
  print('done')

# COMMAND ----------

# if expression - returns a result
# if statement - doesn't return return

n = 11
# True result part if (predicate) else False result part
result =  "Even" if (n % 2 == 0) else "Odd"
print(result)

# COMMAND ----------

for i in range(5):
  print (i, i * i)

# COMMAND ----------

# function , reusable code, accept parameters, return result
# b =  0 is default, used when b arg value is not supplied
def add(a, b =  0):
  print(a, b)
  return a + b

# how to call, left to right
print(add(10, 20)) # a = 10, b = 20

print(add(10)) # using default value for b. A = 10, b = 0

# named arguments
print (add ( a = 10, b = 20)) # a = 10, b = 20
print (add ( b = 20, a = 10 )) # a = 10, b = 20

# COMMAND ----------

# function with *  variable number of arguments, 0 arg to N number args, args is tuple type
def sum(*numbers): 
  s = 0
  print("Numbers size ", len(numbers))
  print("Type", type (numbers))
  for n in numbers:
    s += n
    
  return s

print(sum()) # with no arg, return 0
print(sum(5)) # 5
print(sum(5, 10))  #15
print(sum(5, 10, 25, 30))  #70

marks = [90,80,99,98,76]

# sum(marks) # error the marks is passed as single entitye into numbers, numbers (   [90,80,99,98,76]     )
# convert the existing list, tuple into variable argument list
sum(*marks) # numbers (   90,80,99,98,76     )

# COMMAND ----------

# ** - keyworded variable number of arguments, 
# key and value pair (host=localhost, port=7777, path= /getdata)
# connectionArgs is a dict with key value pair
def connect(**connectionArgs):
  print("type", type(connectionArgs))
  print(connectionArgs) # print dict as is
  print(connectionArgs["host"])
  print(connectionArgs["port"])
  
connect(host='127.0.0.1', port = 7777, path = '/getdata')
connect(host='10.0.0.1', port = 7778, path = '/getdata')

# what if we already have dict/key-value pair, how to convert into keyworded  variable number of arguments
cArgs = { 'host': 'localhost', 'port': 7745, 'path': '/'  }
# convert dict into keyworded argument
connect(**cArgs)

# COMMAND ----------

# Destructring or unpacking

# assignment, a = 10, b = 20
a, b = 10, 20 
print (a, b)

# Tuple, x = 100,  y = 200

x, y = (100, 200)
print(x, y)

# a is 10, b = 20, c is list of remaining items [30, 40, 50, 60]
a, b, *c = [10, 20, 30, 40, 50, 60]
print (a, b)
print(c)

# COMMAND ----------

# Destructring or unpacking
# Debt, Name, Gender, Salary
employee = ('Account', 'Mary', 'F', 9000) # Tuple

# ignoring value using _
debt, _, _, salary = employee # unpacking from tuple/list, left to right
print(debt, salary)

# COMMAND ----------

data = [
  ('Account', 'Mary', 'F', 9000),
  ('Invoice', 'Joe', 'M', 7500),
   ('Operation', 'Jeff', 'M', 6000),
]

for debt, _, _ , salary in data:
  #print(employee[0], employee[1])
  print (debt, salary)

# COMMAND ----------

# class is a blueprint of an object
# class is a factory to create instance(s)
# class provide encapsulation, hide internal details of behavior, data stuffs
# class with member variables/attributes, behavior/member functions

class Product:
  # constructor in other languge
  # in python, called as initializer
  # called during object creation time to initizlise default state of an object
  # __init__ is a special function
  # self is nothing but this in other language
  # python, self/this is not implicit on the parameter list
  def __init__(self, name, price):
    print("Product created")
    # name and price are attributes of object
    self.name = name
    self.price = price
    pass
  
  # instance method
  def grandTotal(self): # self must explicit to receive the self/this
    self.total = self.price - self.price * .10
    return self.total
  
  # called when we do print(p1) or str concat "here our product "+ p1 
  # str to convert int to string
  def __str__(self):
    return 'Product(' + self.name + ',' + str(self.price) + ')'
  
# let us create object for product
# no new keyword in python
p1 = Product('iPhone', 50000) # calls __init__, pass newly created object as self

p2 = Product('Galaxy', 25000)

print(p1.name, p1.price)
print(p2.name, p2.price)

print (p1.grandTotal())  # p1 is passed as self implicit
print (p2.grandTotal()) # p2 is passed as self

# print (p1) # we try to print p1 object print as <__main__.Product object at 0x7efc361fdf90>
# but we want this to print output like Product(iPhone,50000)
print(p1) # print function or string concatenation calls a function called __str__ special function/toString in other lang

newOffer = "Here new phone " + str(p2) # calls __str__
print(newOffer)

# COMMAND ----------

