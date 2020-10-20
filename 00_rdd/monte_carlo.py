#!/usr/bin/env python
# coding: utf-8

# In[1]:


import random

def gen_random_point(p):
    x = random.random() 
    y = random.random() # returns (0, 1)
    return (x, y)

def is_inside_circle(p):
    x = p[0]
    y = p[1]
    return (x**2 + y**2 < 1)


# In[20]:


#  len([ generate a "dummy list" with 0s or 1s... or does not matter ]) = N
#  -> map( 0 -> random point) -> filter(is inside the circle) -> count() = Nc

num_points = 1000000 # = N
dummy_points = range(num_points)
random_points = map(gen_random_point, dummy_points)
inside_points = filter(is_inside_circle, random_points)
Nc = len(list(inside_points))


# In[21]:


pi = 4 * (Nc / num_points)
print(pi)


# In[22]:


import pyspark
sc = pyspark.SparkContext()


# In[ ]:


# parallelize, map, filter, count()
# rdd = sc.parallelize(range(1000 * 1000))
# rdd.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).take(1)
# rdd.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).collect()
#
# Task: implement the same logic above in spark

