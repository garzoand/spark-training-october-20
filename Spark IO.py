#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession

sc = pyspark.SparkContext()
spark = SparkSession(sc)


# In[22]:


from pyspark.sql.types import StructField, StructType, StringType, LongType
csv_schema = StructType([
    # StructField (name, dataType, nullable, metadata)
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("count", LongType(), False)    
])

# spark.read is a DataFrameReader singleton class
df = spark.read     .format('csv')     .option('header', 'true')     .schema(csv_schema)     .load('spark_training_baseline/data/flights_multiple')

#.load('spark_training_baseline/data/flights.csv')
df.show()

print(df.rdd.getNumPartitions())


# In[5]:


get_ipython().system('head -10 spark_training_baseline/data/flights.json')


# In[13]:


df_json = spark.read     .format('json')     .option('compression', 'gzip')     .schema(csv_schema)     .load('/home/andras/git/spark/data/flights.json.gz')
df_json.show()


# In[16]:


df.createOrReplaceTempView('flights')
result_df = spark.sql("""
SELECT ORIGIN_COUNTRY_NAME, sum(count) as total_outbound
FROM flights
GROUP BY ORIGIN_COUNTRY_NAME
ORDER BY total_outbound DESC;
""")
result_df.show()


# In[17]:


result_df.write     .format('csv')     .option('header', 'true')     .option('sep', ';')     .mode('overwrite')     .save('data/flgiths_stat')   


# In[23]:


get_ipython().system('ls data/flgiths_stat')


# In[24]:


df2 = spark.read     .format('csv')     .option('header', 'true')     .option('sep', ';')     .load('data/flgiths_stat')   


# In[25]:


df2.rdd.getNumPartitions()


# In[30]:


df3 = df2.repartition(3)
df3.rdd.getNumPartitions()


# In[33]:


df4 = df3.repartition(1)
df4.write     .format('csv')     .option('header', 'true')     .option('sep', ';')     .mode('overwrite')     .save('data/flgiths_stat2')   


# In[36]:


get_ipython().system('ls data/flgiths_stat2')


# In[35]:


#df4 = df3.repartition(1)
df4 = df3.coalesce(1)
df4.write     .format('csv')     .option('header', 'true')     .option('sep', ';')     .mode('overwrite')     .save('data/flgiths_stat2')   


# In[38]:


df_json.write     .format('csv')     .option('header', 'true')     .option('sep', ';')     .mode('overwrite')     .partitionBy('ORIGIN_COUNTRY_NAME')     .save('data/flgiths_stat2')   


# In[39]:


get_ipython().system('pwd')


# In[40]:


df4.write     .format('parquet')     .mode('overwrite')     .save('data/flgiths_pq')   


# In[41]:


get_ipython().system('ls data/flgiths_pq')


# In[42]:


df_from_pq = spark.read     .format('parquet')     .load('data/flgiths_pq')   


# In[45]:


df_from_pq.select('ORIGIN_COUNTRY_NAME').show()


# In[44]:


get_ipython().system('pwd')


# In[ ]:




