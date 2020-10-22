import pyspark
from pyspark.sql import SparkSession

def gen_spark_context(appName, local=False):
    conf = pyspark.SparkConf()
    conf.setAppName(appName)
    if local:
        conf.setMaster('local')
    sc = pyspark.SparkContext(conf=conf)
    spark = SparkSession(sc)
    return (sc, spark)

