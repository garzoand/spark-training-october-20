import pyspark
from pyspark.sql import SparkSession
import pipeline
import utils

ACCESS_LOG_INPUT = '...'
EVIL_IP_INPUT = '...'

EVIL_REPORT_OUTPUT_FOLDER = '...'

## Create and Configure Spark Context and Session
(sc, spark) = utils.gen_spark_context('LogProcessor', local=False)

## Configure Input
access_log_rdd = sc.textFile(ACCESS_LOG_INPUT)
evil_ip_rdd = sc.textFile(EVIL_IP_INPUT)

## Call the pipeline
pipeline = pipeline.LogProcessorPipeline(sc, spark)
(stat_df, evil_ip_report_df) = pipeline.build_pipeline(access_log_rdd, evil_ip_rdd)

## Configure Output
stat_df.write \
    .format('jdbc') \
    .option('url', 'jdbc:mysql://localhost/spark_test') \
    .option('dbtable', 'log_report') \
    .option('user', 'spark') \
    .option('driver', 'com.mysql.jdbc.Driver') \
    .option('password', 'spark123') \
    .option('numPartition', '1') \
    .save()
    
evil_ip_report_df.write \
    .format('json') \
    .mode('overwrite') \
    .save(EVIL_REPORT_OUTPUT_FOLDER)

## Cleanup
sc.stop()
