import pyspark
from pyspark.sql import SparkSession
import pipeline
import utils

ACCESS_LOG_INPUT = 'sample_data/unit-test2.log'
EVIL_IP_INPUT = 'sample_data/ip-list.txt'

EVIL_REPORT_OUTPUT_FOLDER = 'report_output'

## Create and Configure Spark Context and Session
(sc, spark) = utils.gen_spark_context('LogProcessor', local=False)

## Configure Input
access_log_rdd = sc.textFile(ACCESS_LOG_INPUT)
evil_ip_rdd = sc.textFile(EVIL_IP_INPUT)

## Call the pipeline
pipeline = pipeline.LogProcessorPipeline(sc, spark)
(stat_df, evil_ip_report_df) = pipeline.build_pipeline(access_log_rdd, evil_ip_rdd)

## Configure Output
# spark.conf.set('spark.sql.shuffle.partitions', 100)

stat_df.write \
    .format('jdbc') \
    .option('url', 'jdbc:mysql://localhost/spark_test') \
    .option('dbtable', 'log_report') \
    .option('user', 'spark') \
    .option('driver', 'com.mysql.jdbc.Driver') \
    .option('password', 'spark123') \
    .option('numPartition', '1') \
    .save()


# spark.conf.set('spark.sql.shuffle.partitions', 300)

evil_ip_report_df.coalese(1).write \
    .format('json') \
    .mode('overwrite') \
    .save(EVIL_REPORT_OUTPUT_FOLDER)

## Cleanup
sc.stop()
