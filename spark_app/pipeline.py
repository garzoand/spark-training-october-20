import time
import datetime
from pyspark.sql.types import *

## HELPER FUNCTIONS (not spark related) 
#######################################

def get_ip(s):
    return s.split(' ')[0]

def get_timestamp(str):
    s = str.find('[')
    l = str.find(']')
    ts_str = str[s + 1:l]
    #return long(ts)
    return ts_str

def get_header(str):
    s = str.find('"')
    l = str[s + 1:].find('"')
    header = str[s + 1:s + l + 1].split(' ')
    method = header[0] if len(header) > 0 else "malformed"
    resource = header[1] if len(header) > 1 else "malformed"
    protocol = header[2] if len(header) > 2 else "malformed"        
    return (method, resource, protocol)
    
def get_error_code(str):
    f = str.split(' ')
    if len(f) < 9:
        return 0
    try:
        code = int(f[8])
    except ValueError:
        code = 0
    return code

# input: raw access log from the RDD
# output: structured daa: (ip, ts, date, hour, method, resource, protocol, response code)
def process_access_log_line(log_line):
    header = get_header(log_line)
    ts_str = get_timestamp(log_line)
    date_str = "1980-01-01"
    hour = "12"
    try:
        td = datetime.datetime.strptime(ts_str, "%d/%b/%Y:%H:%M:%S %z")
        date_str = '{}-{}-{}'.format(td.year, td.month, td.day)   
        hour = td.hour
    except ValueError:
        pass
    return (get_ip(log_line), ts_str, date_str, hour, header[0], header[1], header[2], get_error_code(log_line))

def process_ip_list(ip_line):
    # This should be more error prone, sorry about that!    
    return (ip_line.split(':')[1].replace(' ', ''),)


class LogProcessorPipeline:

    def __init__(self, sc, spark):
        self.sc = sc
        self.spark = spark
    

    def build_pipeline(self, access_log_rdd, evil_ip_list_rdd):
        # Step 1: Create the (raw) access log DF
        df = self.build_access_log_df(access_log_rdd)
        # Step 2: create statistics dataframe
        stat_df = self.build_stat_df(df)
        # Step 3: create the report on malicious activity
        evil_ip_report_df = self.build_evil_ip_report_df(df, evil_ip_list_rdd)
        return (stat_df, evil_ip_report_df)


    def build_access_log_df(self, access_log_rdd):
        self.access_log_schema = StructType([
            StructField('ip', StringType(), True),
            StructField('ts', StringType(), True),
            StructField('date', StringType(), True),
            StructField('hour', IntegerType(), True),
            StructField('method', StringType(), True),
            StructField('resource', StringType(), True),
            StructField('protocol', StringType(), True),
            StructField('response', IntegerType(), True)
        ])

        df = access_log_rdd \
            .filter(lambda log_line: len(log_line) > 1) \
            .map(lambda log_line: process_access_log_line(log_line)) \
            .toDF(self.access_log_schema)
        
        return df

    def build_stat_df(self, access_log_df):
        access_log_df.createOrReplaceTempView('access_log_temp')
        stat_df = self.spark.sql("""
        SELECT date, hour, method, resource, response, count(1) as access_count
        FROM access_log_temp
        GROUP BY date, hour, method, resource, response
        """)
        return stat_df

    def build_evil_ip_report_df(self, access_log_df, ip_rdd):
        ip_df = ip_rdd.map(process_ip_list).toDF(['evil_ip'])
        joined_df = access_log_df.join(ip_df.hint('broadcast'), access_log_df.ip == ip_df.evil_ip, 'inner')
        joined_df.createOrReplaceTempView('evil_ip_view')
        malicious_activity_df = self.spark.sql("""
        SELECT evil_ip, count(1) as num_requests
        FROM evil_ip_view
        GROUP BY evil_ip
        ORDER BY num_requests DESC
        """)
        return malicious_activity_df
        
    

