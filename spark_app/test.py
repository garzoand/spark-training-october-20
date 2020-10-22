import pyspark
import pandas as pd
import utils
import pipeline

def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)

def test_access_log_df(pipeline, test_input_rdd):
    df = pipeline.build_access_log_df(test_input_rdd)

    expected_output = [
        ['109.169.248.247', "12/Dec/2015:18:25:11 +0100", '2015-12-12', 18, 'GET', '/administrator/', 'HTTP/1.1', 200],
        ['109.169.248.247', "12/Dec/2015:18:25:11 +0100", '2015-12-12', 18, 'POST', '/administrator/index.php', 'HTTP/1.1', 200],
    ]

    expected_df = pipeline.spark.createDataFrame(expected_output, pipeline.access_log_schema)

    df.show()
    expected_df.show()

    # match df to expected_df
    actual = get_sorted_data_frame(df.toPandas(), ['ip', 'ts', 'method', 'resource', 'protocol', 'response'])
    expected = get_sorted_data_frame(expected_df.toPandas(), ['ip', 'ts', 'method', 'resource', 'protocol', 'response'])
    pd.testing.assert_frame_equal(expected, actual, check_like=True)
    print('#### Unit test 1 passed! ######')    


if __name__ == '__main__':

    (sc, spark) = utils.gen_spark_context('LogProcessor-Test', local=True)
    pipeline = pipeline.LogProcessorPipeline(sc, spark)

    ## Unit Test #1 ##
    test_input_rdd = sc.textFile('sample_data/unit-test.log')
    test_access_log_df(pipeline, test_input_rdd)

    # All test passed
    sc.stop()
