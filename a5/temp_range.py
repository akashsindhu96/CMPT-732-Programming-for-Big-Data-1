# Akash Sindhu
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
from timeit import default_timer as timer


def main(inputs, output):
    schema = "station STRING, date STRING, observation STRING, value INT, mflag STRING, " \
             "qflag STRING, sflag STRING, obstime STRING"

    df = spark.read.csv(path=inputs, schema=schema)
    correct_data = df.filter(df['qflag'].isNull())
    # print(correct_data.show())
    qflag_df = correct_data.filter((correct_data['observation'] == 'TMAX') | (correct_data['observation'] == 'TMIN'))
    # print(qflag_df.show())
    find_ranges = qflag_df.groupBy(qflag_df['station'], qflag_df['date']).agg(functions.max(qflag_df['value']).alias('max_value'), functions.min(qflag_df['value']).alias('min_value'))
    # print(find_ranges.show())
    range_df = find_ranges.withColumn('range', (find_ranges['max_value'] - find_ranges['min_value'])).cache()
    max_specific_date_df = range_df.groupBy(range_df['date'].alias('date1')).agg(functions.max(range_df['range']).alias('max_range')) # return the max values for a specific date
    max_range_df = range_df.join(max_specific_date_df, (range_df['range'] == max_specific_date_df['max_range']) & (range_df['date'] == max_specific_date_df['date1']))
    # print(max_range_df.show())
    final_range_df = max_range_df.withColumn('range', max_range_df['max_range'] / 10)
    sorted_df = final_range_df.orderBy(final_range_df['date'], final_range_df['station'])
    # print(sorted_df.show())
    output_df = sorted_df.select(sorted_df['date'], sorted_df['station'], sorted_df['range'])
    # print(output_df.show())
    output_df.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    st = timer()
    main(inputs, output)
    print("Execution time: {}".format(timer() - st))