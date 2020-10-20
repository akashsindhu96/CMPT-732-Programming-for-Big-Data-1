# Akash Sindhu
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, DataFrameReader


def main(inputs, output):
    # schema = "'station' STRING, 'date' STRING, 'observation' STRING, 'value' INT, 'mflag' STRING, " \
    #          "'qflag' STRING, 'sflag' STRING, 'obstime' STRING"
    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])
    df = spark.read.csv(path=inputs, schema=observation_schema)

    correct_data = df.filter(df['qflag'].isNull())
    canadian_data = correct_data.filter(correct_data['station'].startswith('CA'))
    max_temp = canadian_data.filter(canadian_data['observation'] == 'TMAX')
    outdata = max_temp.select('station', 'date', (max_temp['value'] / 10).alias('tmax'))
    # print(outdata.toJSON().take(10))
    outdata.write.json(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)