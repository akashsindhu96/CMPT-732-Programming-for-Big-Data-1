# Akash Sindhu
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from timeit import default_timer as timer


def main(inputs, output):
    schema = "station STRING, date STRING, observation STRING, value INT, mflag STRING, " \
             "qflag STRING, sflag STRING, obstime STRING"

    df = spark.read.csv(path=inputs, schema=schema)
    df.createOrReplaceTempView("dataframe")
    df1 = spark.sql("SELECT * FROM dataframe WHERE qflag IS null AND observation IN ('TMAX', 'TMIN')")
    df1.createOrReplaceTempView("dataframe_1")
    # print(df1.show())
    df2 = spark.sql("SELECT date, station, MAX(value) AS max_value, MIN(value) AS min_value FROM dataframe_1 GROUP BY date, station")
    df2.createOrReplaceTempView("dataframe_2")
    # print(df2.show())
    df3 = spark.sql("SELECT date, station, max_value - min_value AS range FROM dataframe_2").cache()
    df3.createOrReplaceTempView("dataframe_3")
    # print(df3.show())
    df4 = spark.sql("SELECT date, MAX(range) AS max_range FROM dataframe_3 GROUP BY date")
    df4.createOrReplaceTempView("dataframe_4")
    # print(df4.show())
    df5 = spark.sql("SELECT dataframe_3.date, dataframe_3.station, dataframe_3.range FROM dataframe_3 JOIN dataframe_4 ON dataframe_3.range == dataframe_4.max_range AND dataframe_3.date == dataframe_4.date")
    # print(df5.show())
    df5.createOrReplaceTempView("dataframe_5")
    df6 = spark.sql("SELECT date, station, range / 10 AS range FROM dataframe_5")
    # print(df6.show())
    df6.createOrReplaceTempView("dataframe_6")
    df7 = spark.sql("SELECT date, station, range FROM dataframe_6 ORDER BY date, station")
    # df7.show()
    df7.write.csv(output, mode='overwrite')


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