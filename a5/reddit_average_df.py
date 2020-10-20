# Akash Sindhu
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions


def main(inputs, output):
    schema = "archived BOOLEAN, author STRING, body STRING, controversiality INT, created_utc STRING," \
             "downs INT, edited STRING, gilded INT, id STRING, link_id STRING, name STRING, parent_id STRING," \
             "retrieved_on INT, score INT, score_hidden BOOLEAN, subreddit STRING, subreddit_id STRING, " \
             "ups INT, month INT"

    df = spark.read.json(path=inputs, schema=schema)
    df_new = df.select('subreddit', 'score')
    # print(df_new.rdd.getNumPartitions())
    df_average = df_new.groupBy('subreddit').agg(functions.avg('score').alias('average'))
    # print(df_average.explain(extended=True))
    df_average.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)