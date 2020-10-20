# Akash Sindhu
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from timeit import default_timer as timer
from pyspark.sql import SparkSession, functions, types, DataFrameReader

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    splitted = path.split("/")
    hour = splitted[-1][11:22]
    return hour


def main(inputs, output):
    schema = "language STRING, title STRING, views INT, bytes INT"

    df_new = spark.read.csv(path=inputs, schema=schema, sep=" ", header=False).withColumn("filename", functions.input_file_name())
    df = df_new.withColumn('hour', path_to_hour((df_new['filename'])))

    filter_df = df.filter((df['language'] == 'en') & (df['title'] != 'Main_Page') & (df['title'].startswith('Special:') == False)).cache()
    # print(df_2.show(10))
    df_1 = filter_df.alias('data_new_1')

    smaller_df = df_1.groupBy(filter_df["hour"]).agg(functions.max(filter_df["views"]).alias('views'))
    # print(data.show(10))
    df_2 = smaller_df.alias('data_new_2')

    joined_df = df_1.join(functions.broadcast(df_2), (functions.col("data_new_1.hour") == functions.col("data_new_2.hour")) & (functions.col("data_new_1.views") == functions.col("data_new_2.views")))
    final_df = joined_df.select("data_new_1.hour", "data_new_1.title", "data_new_2.views")
    output_df = final_df.sort("hour")
    # print(sorted_df.explain())
    output_df.coalesce(1).write.json(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    # spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    start_time = timer()
    # for i in range(5):
    main(inputs, output)
    print("Average Execution Time: {}".format(timer() - start_time))