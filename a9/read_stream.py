from kafka import KafkaProducer, KafkaConsumer
import sys
from pyspark.sql import SparkSession, functions
from timeit import default_timer as timer

def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))

    split_col = functions.split(values['value'], ' ')
    x_df = values.withColumn('x', split_col.getItem(0))\
        .withColumn('y', split_col.getItem(1))

    updated_df = x_df.withColumn("1", functions.lit(1))\
        .withColumn('xy', x_df['x'] * x_df['y'])\
        .withColumn("xsquare", x_df['x'] * x_df['x']).drop("value")

    sum_df = updated_df.agg(functions.sum('1').alias('n'), functions.sum("x").alias("sum_x"), functions.sum('y').alias("sum_y"), functions.sum("xy").alias("sum_xy"), functions.sum("xsquare").alias("sum_xsquare"))
    # another way of doing the same thing (this is for reference)
    # updated_df.createOrReplaceTempView("table")
    # sum_val = spark.sql("SELECT SUM(1) as n, SUM(x) as sum_x, SUM(y) as sum_y, SUM(xy) as sum_xy, SUM(xsquare) as sum_xsquare from table")

    beta_numerator = sum_df['sum_xy'] - (sum_df['sum_x'] * sum_df['sum_y'])/sum_df['n']
    beta_denominator = sum_df['sum_xsquare'] - (sum_df['sum_x']**2)/sum_df['n']
    beta = beta_numerator/beta_denominator
    alpha = sum_df['sum_y']/sum_df['n'] - (beta * sum_df['sum_x'])/sum_df['n']
    final_df = sum_df.withColumn('beta', beta)\
        .withColumn('alpha', alpha)\
        .drop("n", 'sum_x', 'sum_y', 'sum_xy', 'sum_xsquare')

    stream = final_df.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(50)



if __name__ == "__main__":
    topic = sys.argv[1]
    spark = SparkSession.builder.appName("read stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '2.4'
    sc = spark.sparkContext
    st = timer()
    main(topic)
    print("Execution time: {}".format(timer() - st))