# Name: Akash Sindhu
from pyspark.sql import SparkSession, functions
import sys, re, math
from timeit import default_timer as timer


def preprocess(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    cleaned = line_re.split(line)

    if len(cleaned) == 6:
        sel_tag = (cleaned[1], int(cleaned[4]))
        return sel_tag


def main(inputs):
    rdd = sc.textFile(inputs)
    preprocess_rdd = rdd.map(preprocess)
    cleaned_rdd = preprocess_rdd.filter(lambda x: x is not None)

    schema = "hostname STRING, bytes INT"
    # sch = ['hostname', 'bytes']

    df = spark.createDataFrame(data=cleaned_rdd, schema=schema)
    groupby_hostname_df = df.groupBy("hostname").agg(functions.count("bytes").alias("x"), functions.sum("bytes").alias("y"))
    six_value_df = groupby_hostname_df.withColumn('one', functions.lit(1)).withColumn('x2', groupby_hostname_df['x']*groupby_hostname_df['x'])\
        .withColumn('y2', groupby_hostname_df['y']*groupby_hostname_df['y']).withColumn('xy', (groupby_hostname_df['x']*groupby_hostname_df['y']))
    sum_df = six_value_df.groupBy().sum().collect()
    numerator = sum_df[0][2] * sum_df[0][5] - sum_df[0][0] * sum_df[0][1]
    denominator = (math.sqrt((sum_df[0][2] * sum_df[0][3]) - (sum_df[0][0] ** 2))) * (math.sqrt((sum_df[0][2] * sum_df[0][4]) - (sum_df[0][1] ** 2)))

    r = numerator/denominator
    r_sqr = math.pow(r, 2)
    print("r = {}".format(r))
    print("r^2 = {}".format(r_sqr))



if __name__=="__main__":
    spark = SparkSession.builder.appName("correlate logs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '2.4'
    sc = spark.sparkContext
    inputs = sys.argv[1]
    st = timer()
    main(inputs)
    print("Execution time: {}".format(timer() - st))