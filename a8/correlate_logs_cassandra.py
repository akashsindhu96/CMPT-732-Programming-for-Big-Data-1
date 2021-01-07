# Name: Akash Sindhu
from pyspark.sql import SparkSession, functions
import sys, re, math, uuid
from timeit import default_timer as timer
from cassandra.cluster import Cluster
from datetime import datetime

def preprocess(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    cleaned = line_re.split(line)

    if len(cleaned) == 6:
        sel_tag = (str(uuid.uuid4()), cleaned[1], datetime.strptime(cleaned[2], '%d/%b/%Y:%H:%M:%S'), cleaned[3], int(cleaned[4]))
        return sel_tag


def main(input_dir, keyspace_name, table_name):
    rdd = sc.textFile(input_dir)
    preprocess_rdd = rdd.map(lambda line: preprocess(line))
    cleaned_rdd = preprocess_rdd.filter(lambda x: x is not None)

    schema = "id STRING, host STRING, datetime TIMESTAMP, path STRING, bytes INT"
    # sch = ['id', 'host', 'datetime', 'path', 'bytes']

    df = spark.createDataFrame(data=cleaned_rdd, schema=schema)
    session = Cluster(cluster_seeds).connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspace_name+" WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2}")
    session.set_keyspace(keyspace_name)
    session.execute("DROP TABLE IF EXISTS "+keyspace_name+"."+table_name)
    session.execute("CREATE TABLE IF NOT EXISTS "+table_name+" (id UUID, host TEXT, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY (host, id))")
    session.shutdown()

    df.write.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace_name).save()
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace_name).load()
    groupby_hostname_df = df.groupBy("host").agg(functions.count("bytes").alias("x"), functions.sum("bytes").alias("y"))
    six_value_df = groupby_hostname_df.withColumn('one', functions.lit(1)).withColumn('x2', groupby_hostname_df['x'] * groupby_hostname_df['x'])\
        .withColumn('y2', groupby_hostname_df['y'] * groupby_hostname_df['y']).withColumn('xy', ( groupby_hostname_df['x'] * groupby_hostname_df['y']))
    sum_df = six_value_df.groupBy().sum().collect()
    numerator = sum_df[0][2] * sum_df[0][5] - sum_df[0][0] * sum_df[0][1]
    denominator = (math.sqrt((sum_df[0][2] * sum_df[0][3]) - (sum_df[0][0] ** 2))) * (
        math.sqrt((sum_df[0][2] * sum_df[0][4]) - (sum_df[0][1] ** 2)))

    r = numerator / denominator
    r_sqr = math.pow(r, 2)
    print("r = {}".format(r))
    print("r^2 = {}".format(r_sqr))




if __name__=="__main__":
    input_dir = sys.argv[1]
    keyspace_name = sys.argv[2]
    table_name = sys.argv[3]
    cluster_seeds = ['199.60.17.103']

    spark = SparkSession.builder.appName("correlate logs cassandra").config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '2.4'
    sc = spark.sparkContext
    st = timer()
    main(input_dir, keyspace_name, table_name)
    print("Execution time: {}".format(timer() - st))