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
    # session.execute("TRUNCATE "+table_name)
    session.shutdown()

    df.write.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace_name).save()


if __name__=="__main__":
    input_dir = sys.argv[1]
    keyspace_name = sys.argv[2]
    table_name = sys.argv[3]
    cluster_seeds = ['199.60.17.103']

    spark = SparkSession.builder.appName("Spark Cassandra example").config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '2.4'
    sc = spark.sparkContext
    st = timer()
    main(input_dir, keyspace_name, table_name)
    print("Execution time: {}".format(timer() - st))