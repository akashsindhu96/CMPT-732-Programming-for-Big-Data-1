# Name: Akash Sindhu
from pyspark.sql import SparkSession, functions
import sys, re, math, uuid, os
from timeit import default_timer as timer


def output_line(row):
    orderkey, price, names = row[0], row[1], row[2]
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)


def main(keyspace_name, outdir, orderkeys):
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=keyspace_name).load()
    out_df = df.select(df.orderkey, df.totalprice, df.part_names).filter(df.orderkey.isin(orderkeys)).sort(df['orderkey'])
    out = out_df.rdd.map(output_line)
    out.saveAsTextFile(outdir)


if __name__=="__main__":
    keyspace_name = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]

    cluster_seeds = ['199.60.17.103']

    spark = SparkSession.builder.appName("tpch orders denorm").config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '2.4'
    sc = spark.sparkContext
    st = timer()
    main(keyspace_name, outdir, orderkeys)
    print("Execution time: {}".format(timer() - st))