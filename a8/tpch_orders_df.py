# Name: Akash Sindhu
from pyspark.sql import SparkSession, functions
import sys, re, math, uuid, os
from timeit import default_timer as timer
from cassandra.cluster import Cluster


def output_line(row):
    orderkey, price, names = row[0], row[1], row[2]
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)


def main(keyspace_name, outdir, orderkeys):
    session = Cluster(cluster_seeds).connect()
    session.set_keyspace(keyspace_name)
    session.shutdown()

    df_part = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=keyspace_name).load()
    df_orders = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=keyspace_name).load()
    df_lineitem = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=keyspace_name).load()
    joined_df = df_orders.join(df_lineitem, df_orders.orderkey == df_lineitem.orderkey)
    final_joined_df = joined_df.join(df_part, df_part.partkey == joined_df.partkey)
    filter_df = final_joined_df.filter(df_orders.orderkey.isin(orderkeys))
    selected_df = filter_df.groupBy(df_orders['orderkey'], 'totalprice').agg(functions.collect_set('name').alias('names')).sort('orderkey')
    out_df = selected_df.rdd.map(output_line)
    out_df.saveAsTextFile(outdir)


if __name__=="__main__":
    keyspace_name = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]

    cluster_seeds = ['199.60.17.103']

    spark = SparkSession.builder.appName("tpch orders df").config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '2.4'
    sc = spark.sparkContext
    st = timer()
    main(keyspace_name, outdir, orderkeys)
    print("Execution time: {}".format(timer() - st))