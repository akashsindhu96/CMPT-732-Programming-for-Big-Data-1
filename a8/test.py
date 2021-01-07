from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types,Row
from pyspark.sql.functions import count,sum,lit
from math import sqrt
import datetime
import sys
import re
import operator
import uuid
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def output_line(tup):
    namestr = ', '.join(sorted(list(tup[2])))
    return 'Order #%d $%.2f: %s' % (tup[0], tup[1], namestr)

def main(keyspace, outdir, orderkeys):
    cluster_seeds = ['199.60.17.103']
    spark = SparkSession.builder.appName('tpch orders df').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table="part", keyspace=keyspace).load()
    lineitem_df = spark.read.format("org.apache.spark.sql.cassandra").options(table="lineitem", keyspace=keyspace).load()
    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table="orders", keyspace=keyspace).load()
    join1_df = orders_df.join(lineitem_df, "orderkey")
    join2_df = join1_df.join(part_df, "partkey")
    new_df = join2_df.filter(join1_df.orderkey.isin(orderkeys))
    agg_new_df = new_df.groupBy(new_df['orderkey'],new_df['totalprice']).agg(functions.collect_set(new_df['name']).alias('names')).sort(new_df['orderkey'])
    final_rdd = agg_new_df.rdd.map(output_line)
    final_rdd.saveAsTextFile(outdir)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace, outdir, orderkeys)