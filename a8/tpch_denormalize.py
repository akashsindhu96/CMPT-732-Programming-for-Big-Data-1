# Name: Akash Sindhu
from pyspark.sql import SparkSession, functions
import sys, re, math, uuid, os
from timeit import default_timer as timer
from cassandra.cluster import Cluster


def main(input_keyspace, output_keyspace):
    session = Cluster(cluster_seeds).connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 2}" % output_keyspace)
    session.set_keyspace(output_keyspace)
    session.execute("CREATE TABLE IF NOT EXISTS orders_parts ("
                    "orderkey int, "
                    "custkey int, "
                    "orderstatus text, "
                    "totalprice decimal, "
                    "orderdate date, "
                    "order_priority text, "
                    "clerk text, "
                    "ship_priority int, "
                    "comment text, "
                    "part_names set<text>, "
                    "PRIMARY KEY (orderkey))")
    session.execute('TRUNCATE orders_parts')
    session.shutdown()

    df_part = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=input_keyspace).load()
    df_orders = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=input_keyspace).load().cache()
    df_lineitem = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=input_keyspace).load()
    joined_df = df_orders.join(df_lineitem, df_orders.orderkey == df_lineitem.orderkey)
    final_joined_df = joined_df.join(df_part, df_part.partkey == joined_df.partkey)
    selected_df = final_joined_df.groupBy(df_orders['orderkey'], 'totalprice').agg(functions.collect_set(df_part['name']).alias('part_names')).drop('totalprice')
    output_df = selected_df.join(df_orders, "orderkey")
    output_df.write.format("org.apache.spark.sql.cassandra").options(table="orders_parts", keyspace=output_keyspace).save()


if __name__=="__main__":
    input_keyspace = sys.argv[1]
    output_keyspace = sys.argv[2]
    cluster_seeds = ['199.60.17.103']

    spark = SparkSession.builder.appName("tpch denormalize").config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '2.4'
    sc = spark.sparkContext
    st = timer()
    main(input_keyspace, output_keyspace)
    print("Execution time: {}".format(timer() - st))