# Name: Akash Sindhu
from pyspark.sql import SparkSession
import sys
from timeit import default_timer as timer


def source_dest(line):
    pairs = line.split(':')
    source = pairs[0]
    dest = pairs[1].split(" ")
    for value in dest:
        if value != "":
            # print(int(source), int(value))
            yield (int(source), int(value))


def main(input, output, source, destination):
    line = sc.textFile(input + '/links-simple-sorted.txt')
    node_dest_pair = line.flatMap(source_dest)
    schema = "source INT, node INT"
    graph_df = spark.createDataFrame(node_dest_pair, schema=schema).cache()
    # print(graph_df.show(10))
    initial_df = spark.createDataFrame([(source, 0, 0)], ['node', 'source', 'distance'])
    # print(initial_df.show())

    for i in range(6):
        known_df = initial_df.join(graph_df).where(graph_df['source'] == initial_df['node'])
        # print(known_df.show())
        update_df = known_df.select(graph_df['node'], graph_df['source'], initial_df['distance'])
        # print(update_df.show())
        add_df = update_df.withColumn('distance', update_df['distance'] + 1)
        # print(add_df.show())
        sub_df = add_df.subtract(initial_df)
        # print (sub_df.show())
        initial_df = initial_df.unionAll(sub_df).cache()
        # print(initial_df.show())
        initial_df.write.csv(output + '/iter-' + str(i), mode='overwrite')
        if initial_df.where(initial_df['node'] == destination).collect():
            break

    path = [destination]
    # distance = []
    pth = destination
    while(pth != source):
        row = initial_df.where(initial_df['node'] == pth).collect()
        pth = row[0][1]
        # dist = row[0][2]
        path.append(pth)
        # distance.append(dist)
    path_rdd = sc.parallelize(path[::-1])
    path_rdd.saveAsTextFile(output + '/path')
    print("path: {}".format(path))


if __name__=="__main__":
    spark = SparkSession.builder.appName("correlate logs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '2.4'
    sc = spark.sparkContext
    input = sys.argv[1]
    output = sys.argv[2]
    source = int(sys.argv[3])
    destination = int(sys.argv[4])
    st = timer()
    main(input, output, source, destination)
    print("Execution time: {}".format(timer() - st))