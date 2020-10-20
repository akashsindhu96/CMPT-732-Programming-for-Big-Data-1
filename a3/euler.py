#Name: Akash Sindhu

from pyspark import SparkConf, SparkContext
import sys, operator
import random, timeit
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def eular(samples):
    random.seed()
    total_iterations = 0
    for ele in samples:
        sum = 0.0
        while sum < 1:
            sum = sum + random.random()
            total_iterations = total_iterations + 1
    return total_iterations


def main(num_samples):
    samples = sc.range((num_samples), numSlices=45).glom()
    eularrdd = samples.map(eular)
    #print("No. of Partitions: {}".format(eularrdd.getNumPartitions()))
    total_iterations = eularrdd.reduce(operator.add)
    #print("No. of Partitions: {}".format(total_iterations))
    eular_constant = total_iterations/num_samples
    print("Eular Constant: {}".format(eular_constant))


if __name__=='__main__':
    conf = SparkConf().setAppName('word count improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    num_samples = sys.argv[1]
    #starttime = timeit.default_timer()
    # output = sys.argv[2]
    main(int(num_samples))
    #endtime = timeit.default_timer()
    #print("Time difference is: {}".format(endtime - starttime))