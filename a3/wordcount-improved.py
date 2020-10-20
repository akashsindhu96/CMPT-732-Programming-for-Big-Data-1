#Name: Akash Sindhu

from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    result = wordsep.split(line)
    # print(result)
    for w in result:
        # print(w.lower())
        yield (w.lower(), 1)

def remove_empty(word):
    if word[0] != "":
        return word

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)


def main(inputs, output):

    text = sc.textFile(inputs).repartition(32)
    #print("No. of Partitions0: {}".format(text.getNumPartitions()))
    words = text.flatMap(words_once)
    words_new_rdd = words.filter(remove_empty)
    #print("No. of Partitions1: {}".format(words_new_rdd.getNumPartitions()))
    wordcount = words_new_rdd.reduceByKey(add)
    #print("No. of partitions2: {}".format(wordcount.getNumPartitions()))

    outdata = wordcount.sortBy(get_key).map(output_format)
    #print("No. of Partitions3: {}".format(outdata.getNumPartitions()))
    outdata.saveAsTextFile(output)


if __name__=='__main__':
    conf = SparkConf().setAppName('word count improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
