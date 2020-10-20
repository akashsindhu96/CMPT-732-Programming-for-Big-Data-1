from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once(line):
    for w in line.split():
        yield (w, 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
words = text.flatMap(words_once)
print("10 words: {}".format(words.take(10)))
wordcount = words.reduceByKey(add)
print("wordcount: {}".format(wordcount.take(10)))

outdata = wordcount.sortBy(get_key).map(output_format)
print("output: {}".format(outdata.take(10)))
outdata.saveAsTextFile(output)