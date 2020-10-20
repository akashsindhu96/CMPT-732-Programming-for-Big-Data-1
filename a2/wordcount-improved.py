#Name: Akash Sindhu

from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count improved')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

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

text = sc.textFile(inputs)
words = text.flatMap(words_once)
words_new_rdd = words.filter(remove_empty)
print("10 words: {}".format(words_new_rdd.take(10)))
wordcount = words_new_rdd.reduceByKey(add)
print("wordcount: {}".format(wordcount.take(10)))

outdata = wordcount.sortBy(get_key).map(output_format)
print("output: {}".format(outdata.take(10)))
outdata.coalesce(1).saveAsTextFile(output)