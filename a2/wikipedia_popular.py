# Name: Akash Sindhu

from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
outputs = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'

def words_once(lines):
    splits = lines.split()
    date = splits[0]
    # print(date)
    language = splits[1]
    title = splits[2]
    popular = splits[3]
    # print(date)

    if language == 'en' and title != "Main_Page" and not title.startswith("Special:"):
        yield (date, (int(popular), title))


def max_val(x, y):

    if x[0] >= y[0]:
        return x
    else:
        return y

def sortt(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

text = sc.textFile(inputs)
tupl = text.flatMap(words_once)
# print(tupl.take(10))
max_count = tupl.reduceByKey(max_val)
sorted_pairs = max_count.sortBy(keyfunc=sortt)
sorted_pairs.map(tab_separated).saveAsTextFile(outputs)