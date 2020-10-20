#Name: Akash Sindhu
from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def formkeyvaluepairs(key_value):
    """
    extracts the subreddit and score
    returns: tuple key value pair with count
    """
    key = key_value["subreddit"]
    score = key_value["score"]
    count = 1
    return (key, (count, score))

def sum_pairs(pair1, pair2):
    """
    Adds the two values of two different keys. Used in reducebykey()
    returns: tuple of sum
    """
    return (pair1[0] + pair2[0], pair1[1] + pair2[1])

def average(keyvalue):
    """
    counts the average score
    returns: tuple of key and average score
    """
    key, pair = keyvalue[0], keyvalue[1]
    count, score = pair[0], pair[1]
    average_score = score/count
    return (key, average_score)

def ignore(kv):
    """
    returns only the values > 0
    """
    key, value = kv[0], kv[1]
    if value > 0:
        return (key, value)

def avg(ele):
    """
    returns tuple of relative score and author
    """
    subreddit = ele[0]
    score = ele[1][0][0]
    author = ele[1][0][1]
    average = ele[1][1]
    relative_score = score/average
    return (relative_score, author)

def main(inputs, output):

    jsonfile = sc.textFile(inputs)
    json_load = jsonfile.map(json.loads).cache()

    key_value_pairs = json_load.map(formkeyvaluepairs)
    key_value_summed = key_value_pairs.reduceByKey(sum_pairs)
    key_avg_score = key_value_summed.map(average)
    positive_average_rdd = key_avg_score.filter(ignore)

    commentbysub = json_load.map(lambda c:(c["subreddit"], (c['score'], c["author"]))).join(positive_average_rdd)
    outdata = commentbysub.map(avg).sortBy(lambda a: a[0], ascending=False)

    json_output = outdata.map(json.dumps)
    json_output.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)