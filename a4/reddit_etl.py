# Akash Sindhu
from pyspark import SparkConf, SparkContext
import sys, json, timeit
assert sys.version_info >= (3,5) # make sure python is 3.5 +


def extract_fields(json_object):
    """
    returns the subreddit, score and author
    """
    subreddit = json_object['subreddit']
    score = json_object['score']
    author = json_object['author']
    return subreddit, score, author

def more_than_0(sca):
    """
    Check if e is in subreddit and score is more than 0
    returns: tuple
    """
    s,c,a = sca
    if 'e' in s and c > 0:
        return (s, c, a)

def less_than_0(sca):
    """
    Check if e is in subreddit and score is smaller than 0
    returns: tuple
    """
    s,c,a = sca
    if 'e' in s and c <= 0:
        return (s,c,a)


def main(inputs, output):
    text = sc.textFile(inputs)
    json_object = text.map(json.loads)
    extracted_fields_rdd = json_object.map(extract_fields).cache()

    more_than_rdd = extracted_fields_rdd.filter(more_than_0)
    less_than_rdd = extracted_fields_rdd.filter(less_than_0)

    more_than_rdd.map(json.dumps).saveAsTextFile(output + '/positive')
    less_than_rdd.map(json.dumps).saveAsTextFile(output + '/negative')


if __name__=='__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    assert sc.version >= '2.3' # spark version 2.3 +
    inputs = sys.argv[1]
    output = sys.argv[2]
    starttime = timeit.default_timer()
    main(inputs, output)
    endtime = timeit.default_timer()
    print("total time: {}".format(endtime - starttime))