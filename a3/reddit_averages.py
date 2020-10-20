#Name: Akash Sindhu

from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def formkeyvaluepairs(json_sting):
    key_value = json.loads(json_sting)
    key = key_value["subreddit"]
    score = key_value["score"]
    count = 1
    return ([key, ([count, score])])

def sum_pairs(pair1, pair2):
    return (pair1[0] + pair2[0], pair1[1] + pair2[1])

def average(keyvalue):
    key, pair = keyvalue[0], keyvalue[1]
    count, score = pair[0], pair[1]
    average_score = score/count
    return ([key, average_score])


def main(inputs, output):
    # main logic starts here
    jsonfile = sc.textFile(inputs)
    key_value_pairs = jsonfile.map(formkeyvaluepairs)
    key_value_summed = key_value_pairs.reduceByKey(sum_pairs)
    key_avg_score = key_value_summed.map(average)
    json_output = key_avg_score.map(json.dumps)
    json_output.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)