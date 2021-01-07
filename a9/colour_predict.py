import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25], seed=40)
    train = train.cache()
    validation = validation.cache()

    # TODO: create a pipeline to predict RGB colours -> word
    rgb_assembler = VectorAssembler(inputCols=['R', 'G', 'B'], outputCol='features')
    word_indexer = StringIndexer(inputCol='word', outputCol='label')
    classifier = MultilayerPerceptronClassifier(featuresCol='features', labelCol='label', layers=[3, 40, 11], seed=40)
    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)

    # TODO: create an evaluator and score the validation data
    predictions = rgb_model.transform(validation)
    evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='accuracy')
    score = evaluator.evaluate(predictions)
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model: {}'.format(score))

    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])

    # TODO: create a pipeline RGB colours -> LAB colours -> word; train and evaluate.
    sql_trans = SQLTransformer(statement=rgb_to_lab_query)
    lab_assembler = VectorAssembler(inputCols=['labL','labA','labB'], outputCol='features')
    lab_word_indexer = StringIndexer(inputCol='word', outputCol='label')
    lab_classifier = MultilayerPerceptronClassifier(featuresCol='features', labelCol='label', layers=[3, 40, 11], seed=40)
    lab_pipeline = Pipeline(stages=[sql_trans, lab_assembler, lab_word_indexer, lab_classifier])
    lab_model = lab_pipeline.fit(train)

    # evaluate and score
    lab_predictions = lab_model.transform(validation)
    lab_evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='accuracy')
    lab_score = lab_evaluator.evaluate(lab_predictions)
    plot_predictions(lab_model, "LAB", labelCol='word')
    print('Validation score of LAB model: {}'.format(lab_score))


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
