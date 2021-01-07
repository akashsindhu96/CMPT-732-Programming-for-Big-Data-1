import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def main(inputs, model_file):
    tmax_schema = 'station STRING, date TIMESTAMP, latitude FLOAT, longitude FLOAT, elevation FLOAT, tmax FLOAT'
    df = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = df.randomSplit([0.75, 0.25], seed=40)
    train = train.cache()
    validation = validation.cache()
    yesterday_tmax_tranformer = SQLTransformer(
        statement="""SELECT today.station, today.latitude, today.longitude, today.elevation, DAYOFYEAR(today.date) AS day_of_year, today.tmax, yesterday.tmax AS yesterday_tmax 
        FROM __THIS__ as today 
        INNER JOIN __THIS__ as yesterday 
        ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station""")

    vector_assembler = VectorAssembler(inputCols=['day_of_year', 'latitude', 'longitude', 'elevation', 'yesterday_tmax'], outputCol='features')

    rf = RandomForestRegressor(numTrees=20, featuresCol='features', labelCol='tmax', seed=40, maxDepth=3, maxBins=32)
    pipeline = Pipeline(stages=[yesterday_tmax_tranformer, vector_assembler, rf])
    model = pipeline.fit(train)
    predictions = model.transform(validation)
    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol='tmax', metricName='rmse')
    score = evaluator.evaluate(predictions)
    print("Validation RMSE score is: {}".format(score))

    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol='tmax', metricName='r2')
    score = evaluator.evaluate(predictions)
    print("Validation R-SQUARE score is: {}".format(score))

    model.write().overwrite().save(model_file)


if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(inputs, model_file)