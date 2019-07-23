from __future__ import print_function

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from pyspark.sql import SparkSession
import os
from pyspark import SparkContext

os.environ["SPARK_HOME"] = '/home/apandit7'

sc = SparkContext.getOrCreate()

data = spark.read.format("csv").option('header','true').option('inferSchema','true').load('diabetic_data.csv')

spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

assembler = VectorAssembler(inputCols =['admission_type_id','discharge_disposition_id','admission_source_id','time_in_hospital','num_procedures','num_medications','number_emergency','number_diagnoses'], outputCol = 'features')

final_df = assembler.transform(data)

final_df.show(3)

from pyspark.ml.feature import StandardScaler

data_feature=data.select('admission_type_id','discharge_disposition_id','admission_source_id','time_in_hospital','num_procedures','num_medications','number_emergency','number_diagnoses')

scaler = StandardScaler(inputCol = 'features', outputCol = 'scaledFeatures')

scaler_model = scaler.fit(final_df)

final_df = scaler_model.transform(final_df)

final_df.show(3)

kmeans = KMeans(featuresCol = 'scaledFeatures', k=3)

model = kmeans.fit(final_df)

print('WSSSE:', model.computeCost(final_df))

model.transform(final_df).select('scaledFeatures', 'prediction').show(10,100)


autoData = spark.read.csv("auto.csv",header='true',inferSchema='true')

from sklearn import preprocessing

le = preprocessing.LabelEncoder()

import pandas as pd
le = preprocessing.LabelEncoder()
autopd=autoData.toPandas()
autopd['fuel-system']=le.fit_transform(autopd['fuel-system'])
autopd['engine-type']=le.fit_transform(autopd['engine-type'])
autoData=spark.createDataFrame(autopd)
autoData.show()

autodata1=autoData.select('wheel-base','length','width','height','curb-weight','engine-size','fuel-system','engine-type','symboling')
autodata1.show()

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols = ['wheel-base','length','width','height','curb-weight','engine-size','fuel-system','engine-type'], outputCol = 'features')
vector=vectorAssembler.transform(autodata1)
vhouse_df = vector.select(['features', 'engine-type'])

splits = vhouse_df.randomSplit([0.8, 0.2])

train_df = splits[0]
test_df = splits[1]

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol = 'features', labelCol='engine-type', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)
train_df.describe().show()

lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction","engine-type","features").show(5)
from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction",labelCol="engine-type",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

from pyspark.ml.regression import DecisionTreeRegressor
dt = DecisionTreeRegressor(featuresCol ='features', labelCol = 'engine-type')
dt_model = dt.fit(train_df)
dt_predictions = dt_model.transform(test_df)
dt_evaluator = RegressionEvaluator(
    labelCol="engine-type", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)


from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

rf = RandomForestClassifier(labelCol="engine-type",featuresCol="features",numTrees = 100,maxDepth = 4,maxBins = 32)
# Train model with Training Data
rfModel = rf.fit(train_df)
predictions = rfModel.transform(test_df)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol="engine-type")
evaluator.evaluate(predictions)

from pyspark.ml.classification import NaiveBayes

nb = NaiveBayes(smoothing=1,labelCol="engine-type",featuresCol="features")

model = nb.fit(train_df)

predictions = model.transform(test_df)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol="engine-type")
evaluator.evaluate(predictions)


