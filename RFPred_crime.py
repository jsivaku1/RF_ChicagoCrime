from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
from dateutil.parser import parse
from pyspark.sql.functions import lit, year, dayofmonth, month
import calendar
from time import strftime
from datetime import datetime
from pyspark.sql.functions import udf, count
import pandas as pd
from collections import OrderedDict
from math import log, sqrt
import numpy as np

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml.feature import VectorAssembler

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier


from pyspark.ml.evaluation import BinaryClassificationEvaluator



from pyspark.mllib import linalg as mllib_linalg
from pyspark.ml import linalg as ml_linalg
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import col

from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import MLUtils


from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.feature import StandardScaler
from pyspark.ml.linalg import Vector as MLVector, Vectors as MLVectors
from pyspark.mllib.linalg import Vector as MLLibVector, Vectors as MLLibVectors
from pyspark.mllib.regression import  LabeledPoint


def as_mllib(v):
    if isinstance(v, ml_linalg.SparseVector):
        return MLLibVectors.sparse(v.size, v.indices, v.values)
    elif isinstance(v, ml_linalg.DenseVector):
        return MLLibVectors.dense(v.toArray())
    else:
        raise TypeError("Unsupported type: {0}".format(type(v)))


sc = SparkContext()
sqlContext = SQLContext(sc)





date=sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/Users/jayanthsivakumar/Desktop/spark-2.1.0-bin-hadoop2.7/samplecrime.csv')
data = sqlContext.read.load('/Users/jayanthsivakumar/Desktop/spark-2.1.0-bin-hadoop2.7/samplecrime.csv',format='com.databricks.spark.csv', header='true', inferSchema='true')
#data.printSchema()
data = data.fillna(0)
data = data.rdd
parse_time = udf(lambda time:dt.strptime(time, '%m/%d/%Y %I:%M:%S %p'))


crime_data = data.map(lambda d:(int(d[0]),d[1],parse(d[2]),d[3],d[4],d[5],d[6],d[7],str(d[8]),str(d[9]),int(d[10]),int(d[11]),int(d[12]),
int(d[13]),d[14],int(d[15]),
int(d[16]),int(d[17]),parse(d[18]),float(d[19]),float(d[20]),d[21]))





crime_df = sqlContext.createDataFrame(crime_data,["ID","Case Number","Date","Block","IUCR","Primary Type","Description","Location Description","Arrest","Domestic","Beat","District","Ward","Community Area","FBI Code","X Coordinate","Y Coordinate","Year","Updated On","Latitude","Longitude","Location"])

#crime_df.printSchema()

#print crime_df.show()

crime_detail = crime_df.select(crime_df.ID,crime_df["Case Number"],year(crime_df.Date).alias("Year"),month(crime_df.Date).alias("Month"),dayofmonth(crime_df.Date).alias("Dayofmonth"),hour(crime_df.Date).alias("Hour"),crime_df.Block,crime_df["Primary Type"],crime_df.Description,crime_df["Location Description"], crime_df.Arrest,crime_df.Domestic,crime_df.Latitude,crime_df.Longitude,crime_df.Location)

crime_pred = crime_detail.select(crime_detail.Hour,
                                crime_detail["Primary Type"].alias("crime_type"),
                                crime_detail.Domestic,
                                crime_detail["Location Description"].alias("loc_desc"),
                                crime_detail.Arrest)


#crime_pred.printSchema()
arrest_indexer = StringIndexer(inputCol = 'Arrest', outputCol = 'Arrest_indexed')
model = arrest_indexer.fit(crime_pred)
encoded_arrest = model.transform(crime_pred)

# arrest_encoder = OneHotEncoder(inputCol="Arrest_indexed", outputCol="ArrestVec")
# encoded_arrest = arrest_encoder.transform(encoded_arrest)
# # encoded.show()


crime_indexer = StringIndexer(inputCol = 'crime_type', outputCol = 'crime_type_indexed')
model = crime_indexer.fit(encoded_arrest)
encoded_crimeType = model.transform(encoded_arrest)

crimeType_encoder = OneHotEncoder(inputCol="crime_type_indexed", outputCol="crimeTypeVec")
encoded_crimeType = crimeType_encoder.transform(encoded_crimeType)
# encoded.show()
encoded_crimeType = encoded_crimeType.na.drop()

locdesc_indexer = StringIndexer(inputCol = 'loc_desc', outputCol = 'loc_desc_indexed')
model = locdesc_indexer.fit(encoded_crimeType)
encoded_locdesc = model.transform(encoded_crimeType)

locDesc_encoder = OneHotEncoder(inputCol="loc_desc_indexed", outputCol="locDescVec")
encoded_locDesc = locDesc_encoder.transform(encoded_locdesc)
# encoded.show()

dom_indexer  = StringIndexer(inputCol = 'Domestic', outputCol = 'dom_indexed')
model = dom_indexer.fit(encoded_locDesc)
encoded_domes = model.transform(encoded_locDesc)

domes_encoder = OneHotEncoder(inputCol="dom_indexed", outputCol="domesVec")
encoded_domes = domes_encoder.transform(encoded_domes)
# encoded.show()

assembler = VectorAssembler(inputCols = ['crimeTypeVec','locDescVec','domesVec','Hour'],outputCol = 'features')



transformed = assembler.transform(encoded_domes)

print transformed.show()


parsedData = transformed.select('Arrest_indexed','features').rdd.map(lambda row:LabeledPoint(row.Arrest_indexed,[row.features]))


'''

features = transformed.rdd.map(lambda row: row[6:])
standardizer = StandardScaler()
model = standardizer.fit(features)
features_transform = model.transform(features)

label = transformed.rdd.map(lambda row: row[5])
transformedData = label.zip(features_transform)
transformedData = transformedData.map(lambda row: LabeledPoint(row[0],[row[1]]))
'''
(trainingData, testData) = parsedData.randomSplit([0.7, 0.3])


model = RandomForest.trainClassifier(trainingData, numClasses=100, categoricalFeaturesInfo={},
                                     numTrees=3, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=10, maxBins=32)

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
print('Test Error = ' + str(testErr))
#print('Learned classification forest model:')
#print(model.toDebugString())

#print testData.collect()
#print labelsAndPredictions.collect()
