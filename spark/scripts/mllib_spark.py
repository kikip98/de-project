print('hello world')


# Get a spark context
import pyspark
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import when,split,col,udf,from_unixtime, unix_timestamp, regexp_replace
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql.types import IntegerType
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql.functions import *
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF
from pyspark.ml import Pipeline, PipelineModel
import os
import boto3

#Building Spark ssession
spark = SparkSession   .builder     .appName("PySpark App")     .config("spark.jars", "/home/master/jar/postgresql-42.3.2.jar")     .getOrCreate()



import psycopg2
import pandas as pd

# Connect to DB
engine = psycopg2.connect(
    database="kikipng",
    user="postgres",
    password="qwerty123",
    host="kikipng.ckoss6hrgu4d.eu-west-2.rds.amazonaws.com",
    port=5432)

query = """select * from airline_postgres_schema.skytrax_reviews"""
df_skytrax  = pd.read_sql(query, engine)
dfs_skytrax = spark.createDataFrame(df_skytrax)


dfs_skytrax.printSchema()


# Exploration



dfs_skytrax.show(5)


# Check if the taget column is balanced
dfs_skytrax.groupBy('recommendation').count().show()



# Feature Engineering


def get_one_hot_encodings(dfs, column_name):   
    '''
    Input: spark dataframe and name of column we want to one-hot-encode
    Output: spark dataframe with one-hot-encoding on the column requested
    '''
    unique_values = dfs.select(column_name)                        .distinct()                        .rdd                        .flatMap(lambda x: x).collect()

    # for each of the gathered values create a new column 
    for unique_value in unique_values:
        function = udf(lambda item: 
                       1 if item == unique_value else 0, 
                       IntegerType())
        new_column_name = column_name + '_' + unique_value.lower().replace(' ','_')
        dfs = dfs.withColumn(new_column_name, function(col(column_name)))
    dfs = dfs.drop(column_name)
    return dfs



# One-Hot-Encode cabin_flown, traveller_type and aircraft
column_names = ['cabin_flown','traveller_type', 'aircraft']
for column_name in column_names:
    dfs_skytrax = get_one_hot_encodings(dfs_skytrax,column_name )




# Replace in the recommendation column 'no' with 0 and 'yes' with 1
dfs_skytrax = dfs_skytrax.withColumn('recommendation', regexp_replace('recommendation', 'no', '0'))
dfs_skytrax = dfs_skytrax.withColumn('recommendation', regexp_replace('recommendation', 'yes', '1'))




# Model

# Now that all categorical columns have been converted to numeric assemble all numeric columns
numericCols = [item[0] for item in dfs_skytrax.dtypes if item[1]== 'bigint' or item[1]== 'int' or item[1]== 'double' or item[1]== 'float' ]#[1:]
assembler = VectorAssembler(inputCols=numericCols, outputCol="features")
label_stringIdx = StringIndexer(inputCol = 'recommendation', outputCol = 'labelIndex')
pipeline = Pipeline(stages=[assembler, label_stringIdx])
preprocessing_pipeline_model = pipeline.fit(dfs_skytrax)
dfs_skytrax = preprocessing_pipeline_model.transform(dfs_skytrax)

train, test = dfs_skytrax.randomSplit([0.7, 0.3], seed = 2018)
print("Records for training dataset: " + str(train.count()))
print("Records for test dataset: " + str(test.count()))


rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'labelIndex')
rfModel = rf.fit(train)


def uploadDirectory(path):
    session = boto3.Session(
        aws_access_key_id='AKIA2X2ER6BK2I4N3NO4',
        aws_secret_access_key='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'
    )

    s3C = session.resource('s3').Bucket('airline-project-kikipng')
    bucketname = 'airline-project-kikipng'
    for root,dirs,files in os.walk(path):
        for file in files:
            print(file)
            s3C.upload_file(os.path.join(root,file),os.path.join(root,file))


rfModel.write().overwrite().save('model')
uploadDirectory('model')


predictions = rfModel.transform(test)
predictions.select('labelIndex', 'rawPrediction', 'prediction', 'probability').show(10)



from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %s" % (accuracy))
print("Test Error = %s" % (1.0 - accuracy))



from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F

preds_and_labels = predictions.select(['prediction','labelIndex']).withColumn('labelIndex', F.col('labelIndex').cast(FloatType())).orderBy('prediction')
preds_and_labels = preds_and_labels.select(['prediction','labelIndex'])
metrics = MulticlassMetrics(preds_and_labels.rdd.map(tuple))
print(metrics.confusionMatrix().toArray())

