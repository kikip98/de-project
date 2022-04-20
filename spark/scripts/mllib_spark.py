# Get a spark context
import pyspark
import pandas as pd
import numpy as np
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,col,udf, regexp_replace
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql.functions import *
from pyspark.ml import Pipeline, PipelineModel
import os
import boto3
from pyspark.ml.evaluation import MulticlassMetrics, MulticlassClassificationEvaluator
import pyspark.sql.functions as F

#Building Spark ssession
spark = SparkSession   .builder     .appName("PySpark App")     .config("spark.jars", "/home/master/jar/postgresql-42.3.2.jar")     .getOrCreate()


# Connect to DB
engine = psycopg2.connect(
    database="kikipng",
    user="postgres",
    password="qwerty123",
    host="kikipng.ckoss6hrgu4d.eu-west-2.rds.amazonaws.com",
    port=5432)

# Query data
query = """select * from airline_postgres_schema.skytrax_reviews"""
df_skytrax  = pd.read_sql(query, engine)
dfs_skytrax = spark.createDataFrame(df_skytrax)


# Print Schema
dfs_skytrax.printSchema()



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




# Prepare the data for the model
numericCols = [item[0] for item in dfs_skytrax.dtypes if item[1]== 'bigint' or item[1]== 'int' or item[1]== 'double' ]#[1:]
assembler = VectorAssembler(inputCols=numericCols, outputCol="features")
dfs_new = assembler.transform(dfs_skytrax)
label_stringIdx = StringIndexer(inputCol = 'recommendation', outputCol = 'labelIndex')
dfs_new = label_stringIdx.fit(dfs_new).transform(dfs_new)


# Split the data to train and test
train, test = dfs_new.randomSplit([0.7, 0.3], seed = 2018)
print("Records for training dataset: " + str(train.count()))
print("Records for test dataset: " + str(test.count()))


# Fit the train data to the model
rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'labelIndex')
rfModel = rf.fit(train)


# Create a function that saves the data to s3 bukcet
def uploadDirectory(path):
    session = boto3.Session(
        aws_access_key_id='AKIA2X2ER6BK2I4N3NO4',
        aws_secret_access_key='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'
    )
    s3 = session.resource('s3').Bucket('airline-project-kikipng')
    bucketname = 'airline-project-kikipng'
    for root,dirs,files in os.walk(path):
        for file in files:
            print(file)
            s3.upload_file(os.path.join(root,file),os.path.join(root,file))


# Save the model locally
rfModel.write().overwrite().save('model')

# Transfer the model to s3 busket
uploadDirectory('model')


# Transform the test data 
predictions = rfModel.transform(test)
predictions.select('labelIndex', 'rawPrediction', 'prediction', 'probability').show(10)

# Evaluate the model
evaluator = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %s" % (accuracy))
print("Test Error = %s" % (1.0 - accuracy))

# Create a confusion matrix
preds_and_labels = predictions.select(['prediction','labelIndex']).withColumn('labelIndex', F.col('labelIndex').cast(FloatType())).orderBy('prediction')
preds_and_labels = preds_and_labels.select(['prediction','labelIndex'])
metrics = MulticlassMetrics(preds_and_labels.rdd.map(tuple))
print(metrics.confusionMatrix().toArray())

