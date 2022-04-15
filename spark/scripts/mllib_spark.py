print('hello world')


# Get a spark context
import pyspark
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
#import pyspark.pandas as ps
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import when,split,col,udf,from_unixtime, unix_timestamp, regexp_replace
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql.types import IntegerType

from pyspark.sql.functions import *
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF
from pyspark.ml import Pipeline, PipelineModel




import os
# import matplotlib.pyplot as plt
# import seaborn as sns
# get_ipython().run_line_magic('matplotlib', 'inline')

# from IPython.core.display import HTML
# display(HTML("<style>pre { white-space: pre !important; }</style>"))

#Spark set-up
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
# os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"


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



def clean_text(text):
  ex_list = ['rt', 'http', 'RT']
  exc = '|'.join(ex_list)
  text = re.sub(exc, ' ' , text)
  text = text.lower()
  words = text.split()
  stopword_list = stopwords.words('english')
  words = [word for word in words if not word in stopword_list]
  clean_text = ' '.join(words)
  return clean_text

dfs_skytrax.printSchema()


# ### Exploration



dfs_skytrax.show(5)


def count_nan(dfs_skytrax):
    '''
    Input: a spark df  
    Output: a list with column name and null count
    '''
    null_count_list = []        
    for column in dfs_skytrax.dtypes:    
        col_name = column[0]     
        col_type = column[1]      
        nulls = dfs_skytrax.where( col(col_name).isNull()).count() # count nan in each column 
        result = tuple([col_name, nulls]) #create a tuple with the column name and null count
        null_count_list.append(result)  #append result in list with null counts
    null_count_list=[(x,y) for (x,y) in null_count_list if y!=0] #filter only columns with nan
    return null_count_list



count_nan(dfs_skytrax)



# Drop airline because it has many nan
dfs_skytrax = dfs_skytrax.drop("aircraft")

# Drop review_author column to anonymize data 
dfs_skytrax = dfs_skytrax.drop("review_author")

# Drop review_title, review_date_published as they are not going to be used in the analysis
dfs_skytrax = dfs_skytrax.drop("review_title", "review_date_published")



# Check if the taget column is balanced
dfs_skytrax.groupBy('recommendation').count().show()




# # Check if the taget column is balanced via visualisation
# df_skytrax = dfs_skytrax.toPandas()
# plt.figure(figsize=(10,5))
# sns.set_style("whitegrid", {'axes.grid' : False})
# sns.countplot(x='recommendation', data=df_skytrax, order=df_skytrax['recommendation'].value_counts().index)
# plt.show();


# ### Feature Engineering


# Create a column with the month of date_flown
dfs_skytrax = dfs_skytrax.withColumn("month",split(col("date_flown")," ").getItem(0))            .drop("date_flown")
                         
    

# Create a column that has 1 if the flight is direct and 0 if not
direct_flight = when(col("route").contains("via"), 0).otherwise(1)

dfs_skytrax = dfs_skytrax.withColumn("direct_flight",direct_flight)            .drop("route")



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

# inspired: https://towardsdev.com/how-to-write-pyspark-one-hot-encoding-results-to-an-interpretable-csv-file-626ecb973962



# One-Hot-Encode cabin_flown, traveller_type and airline
column_names = ['cabin_flown','traveller_type', 'airline']
for column_name in column_names:
    dfs_skytrax = get_one_hot_encodings(dfs_skytrax,column_name )




# Convert month name to number
dfs_skytrax = dfs_skytrax.withColumn("month_number",from_unixtime(unix_timestamp(col("month"),'MMMM'),'MM'))                .drop("month")
dfs_skytrax = dfs_skytrax.withColumn("month_number", col("month_number").cast('int'))


# Replace in the recommendation column 'no' with 0 and 'yes' with 1
dfs_skytrax = dfs_skytrax.withColumn('recommendation', regexp_replace('recommendation', 'no', '0'))
dfs_skytrax = dfs_skytrax.withColumn('recommendation', regexp_replace('recommendation', 'yes', '1'))




# Add a new column called weights and fill it with ratios since our dataset is imbalanced
def add_weight_balance(label):
    balancingRatio = dfs_skytrax.filter(col('recommendation') == '1').count() / dfs_skytrax.count()
    return when(label == '1', balancingRatio).otherwise(1-balancingRatio)

dfs_skytrax = dfs_skytrax.withColumn('weights', add_weight_balance(col('recommendation')))










# ### Model

numericCols = [item[0] for item in dfs_skytrax.dtypes if item[1]== 'bigint' or item[1]== 'int' or item[1]== 'double' ]#[1:]

stages = []

assembler = VectorAssembler(inputCols=numericCols, outputCol="features")
stages += [assembler]

dfs_new = assembler.transform(dfs_skytrax)





label_stringIdx = StringIndexer(inputCol = 'recommendation', outputCol = 'labelIndex')
dfs_new = label_stringIdx.fit(dfs_new).transform(dfs_new)



train, test = dfs_new.randomSplit([0.7, 0.3], seed = 2018)
print("Records for training dataset: " + str(train.count()))
print("Records for test dataset: " + str(test.count()))




from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'labelIndex')
rfModel = rf.fit(train)
# pickle.dumps(rfModel)
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

