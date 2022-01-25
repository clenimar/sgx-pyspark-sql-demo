from __future__ import print_function

import sys, time
from operator import add
import os

import matplotlib.pyplot as plt
from datetime import datetime
from dateutil import parser
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, date_format, col, when
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.feature import RFormula
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.evaluation import BinaryClassificationEvaluator

if __name__ == "__main__":
    print("###########################################")
    print("######### Starting Spark Session ##########")
    print("###########################################")
    # Start SparkSession
    spark = SparkSession \
            .builder \
            .appName("Confidential Spark Demo") \
            .getOrCreate()

    # Secrets - this information is retrieved from the
    # container env variables - which is populated by Scone CAS after we make sure
    # our Python interpreter and PySpark code were not tampered with.
    blob_account_name = os.environ.get("AZURE_BLOB_ACCOUNT_NAME", "")
    blob_container_name = os.environ.get("AZURE_BLOB_CONTAINER_NAME", "")
    blob_relative_path = os.environ.get("AZURE_BLOB_RELATIVE_PATH", "")
    blob_sas_token = r"%s" % os.environ.get("AZURE_BLOB_SAS_TOKEN", "")
    azure_sql_ae_jdbc = r"%s" % os.environ.get("AZURE_SQL_AE_JDBC", "")
    
    # Read from Azure Blob Storage
    wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
    spark.conf.set(
      'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
      blob_sas_token)
    print('Remote blob path: ' + wasbs_path)
    input_df = spark.read.parquet(wasbs_path)

    # To make development easier, faster, and less expensive, downsample for now
    sampled_taxi_df = input_df.sample(True, 0.00001, seed=1234)

    taxi_df = sampled_taxi_df.select('totalAmount', 'fareAmount', 'tipAmount', 'paymentType', 'rateCodeId', 'passengerCount'\
                                , 'tripDistance', 'tpepPickupDateTime', 'tpepDropoffDateTime'\
                                , date_format('tpepPickupDateTime', 'hh').alias('pickupHour')\
                                , date_format('tpepPickupDateTime', 'EEEE').alias('weekdayString')\
                                , (unix_timestamp(col('tpepDropoffDateTime')) - unix_timestamp(col('tpepPickupDateTime'))).alias('tripTimeSecs')\
                                , (when(col('tipAmount') > 0, 1).otherwise(0)).alias('tipped')
                                )\
                        .filter((sampled_taxi_df.passengerCount > 0) & (sampled_taxi_df.passengerCount < 8)\
                                & (sampled_taxi_df.tipAmount >= 0) & (sampled_taxi_df.tipAmount <= 25)\
                                & (sampled_taxi_df.fareAmount >= 1) & (sampled_taxi_df.fareAmount <= 250)\
                                & (sampled_taxi_df.tipAmount < sampled_taxi_df.fareAmount)\
                                & (sampled_taxi_df.tripDistance > 0) & (sampled_taxi_df.tripDistance <= 100)\
                                & (sampled_taxi_df.rateCodeId <= 5)
                                & (sampled_taxi_df.paymentType.isin({"1", "2"}))
                                )
    taxi_featurised_df = taxi_df.select('totalAmount', 'fareAmount', 'tipAmount', 'paymentType', 'passengerCount'\
                                                , 'tripDistance', 'weekdayString', 'pickupHour','tripTimeSecs','tipped'\
                                                , when((taxi_df.pickupHour <= 6) | (taxi_df.pickupHour >= 20),"Night")\
                                                .when((taxi_df.pickupHour >= 7) & (taxi_df.pickupHour <= 10), "AMRush")\
                                                .when((taxi_df.pickupHour >= 11) & (taxi_df.pickupHour <= 15), "Afternoon")\
                                                .when((taxi_df.pickupHour >= 16) & (taxi_df.pickupHour <= 19), "PMRush")\
                                                .otherwise(0).alias('trafficTimeBins')
                                              )\
                                       .filter((taxi_df.tripTimeSecs >= 30) & (taxi_df.tripTimeSecs <= 7200))

    # Because the sample uses an algorithm that works only with numeric features, convert them so they can be consumed
    sI1 = StringIndexer(inputCol="trafficTimeBins", outputCol="trafficTimeBinsIndex")
    en1 = OneHotEncoder(dropLast=False, inputCol="trafficTimeBinsIndex", outputCol="trafficTimeBinsVec")
    sI2 = StringIndexer(inputCol="weekdayString", outputCol="weekdayIndex")
    en2 = OneHotEncoder(dropLast=False, inputCol="weekdayIndex", outputCol="weekdayVec")

    # Create a new DataFrame that has had the encodings applied
    encoded_final_df = Pipeline(stages=[sI1, en1, sI2, en2]).fit(taxi_featurised_df).transform(taxi_featurised_df)

    # Decide on the split between training and testing data from the DataFrame
    trainingFraction = 0.7
    testingFraction = (1-trainingFraction)
    seed = 1234
    
    # Split the DataFrame into test and training DataFrames
    train_data_df, test_data_df = encoded_final_df.randomSplit([trainingFraction, testingFraction], seed=seed)

    ## Create a new logistic regression object for the model
    logReg = LogisticRegression(maxIter=10, regParam=0.3, labelCol = 'tipped')
    
    ## The formula for the model
    classFormula = RFormula(formula="tipped ~ pickupHour + weekdayVec + passengerCount + tripTimeSecs + tripDistance + fareAmount + paymentType+ trafficTimeBinsVec")

    print("###########################################")
    print("############### TRAIN #####################")
    print("###########################################")
    start_time = time.time() # Timer start
    
    ## Undertake training and create a logistic regression model
    lrModel = Pipeline(stages=[classFormula, logReg]).fit(train_data_df)
    print("\nTraining duration: ", time.time()-start_time) # Timer end
    
    ## Saving the model is optional, but it's another form of inter-session cache
    #datestamp = datetime.now().strftime('%m-%d-%Y-%s')
    #fileName = "lrModel_" + datestamp
    #logRegDirfilename = fileName
    #lrModel.save(logRegDirfilename)
    
    start_time = time.time() # Timer start
    ## Predict tip 1/0 (yes/no) on the test dataset; evaluation using area under ROC
    predictions = lrModel.transform(test_data_df)
    predictionAndLabels = predictions.select("label","prediction").rdd
    metrics = BinaryClassificationMetrics(predictionAndLabels)
    print("Area under ROC = %s" % metrics.areaUnderROC)
    print("\nPredicting duration: ", time.time()-start_time) # Timer end

    # Stop Spark Session
    spark.stop()
