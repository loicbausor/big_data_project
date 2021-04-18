# Logging
import os
import argparse
import logging
from pyspark.ml.tuning import ParamGridBuilder
import dill as pickle

# Import Spark NLP
import sparknlp
import pyspark
from sparknlp.base import *
import pyspark.ml.feature as sf
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler("myscript.log")
c_handler.setLevel(logging.DEBUG)
f_handler.setLevel(logging.DEBUG)
c_format = logging.Formatter('%(name)s - [%(levelname)s] - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)
logger.addHandler(c_handler)
logger.addHandler(f_handler)


if __name__ == "__main__": 
    seed = 2020
    save_dir = "models"
    model_dir = "/lr"
    features = "text"
    label = "first_label"
    data_dir = "training_sample"
    logger.info("Starting Spark Context")

    spark = sparknlp.start()
    conf = (pyspark
            .SparkConf()
            .set("spark.ui.showConsoleProgress", "true")
           )
    sc = pyspark.SparkContext.getOrCreate(conf=conf)
    sqlcontext = pyspark.SQLContext(sc)
    training_set = (sqlcontext
                     .read
                     .format("parquet")
                     .option("header",True)
                     .load(data_dir)
                    )
    # TF
    cv = sf.CountVectorizer(inputCol=features, outputCol="tf_features")

    # IDF
    idf = sf.IDF(inputCol="tf_features", outputCol="features")

    # StringIndexer 
    label_string= sf.StringIndexer(inputCol=label, outputCol ="label")

    # Logistic regression
    lr = LogisticRegression(maxIter=10,family="multinomial")
    pipeline = Pipeline(stages=[cv, idf, label_string, lr])

    paramGrid = (ParamGridBuilder() 
                 .addGrid(cv.vocabSize, [500, 1000, 1500]) 
                 .addGrid(lr.regParam, [0.1, 0.01, 0.001]) 
                 .build()
                )
    
    logger.info("Pipeline created ...")
    logger.info("Starts grid search ...")
    crossval = CrossValidator(estimator=pipeline,
                              estimatorParamMaps=paramGrid,
                              evaluator=MulticlassClassificationEvaluator(),
                              numFolds=4,
                              seed=seed)  

    # Run cross-validation, and choose the best set of parameters.
    cvModel = crossval.fit(training_set)
    logger.info("Grid search Done")
    best_model = cvModel.bestModel
    filename = 'ml_lr'
    best_model.write().overwrite().save(save_dir + model_dir +"/"+filename)
    logger.info("Program ended succesfully ! Find the model at :" + save_dir + model_dir + "/" + filename)  
    

