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
    save_dir = "../train/models"
    model_dir = "/lr"
    features = "text"
    label = "first_label"
    data_dir = "training_sample"
    logger.info("Starting Spark Context")
    a= 10
    filename = 'ml_lr.pk'
    with open(save_dir + model_dir +"/"+filename, 'wb') as file:
        pickle.dump(a, file)
    

