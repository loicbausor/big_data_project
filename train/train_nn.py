# Logging
import argparse
import logging

# Import Spark NLP
import pyspark
import pyspark.ml.feature as sf
from pyspark.ml import Pipeline
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, Activation,BatchNormalization
from tensorflow.keras.optimizers import Adam
from tensorflow.keras import backend as K
from pyspark.mllib.regression import LabeledPoint
from elephas.spark_model import SparkMLlibModel
from pyspark.mllib.linalg import Vectors  as MLLibVectors


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler("train_nn.log")
c_handler.setLevel(logging.DEBUG)
f_handler.setLevel(logging.DEBUG)
c_format = logging.Formatter('%(name)s - [%(levelname)s] - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)
logger.addHandler(c_handler)
logger.addHandler(f_handler)

def create_keras_model(input_dim, output_dim):
    model = Sequential()
    model.add(Dense(256, input_shape=(input_dim,)))
    model.add(Activation('relu'))
    model.add(BatchNormalization())
    model.add(Dropout(0.3))
    model.add(Dense(256))
    model.add(Activation('relu'))
    model.add(BatchNormalization())
    model.add(Dropout(0.3))
    model.add(Dense(10))
    model.add(Activation('softmax'))
    model.compile(loss='categorical_crossentropy', optimizer='adam',metrics=['accuracy'])
    return model

if __name__ == "__main__": 
    seed = 2020
    input_dim = 1500
    output_dim = 10
    epochs = 10
    save_dir = "models"
    model_dir = "/nn"
    features = "text"
    label = "first_label"
    data_dir = "/home/loic/train/training_sample"

    logger.info("Starting Spark Context")

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
    cv = sf.CountVectorizer(inputCol="text", outputCol="tf_features", vocabSize=input_dim)
    # IDF
    idf = sf.IDF(inputCol="tf_features", outputCol="features")
    label_string= sf.StringIndexer(inputCol="first_label", outputCol ="label")
    pipeline_dl = Pipeline(stages=[cv, idf, label_string])
    df = pipeline_dl.fit(training_set).transform(training_set)
    df = df.rdd.map(lambda x :( LabeledPoint(x['label'],MLLibVectors.fromML(x['features']))))
    logger.info("Pipeline created ...")
    logger.info("Transforms the text into tf idf RDD ...")
    model = create_keras_model(input_dim, output_dim)

    logger.info("Starts Training ...")
    spark_model = SparkMLlibModel(model=model, frequency='epoch', mode='asynchronous',parameter_server_mode='socket')
    spark_model.fit(df, epochs=epochs, batch_size=132, verbose=1, validation_split=0.2, categorical=True, nb_classes=output_dim)

    logger.info("Training done")
    spark_model._master_network.save(save_dir + model_dir + "/" + filename)
    logger.info("Program ended succesfully ! Find the model at :" + save_dir + model_dir + "/" + filename)

