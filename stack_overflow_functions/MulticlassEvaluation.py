import pyspark
import pandas as pd
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def evaluation_metrics(predictionAndLabels, labels):
    """
    Computes the multiclass metrics for predictions and labels 
    args:
        predictionAndLabels: And RDD composed of (prediction, label)
        labels: A list of the unique labels to predict
    returns:
        metrics_results(df) : The multi class metrics by labels
        global_metrics(dict) : The global metrics 
        confusion_matrix (df): The confusion Matrix
    """

    metrics = MulticlassMetrics(predictionAndLabels)

    results = []
    # Statistics by class
    
    for label in sorted(labels):
        results.append((label, metrics.precision(label),metrics.recall(label), metrics.fMeasure(label, beta=1.0)))
    metrics_results = pd.DataFrame(results, columns = ['Label', "Precision","Recall","F1"])

    # Weighted stats
    global_metrics = {}
    global_metrics["weightedRecall"] = metrics.weightedRecall
    global_metrics["weightedPrecision"] = metrics.weightedPrecision
    global_metrics["weightedFMeasure"] = metrics.weightedFMeasure()
    global_metrics["weightedFPR"] = metrics.weightedFalsePositiveRate
    
    # Confusion Matrix
    confusion_matrix = pd.DataFrame(metrics.confusionMatrix().toArray(), columns = labels, index=labels)
    return metrics_results, global_metrics, confusion_matrix
