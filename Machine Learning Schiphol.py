# Databricks notebook source
from pyspark.sql import functions
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
#Imports we will need for the EDA and machine learning along with EDA

# COMMAND ----------

#Created schema for ML outputs if it does not exist
spark.sql("CREATE SCHEMA IF NOT EXISTS amsterdam_curated.ml_insights")

#calls flight curated as a data frame for machine learning procedures
flights_df = spark.table("amsterdam_curated.curated_data.flights_curated")

#Creates an is delayed field for labeling by machine learning, if the data says late or delayed the is_delayed column is 1 (true) and if not its (false)
delayed_lab = flights_df.withColumn(
    "is_delayed",
    functions.when(
        functions.col("flight_punctuality_status").isin("Late", "Delayed"),
        functions.lit(1.0)
    ).otherwise(functions.lit(0.0))
)

#Keeps rows for ML where delay_minutes is known and status is not Cancelled/Diverted
train_base = delayed_lab.filter(
    (functions.col("delay_minutes").isNotNull())
    & (~functions.col("flight_punctuality_status").isin("Cancelled", "Diverted"))
)

#Conducts EDA with counting the flight count in the punctuality status per group as well as average delay
eda_punctuality = delayed_lab.groupBy("flight_punctuality_status").agg(
    functions.count("*").alias("flight_count"),
    functions.avg("delay_minutes").alias("avg_delay_min")
)

#Separated in numerical and categorical
feature_columns_numeric = [
    "traffic_hour",
    "no_flights_in_airport",
    "no_flights_in_terminal",
    "congestion_index"
]

categorical_columns = [
    "airlineCode",
    "flightDirection",
    "terminal",
    "serviceType"
]

#Converts all columns in categorical columns into number indexes
indexers = [
    StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep")
    for c in categorical_columns
]

#Combines and ties all numerical and categorical columns into a single vector
assembler = VectorAssembler(
    inputCols=feature_columns_numeric + [c + "_idx" for c in categorical_columns],
    outputCol="features",
    handleInvalid="keep"
)

#Makes a logistics regression model that focuses on taking all features into account and predicting the is_delayed column
log_reg = LogisticRegression(
    featuresCol="features",
    labelCol="is_delayed",
    predictionCol="prediction",
    probabilityCol="probability",
    rawPredictionCol="curatedPrediction"
)

#Assembles the pipeline together to run the machine learning process
pipeline = Pipeline(stages=indexers + [assembler, log_reg])

#Splits the data into training and testing data, 80/20 split
train_data, test_data = train_base.randomSplit([0.8, 0.2], seed=42)

#Trains the pipeline with the training data
model = pipeline.fit(train_data)

#Has model makes a prediction with the test set 
test_prediction = model.transform(test_data)

#Evaluates the model's performamnce and provides accuracy of classification and quality
binary_evaluator = BinaryClassificationEvaluator(
    labelCol="is_delayed",
    rawPredictionCol="curatedPrediction",
    metricName="areaUnderROC"
)

multi_evaluator = MulticlassClassificationEvaluator(
    labelCol="is_delayed",
    predictionCol="prediction",
    metricName="accuracy"
)

ML_auc = binary_evaluator.evaluate(test_prediction)
model_accuracy = multi_evaluator.evaluate(test_prediction)


#Assess the accuracy of its counts of true positive, true negative, false positive and false negative
true_positive = test_prediction.filter(
    (functions.col("is_delayed") == 1.0) & (functions.col("prediction") == 1.0)
).count()
true_negative = test_prediction.filter(
    (functions.col("is_delayed") == 0.0) & (functions.col("prediction") == 0.0)
).count()
false_positive = test_prediction.filter(
    (functions.col("is_delayed") == 0.0) & (functions.col("prediction") == 1.0)
).count()
false_negative = test_prediction.filter(
    (functions.col("is_delayed") == 1.0) & (functions.col("prediction") == 0.0)
).count()


#Assembles the metrics into a dataframe for saving
final_metrics = spark.createDataFrame(
    [(model_accuracy, ML_auc, true_positive, true_negative, false_positive, false_negative)],
    ["model_accuracy", "ML_auc", "true_positive", "true_negative", "false_positive", "false_negative"]
)

#Applies the model on the entire data set to make predictions
full_prediction = model.transform(delayed_lab)

#Stores the results in curated catalog under a separate schema and tables
full_prediction.write.mode("overwrite").saveAsTable(
    "amsterdam_curated.ml_insights.flight_delay_predictions"
)

final_metrics.write.mode("overwrite").saveAsTable(
    "amsterdam_curated.ml_insights.flight_ml_metrics"
)

eda_punctuality.write.mode("overwrite").saveAsTable(
    "amsterdam_curated.ml_insights.flight_eda_summary"
)