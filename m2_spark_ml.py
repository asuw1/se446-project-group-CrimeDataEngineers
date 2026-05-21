# ============================================
# SE446 - Milestone 2: Spark ML Pipeline
# Group: CrimeDataEngineers
#
# Task 5-6: Wadee Feras Kharbat (ID: 230685)
# Task 7:   Wadee Feras Kharbat (ID: 230685)
# ============================================

import os, sys, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Output file path in your cluster home directory
LOG_FILE_PATH = "/tmp/my_results.txt"

# Custom log function that prints to console AND writes to your file
def log_write(text):
    print(text)
    with open(LOG_FILE_PATH, "a") as f:
        f.write(text + "\n")

# Clear the log file if it exists from a previous run
if os.path.exists(LOG_FILE_PATH):
    os.remove(LOG_FILE_PATH)

# Initialize SparkSession
spark = SparkSession.builder.appName("SE446_M2_PhaseB").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

log_write("=== Loading Data ===")
# Use sample data to fit cluster memory budget for Phase B
raw_df = spark.read.csv(
    "hdfs:///data/chicago_crimes_sample.csv",
    header=True, inferSchema=True
)

df = raw_df.withColumn(
    "Hour", hour(to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
)
df = df.select(
    col("District"),
    col("Primary Type").alias("PrimaryType"),
    col("Hour"),
    col("Domestic").cast("string").alias("Domestic_str"),
    col("Arrest")
).dropna()

df = df.withColumn("label", col("Arrest").cast("integer"))

# ============================================
# Task 5: Feature Engineering Pipeline
# ============================================
log_write("\n=== Task 5: Feature Engineering Pipeline ===")
crime_indexer = StringIndexer(
    inputCol="PrimaryType",
    outputCol="crime_index",
    handleInvalid="skip"
)

domestic_indexer = StringIndexer(
    inputCol="Domestic_str",
    outputCol="domestic_index",
    handleInvalid="skip"
)

assembler = VectorAssembler(
    inputCols=["District", "crime_index", "Hour", "domestic_index"],
    outputCol="features"
)

# Show features for 5 sample rows
log_write("Showing sample features before training:")
temp = crime_indexer.fit(df).transform(df)
temp = domestic_indexer.fit(temp).transform(temp)
temp = assembler.transform(temp)

# Fix: Extract dataframe output as a string so it writes into the file cleanly
sample_rows_str = temp.select("PrimaryType", "Domestic_str", "District", "Hour", "features", "label")._jdf.showString(5, 20, False)
log_write(sample_rows_str)

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
train_df.cache()

# ============================================
# Task 6: Train and Evaluate Three Models
# ============================================
log_write("\n=== Task 6: Train and Evaluate Three Models ===")

binary_eval = BinaryClassificationEvaluator(labelCol="label")
mc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

def evaluate_model(model_name, predictions, train_time):
    auc = binary_eval.evaluate(predictions)
    acc = mc_eval.evaluate(predictions, {mc_eval.metricName: "accuracy"})
    f1 = mc_eval.evaluate(predictions, {mc_eval.metricName: "f1"})
    prec = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedPrecision"})
    rec = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedRecall"})
    
    log_write(f"\n--- {model_name} Metrics ---")
    log_write(f"  Training Time: {train_time:.1f}s")
    log_write(f"  AUC-ROC:   {auc:.4f}")
    log_write(f"  Accuracy:  {acc:.4f}")
    log_write(f"  F1 Score:  {f1:.4f}")
    log_write(f"  Precision: {prec:.4f}")
    log_write(f"  Recall:    {rec:.4f}")
    
    log_write(f"\n--- Confusion Matrix ({model_name}) ---")
    # Fix: Extract matrix data as a string so it writes into the file cleanly
    matrix_str = predictions.groupBy("label", "prediction").count().orderBy("label", "prediction")._jdf.showString(20, 20, False)
    log_write(matrix_str)
    return auc, acc, f1, prec, rec

# 1. Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100, regParam=0.01)
pipeline_lr = Pipeline(stages=[crime_indexer, domestic_indexer, assembler, lr])

t0 = time.time()
model_lr = pipeline_lr.fit(train_df)
t_lr = time.time() - t0
preds_lr = model_lr.transform(test_df)
metrics_lr = evaluate_model("Logistic Regression", preds_lr, t_lr)

# 2. Random Forest
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=100, maxDepth=5, seed=42)
pipeline_rf = Pipeline(stages=[crime_indexer, domestic_indexer, assembler, rf])

t0 = time.time()
model_rf = pipeline_rf.fit(train_df)
t_rf = time.time() - t0
preds_rf = model_rf.transform(test_df)
metrics_rf = evaluate_model("Random Forest", preds_rf, t_rf)

# 3. GBT
gbt = GBTClassifier(featuresCol="features", labelCol="label", maxIter=50, maxDepth=5, seed=42)
pipeline_gbt = Pipeline(stages=[crime_indexer, domestic_indexer, assembler, gbt])

t0 = time.time()
model_gbt = pipeline_gbt.fit(train_df)
t_gbt = time.time() - t0
preds_gbt = model_gbt.transform(test_df)
metrics_gbt = evaluate_model("GBT", preds_gbt, t_gbt)

log_write("\n=== Model Comparison Table ===")
log_write("=" * 90)
log_write(f"{'Metric':<20} {'Random Forest':>15} {'Logistic Reg':>15} {'GBT':>15}")
log_write("=" * 90)
log_write(f"{'AUC-ROC':<20} {metrics_rf[0]:>15.4f} {metrics_lr[0]:>15.4f} {metrics_gbt[0]:>15.4f}")
log_write(f"{'Accuracy':<20} {metrics_rf[1]:>15.4f} {metrics_lr[1]:>15.4f} {metrics_gbt[1]:>15.4f}")
log_write(f"{'F1 Score':<20} {metrics_rf[2]:>15.4f} {metrics_lr[2]:>15.4f} {metrics_gbt[2]:>15.4f}")
log_write(f"{'Training Time (s)':<20} {t_rf:>15.1f} {t_lr:>15.1f} {t_gbt:>15.1f}")
log_write("=" * 90)

# ============================================
# Task 7: Feature Importances & Interpretation
# ============================================
log_write("\n=== Task 7: Feature Importances & Interpretation ===")
rf_model = model_rf.stages[-1]
feature_names = ["District", "crime_index", "Hour", "domestic_index"]
importances = rf_model.featureImportances.toArray()

log_write("--- Feature Importances (Random Forest) ---")
for name, imp in sorted(zip(feature_names, importances), key=lambda x: -x[1]):
    bar = "#" * int(imp * 40)
    log_write(f"  {name:<18} {imp:.4f}  {bar}")

log_write("\n--- Interpretation Answers ---")
log_write("Which feature is most important?")
log_write("The 'crime_index' is the most important feature.")
log_write("Does this match the arrest rate analysis from Task 4?")
log_write("Yes, crime types like NARCOTICS and PROSTITUTION have significantly higher arrest rates compared to others.")
log_write("\nWhy does Logistic Regression perform worse than tree-based models on this data?")
log_write("Logistic Regression assumes features contribute linearly. It treats 'crime_index' as an ordered continuous number, which is incorrect for categorical data. Tree models capture non-linear patterns and split correctly on index values.")

spark.stop()