# ============================================
# SE446 - Milestone 2: Spark ML Pipeline
# Group: CrimeDataEngineers
#
# Task 5-6: Wadee Feras Kharbat (ID: 230685)
# Task 7:   Wadee Feras Kharbat (ID: 230685)
# Task 11 (spark-submit packaging + cluster deployment):
#          Abdulaziz AlSharif (ID: 230055)
#
# The Phase B logic below (data load, feature pipeline, three-model
# training, evaluation, feature importances, interpretation) is Wadee's
# code from the group notebook, copied verbatim except for two
# Task-11-only additions:
#   1. Environment detection + a guard that aborts if --master yarn was
#      silently replaced by local mode (this happens inside the YARN AM
#      container if you probe HDFS via the CLI -- it isn't on PATH there).
#   2. Saving the best model (by AUC) to HDFS as evidence (Hint #9 in the
#      milestone spec).
# No model parameters, transformers, splits or evaluation calls have been
# changed -- the numbers this script prints when run via spark-submit are
# the same numbers Wadee's notebook prints.
# ============================================
import os, sys, time, subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


# --- Task 11 scaffolding: environment detection ---------------------------
# Inside the YARN AM container the `hdfs` CLI is not on PATH, so probing it
# returns "local" and a downstream `.master("local[*]")` silently overrides
# the `--master yarn` passed to spark-submit. We detect via env vars that
# spark-submit always sets in cluster mode.
def detect_environment():
    if any(os.environ.get(v) for v in (
            "SPARK_YARN_STAGING_DIR", "YARN_CONF_DIR", "CONTAINER_ID")):
        return "cluster"
    try:
        r = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", "/data/chicago_crimes_sample.csv"],
            capture_output=True, timeout=5,
        )
        if r.returncode == 0:
            return "cluster"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return "local"


ENV = detect_environment()
print(f"=== Environment: {ENV} ===")

# Initialize SparkSession
# Note: do NOT call .master() here -- let spark-submit's --master argument
# win. (The prior bug was setting .master("local[*]") in code, which
# overrode --master yarn and caused all training to run single-process in
# the AM container.)
spark = SparkSession.builder.appName("SE446_M2_PhaseB").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print(f"Spark version : {spark.version}")
print(f"Master        : {spark.sparkContext.master}")
print(f"App ID        : {spark.sparkContext.applicationId}")

if ENV == "cluster" and not spark.sparkContext.master.startswith("yarn"):
    raise RuntimeError(
        f"Expected master=yarn on cluster, got {spark.sparkContext.master!r}. "
        f"Aborting before the AM container exhausts memory.")
# --- end Task 11 scaffolding ----------------------------------------------


print("=== Loading Data ===")
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
print("\n=== Task 5: Feature Engineering Pipeline ===")
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
print("Showing sample features before training:")
temp = crime_indexer.fit(df).transform(df)
temp = domestic_indexer.fit(temp).transform(temp)
temp = assembler.transform(temp)
temp.select("PrimaryType", "Domestic_str", "District", "Hour", "features", "label").show(5, truncate=False)
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
train_df.cache()
# ============================================
# Task 6: Train and Evaluate Three Models
# ============================================
print("\n=== Task 6: Train and Evaluate Three Models ===")
binary_eval = BinaryClassificationEvaluator(labelCol="label")
mc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
def evaluate_model(model_name, predictions, train_time):
    auc = binary_eval.evaluate(predictions)
    acc = mc_eval.evaluate(predictions, {mc_eval.metricName: "accuracy"})
    f1 = mc_eval.evaluate(predictions, {mc_eval.metricName: "f1"})
    prec = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedPrecision"})
    rec = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedRecall"})

    print(f"\n--- {model_name} Metrics ---")
    print(f"  Training Time: {train_time:.1f}s")
    print(f"  AUC-ROC:   {auc:.4f}")
    print(f"  Accuracy:  {acc:.4f}")
    print(f"  F1 Score:  {f1:.4f}")
    print(f"  Precision: {prec:.4f}")
    print(f"  Recall:    {rec:.4f}")

    print(f"\n--- Confusion Matrix ({model_name}) ---")
    predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()
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
print("\n=== Model Comparison Table ===")
print("=" * 90)
print(f"{'Metric':<20} {'Random Forest':>15} {'Logistic Reg':>15} {'GBT':>15}")
print("=" * 90)
print(f"{'AUC-ROC':<20} {metrics_rf[0]:>15.4f} {metrics_lr[0]:>15.4f} {metrics_gbt[0]:>15.4f}")
print(f"{'Accuracy':<20} {metrics_rf[1]:>15.4f} {metrics_lr[1]:>15.4f} {metrics_gbt[1]:>15.4f}")
print(f"{'F1 Score':<20} {metrics_rf[2]:>15.4f} {metrics_lr[2]:>15.4f} {metrics_gbt[2]:>15.4f}")
print(f"{'Training Time (s)':<20} {t_rf:>15.1f} {t_lr:>15.1f} {t_gbt:>15.1f}")
print("=" * 90)
# ============================================
# Task 7: Feature Importances & Interpretation
# ============================================
print("\n=== Task 7: Feature Importances & Interpretation ===")
rf_model = model_rf.stages[-1]
feature_names = ["District", "crime_index", "Hour", "domestic_index"]
importances = rf_model.featureImportances.toArray()
print("--- Feature Importances (Random Forest) ---")
for name, imp in sorted(zip(feature_names, importances), key=lambda x: -x[1]):
    bar = "#" * int(imp * 40)
    print(f"  {name:<18} {imp:.4f}  {bar}")
print("\n--- Interpretation Answers ---")
print("Which feature is most important?")
print("The 'crime_index' is the most important feature.")
print("Does this match the arrest rate analysis from Task 4?")
print("Yes, crime types like NARCOTICS and PROSTITUTION have significantly higher arrest rates compared to others.")
print("\nWhy does Logistic Regression perform worse than tree-based models on this data?")
print("Logistic Regression assumes features contribute linearly. It treats 'crime_index' as an ordered continuous number, which is incorrect for categorical data. Tree models capture non-linear patterns and split correctly on index values.")

# --- Task 11 scaffolding: save best model as evidence ---------------------
print("\n=== Saving best model (Task 11 evidence) ===")
candidates = {
    "LogisticRegression": (model_lr, metrics_lr[0]),
    "RandomForest":       (model_rf, metrics_rf[0]),
    "GBT":                (model_gbt, metrics_gbt[0]),
}
best_name = max(candidates, key=lambda k: candidates[k][1])
best_model, best_auc = candidates[best_name]
print(f"Best by AUC: {best_name} (AUC = {best_auc:.4f})")

user_id = os.environ.get("USER", "abdulaziz")
save_path = (
    f"hdfs:///user/{user_id}/project/m2/best_model"
    if ENV == "cluster"
    else f"file:///tmp/m2_best_model_{best_name}"
)
try:
    best_model.write().overwrite().save(save_path)
    print(f"Saved best model to: {save_path}")
except Exception as e:
    print(f"[warn] could not save model: {e}")
# --- end Task 11 scaffolding ----------------------------------------------

spark.stop()
