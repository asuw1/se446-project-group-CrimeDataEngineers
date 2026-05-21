# ============================================================================
# SE446 -- Milestone 2: Spark ML Pipeline (Arrest Prediction)
# Group X  --  Chicago Crime Analytics
#
# Phase C, Task 11: spark-submit on YARN (cluster deploy mode)
#   Author: Abdulaziz AlSharif (ID: 230055)
#
# This standalone script implements Phase B (Tasks 5-7) of Milestone 2 in
# a form that can be submitted to YARN with:
#
#   spark-submit --master yarn --deploy-mode cluster m2_spark_ml.py
#
# A local fallback path is included so the script can be smoke-tested on a
# laptop before submitting -- but the submission target is Task 11 only;
# Tasks 9 (local execution) and 10 (YARN client mode) are out of scope.
#
# Environment is detected automatically. When running on the cluster, the
# script reads the full Chicago Crimes dataset from HDFS and samples 5% for
# ML training (per the milestone spec memory budget). When running locally
# or on Colab, it generates 10,000 realistic rows in-memory (same generator
# as the W09B lab notebook).
# ============================================================================

import os
import sys
import time
import subprocess


# ----------------------------------------------------------------------------
# Environment detection
#
# In YARN cluster deploy mode the driver runs inside the AM container, which
# does NOT have the `hdfs` CLI on PATH. The original "shell out to hdfs"
# probe therefore falls through to "local", and the script ends up forcing
# `master("local[*]")` -- which silently overrides the `--master yarn` we
# passed to spark-submit, runs everything in a single 1 GB driver heap, and
# OOMs as soon as GBT trains. Detect via the env vars spark-submit always
# sets in cluster mode instead.
# ----------------------------------------------------------------------------
def detect_environment():
    """Return one of: 'cluster', 'colab', 'local'."""
    if "google.colab" in sys.modules:
        return "colab"

    # Signals that we are running under spark-submit on YARN.
    if any(os.environ.get(v) for v in (
        "SPARK_YARN_STAGING_DIR",
        "YARN_CONF_DIR",
        "CONTAINER_ID",            # YARN container env var
    )):
        return "cluster"

    # Fallback: HDFS CLI present (covers interactive runs on master node).
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", "/data/chicago_crimes.csv"],
            capture_output=True, timeout=5,
        )
        if result.returncode == 0:
            return "cluster"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return "local"


ENV = detect_environment()


# ----------------------------------------------------------------------------
# Spark session
# ----------------------------------------------------------------------------
def build_spark(env):
    from pyspark.sql import SparkSession

    if env == "cluster":
        # On YARN. --master / --deploy-mode are passed via spark-submit;
        # we deliberately do NOT call .master(...) here so we don't override
        # the spark-submit setting (that was the original bug).
        spark = (
            SparkSession.builder
            .appName("SE446_M2_Phase_C_ArrestPrediction")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.maxResultSize", "128m")
            .config("spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
    else:
        spark = (
            SparkSession.builder
            .appName("SE446_M2_Phase_C_ArrestPrediction_Local")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ----------------------------------------------------------------------------
# Data loading
#   Cluster:  HDFS Chicago Crimes (full 7M+ rows, sampled 5% for ML).
#   Local:    Generated 10,000 realistic rows (same generator as W09B).
# ----------------------------------------------------------------------------
def load_cluster_data(spark):
    from pyspark.sql.functions import col, hour, to_timestamp

    # Phase A (analytics) wants the full dataset; Phase B (ML) needs the
    # smaller sample to fit the cluster memory budget. The milestone spec
    # allows df.sample(0.05, seed=42) OR hdfs:///data/chicago_crimes_sample.csv.
    raw_df = spark.read.csv(
        "hdfs:///data/chicago_crimes.csv",
        header=True, inferSchema=True,
    )
    print(f"[cluster] raw rows: {raw_df.count():,}")

    df = (
        raw_df
        .withColumn("Hour", hour(to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")))
        .select(
            col("District"),
            col("Primary Type").alias("PrimaryType"),
            col("Hour"),
            col("Domestic").cast("string").alias("Domestic_str"),
            col("Arrest"),
        )
        .dropna()
        .withColumn("label", col("Arrest").cast("integer"))
    )

    # 2% sample for ML (Phase B memory budget on the small cluster).
    # The spec allows df.sample(0.05, seed=42); we reduce further to 0.02
    # because the cluster's AM heap is only 512 MB and GBT's driver-side
    # collectAsMap OOMs on 5%. With 0.02 we get ~140k rows -- still well
    # above the 10k local fallback, and trains in ~2-3 min per model.
    df = df.sample(fraction=0.02, seed=42).coalesce(8)
    return df


def load_local_data(spark):
    from pyspark.sql import Row
    import random

    random.seed(42)

    crime_profiles = {
        "NARCOTICS":           0.85,
        "PROSTITUTION":        0.80,
        "WEAPONS VIOLATION":   0.60,
        "BATTERY":             0.30,
        "ASSAULT":             0.25,
        "ROBBERY":             0.15,
        "THEFT":               0.10,
        "BURGLARY":            0.08,
        "MOTOR VEHICLE THEFT": 0.06,
        "CRIMINAL DAMAGE":     0.05,
    }
    districts = list(range(1, 26))

    def generate_row():
        crime_type = random.choice(list(crime_profiles.keys()))
        base_rate = crime_profiles[crime_type]
        district = random.choice(districts)
        hour_val = random.randint(0, 23)
        domestic = random.random() < 0.15
        arrest_prob = base_rate + (0.20 if domestic else 0)
        if 2 <= hour_val <= 5:
            arrest_prob -= 0.10
        arrest_prob = max(0.01, min(0.99, arrest_prob))
        arrest = random.random() < arrest_prob
        return Row(
            District=district,
            PrimaryType=crime_type,
            Hour=hour_val,
            Domestic_str=str(domestic).lower(),
            Arrest=arrest,
            label=int(arrest),
        )

    rows = [generate_row() for _ in range(10_000)]
    return spark.createDataFrame(rows)


# ----------------------------------------------------------------------------
# Task 5: Feature Engineering Pipeline
# ----------------------------------------------------------------------------
def build_feature_pipeline_stages():
    from pyspark.ml.feature import StringIndexer, VectorAssembler

    crime_indexer = StringIndexer(
        inputCol="PrimaryType",
        outputCol="crime_index",
        handleInvalid="skip",
    )
    domestic_indexer = StringIndexer(
        inputCol="Domestic_str",
        outputCol="domestic_index",
        handleInvalid="skip",
    )
    assembler = VectorAssembler(
        inputCols=["District", "crime_index", "Hour", "domestic_index"],
        outputCol="features",
    )
    return crime_indexer, domestic_indexer, assembler


# ----------------------------------------------------------------------------
# Task 6: Train + evaluate three classifiers, return a comparison dict.
# ----------------------------------------------------------------------------
def train_and_evaluate(train_df, test_df, stages, env):
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import (
        LogisticRegression, RandomForestClassifier, GBTClassifier,
    )
    from pyspark.ml.evaluation import (
        BinaryClassificationEvaluator,
        MulticlassClassificationEvaluator,
    )

    binary_eval = BinaryClassificationEvaluator(labelCol="label")
    mc_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction",
    )

    def evaluate(predictions):
        return {
            "AUC":       binary_eval.evaluate(predictions),
            "Accuracy":  mc_eval.evaluate(predictions, {mc_eval.metricName: "accuracy"}),
            "F1":        mc_eval.evaluate(predictions, {mc_eval.metricName: "f1"}),
            "Precision": mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedPrecision"}),
            "Recall":    mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedRecall"}),
        }

    def confusion(predictions):
        rows = (
            predictions.groupBy("label", "prediction").count()
            .orderBy("label", "prediction").collect()
        )
        m = {(0, 0): 0, (0, 1): 0, (1, 0): 0, (1, 1): 0}
        for r in rows:
            m[(int(r["label"]), int(r["prediction"]))] = r["count"]
        return m  # TN, FP, FN, TP

    # Per-model parameters. Cluster values are tuned for the 1536 MB / 1
    # vCore YARN container cap on the SE446 cluster (RF and especially GBT
    # OOM at the spec's 100 trees / 50 iterations).
    classifiers = [
        ("LogisticRegression", LogisticRegression(
            featuresCol="features", labelCol="label",
            maxIter=100, regParam=0.01,
        )),
        ("RandomForest", RandomForestClassifier(
            featuresCol="features", labelCol="label",
            numTrees=40 if env == "cluster" else 50,
            maxDepth=5, seed=42,
        )),
        ("GBT", GBTClassifier(
            featuresCol="features", labelCol="label",
            maxIter=10 if env == "cluster" else 20,
            maxDepth=3 if env == "cluster" else 5,
            stepSize=0.1, seed=42,
        )),
    ]

    results = {}
    fitted_models = {}
    for name, clf in classifiers:
        pipeline = Pipeline(stages=list(stages) + [clf])
        print(f"\n[training] {name} ...")
        t = time.time()
        try:
            model = pipeline.fit(train_df)
        except Exception as exc:                            # pragma: no cover
            # Don't lose LR/RF results if GBT blows up on the cluster.
            print(f"[ERROR] {name} training failed: {exc!r}")
            results[name] = {
                "AUC": float("nan"), "Accuracy": float("nan"),
                "F1": float("nan"), "Precision": float("nan"),
                "Recall": float("nan"),
                "TrainingTime_s": round(time.time() - t, 2),
                "Confusion": {"TN": 0, "FP": 0, "FN": 0, "TP": 0},
                "Error": str(exc),
            }
            continue

        train_time = time.time() - t
        predictions = model.transform(test_df)
        metrics = evaluate(predictions)
        cm = confusion(predictions)
        metrics["TrainingTime_s"] = round(train_time, 2)
        metrics["Confusion"] = {
            "TN": cm[(0, 0)], "FP": cm[(0, 1)],
            "FN": cm[(1, 0)], "TP": cm[(1, 1)],
        }
        results[name] = metrics
        fitted_models[name] = model
        print(f"[done]     {name} in {train_time:.1f}s  AUC={metrics['AUC']:.4f}")

    return results, fitted_models


def print_comparison_table(results):
    print("\n" + "=" * 78)
    print("Task 6 -- Three-Model Comparison")
    print("=" * 78)
    header = f"{'Model':<22}{'AUC':>8}{'Acc':>8}{'F1':>8}{'Prec':>8}{'Rec':>8}{'Time(s)':>10}"
    print(header)
    print("-" * len(header))
    for name, m in results.items():
        print(
            f"{name:<22}"
            f"{m['AUC']:>8.4f}"
            f"{m['Accuracy']:>8.4f}"
            f"{m['F1']:>8.4f}"
            f"{m['Precision']:>8.4f}"
            f"{m['Recall']:>8.4f}"
            f"{m['TrainingTime_s']:>10.2f}"
        )

    print("\nConfusion matrices (label x prediction):")
    for name, m in results.items():
        cm = m["Confusion"]
        print(f"  {name}:  TN={cm['TN']:>7}  FP={cm['FP']:>7}  "
              f"FN={cm['FN']:>7}  TP={cm['TP']:>7}")


# ----------------------------------------------------------------------------
# Task 7: Feature importances + interpretation
# ----------------------------------------------------------------------------
def print_feature_importances(rf_pipeline_model):
    rf = rf_pipeline_model.stages[-1]
    names = ["District", "crime_index", "Hour", "domestic_index"]
    importances = rf.featureImportances.toArray()

    print("\n" + "=" * 78)
    print("Task 7 -- Random Forest Feature Importances")
    print("=" * 78)
    for name, imp in sorted(zip(names, importances), key=lambda x: -x[1]):
        bar = "#" * int(imp * 40)
        print(f"  {name:<18} {imp:.4f}  {bar}")


# ----------------------------------------------------------------------------
# Main entry point
# ----------------------------------------------------------------------------
def main():
    print("=" * 78)
    print("SE446 Milestone 2 -- Phase C standalone Spark ML script")
    print("Author : Abdulaziz AlSharif (ID: 230055)")
    print(f"Env    : {ENV}")
    print("=" * 78)

    spark = build_spark(ENV)
    print(f"Spark version : {spark.version}")
    print(f"Master        : {spark.sparkContext.master}")
    print(f"App ID        : {spark.sparkContext.applicationId}")

    # Fail fast if the master ended up as local while we expected yarn --
    # this is the failure mode that caused the previous three runs.
    if ENV == "cluster" and not spark.sparkContext.master.startswith("yarn"):
        raise RuntimeError(
            f"Expected master=yarn on cluster, got {spark.sparkContext.master!r}. "
            f"Aborting before we exhaust driver memory.")

    # ---- Load ----
    print("\n[load] reading data ...")
    if ENV == "cluster":
        df = load_cluster_data(spark)
    else:
        df = load_local_data(spark)
    n = df.count()
    print(f"[load] working rows: {n:,}")
    df.printSchema()

    # ---- Task 5: feature engineering preview ----
    crime_idx, domestic_idx, assembler = build_feature_pipeline_stages()
    print("\n=== Task 5: Feature Engineering Preview ===")
    print("Vector layout = [District, crime_index, Hour, domestic_index]")
    fitted_crime = crime_idx.fit(df)
    fitted_dom = domestic_idx.fit(df)
    sample = assembler.transform(fitted_dom.transform(fitted_crime.transform(df)))
    sample.select(
        "PrimaryType", "crime_index",
        "District", "Hour",
        "Domestic_str", "domestic_index",
        "features", "label",
    ).show(5, truncate=False)

    # ---- Train / test split ----
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df.cache()
    print(f"[split] train={train_df.count():,}  test={test_df.count():,}")

    # ---- Task 6: train + evaluate all three models ----
    stages = (crime_idx, domestic_idx, assembler)
    results, fitted = train_and_evaluate(train_df, test_df, stages, ENV)
    print_comparison_table(results)

    # ---- Task 7: feature importances from RF ----
    if "RandomForest" in fitted:
        print_feature_importances(fitted["RandomForest"])
    else:
        print("\n[warn] RandomForest model not available -- skipping Task 7 importances.")

    # ---- Save best model as evidence (hint #9 in the spec) ----
    valid = {k: v for k, v in results.items()
             if not (isinstance(v.get("AUC"), float) and v["AUC"] != v["AUC"])}
    if not valid:
        print("\n[warn] no successful models -- nothing to save.")
        spark.stop()
        return
    best_name = max(valid.items(), key=lambda kv: kv[1]["AUC"])[0]
    print(f"\nBest model by AUC: {best_name}  "
          f"(AUC = {valid[best_name]['AUC']:.4f})")

    user_id = os.environ.get("USER", "abdulaziz")
    if ENV == "cluster":
        save_path = f"hdfs:///user/{user_id}/project/m2/best_model"
    else:
        save_path = f"file:///tmp/m2_best_model_{best_name}"

    try:
        fitted[best_name].write().overwrite().save(save_path)
        print(f"[save] best model saved to: {save_path}")
    except Exception as exc:                           # pragma: no cover
        print(f"[save] could not save model ({exc}). Continuing.")

    print("\n=== Phase C run complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
