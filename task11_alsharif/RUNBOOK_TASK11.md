# Phase C -- Task 11 Runbook (spark-submit, YARN cluster mode)

**Author**: Abdulaziz AlSharif (ID: 230055)
**Group**: Crime Data Engineers
**Scope**: Task 11 only (Tasks 9 and 10 are out of scope for this submission).

This runbook is the exact sequence to run on the SE446 Hadoop cluster to
collect the evidence required for Task 11. The script being submitted is
the group's Phase B script at the repo root: `m2_spark_ml.py` (authored
by Wadee Feras Kharbat for Tasks 5-7).

---

## 0. Prerequisites

- SSH access to the cluster (same credentials as Milestone 1; cluster
  host `134.209.172.50`).
- The pre-sampled Chicago Crimes file is on HDFS at
  `hdfs:///data/chicago_crimes_sample.csv` (per the milestone spec).
- Python 3.12 is the cluster's `PYSPARK_PYTHON`.

## 1. Copy the script + runner to the cluster

From your laptop, at the repo root:

```bash
scp m2_spark_ml.py task11_alsharif/run_task11_spark_submit.sh \
    <user>@134.209.172.50:~/
```

## 2. SSH in and submit the job

```bash
ssh <user>@134.209.172.50
chmod +x run_task11_spark_submit.sh
mkdir -p output/spark_submit

./run_task11_spark_submit.sh
```

The runner script does three things automatically:

1. Calls `spark-submit --master yarn --deploy-mode cluster ...` with the
   memory flags tuned for this cluster (driver 1024m, executor 1g, 1 vCore).
2. Parses the YARN `application_id` out of the submit output.
3. Calls `yarn logs -applicationId <appId>` and stitches the spark-submit
   terminal output plus the driver stdout into `output/spark_submit/run.log`.

**Why `--deploy-mode cluster`?** The milestone spec explicitly requires
it. The master VM is small (~4 GB) and shared with Hadoop daemons;
running the driver on master in client mode is OOM-killed. In cluster
mode the driver runs on a worker.

**Why `--driver-memory 1024m`?** The spec's template uses 512m, but with
the group's full-spec hyperparameters (RF 100 trees / depth 5, GBT 50
iter / depth 5) the driver-side `collectAsMap` of tree splits OOMs the
AM at 512m. The YARN max container is 1536 MB, so we use 1024 MB driver
+ 256 MB overhead = 1280 MB AM container, still under the cap.

**Why a pre-sampled file?** The full Chicago Crimes dataset (~7M rows)
does not fit the ML pipeline under a 1 GB executor budget. The spec
explicitly allows `hdfs:///data/chicago_crimes_sample.csv` as the input.

## 3. Copy the evidence back to your laptop

From your laptop, at the repo root:

```bash
scp <user>@134.209.172.50:~/output/spark_submit/run.log \
    task11_alsharif/output/spark_submit/run.log
```

## 4. Verify the evidence

`task11_alsharif/output/spark_submit/run.log` should contain, in order:

- The `spark-submit` invocation banner with `--master yarn --deploy-mode cluster`.
- `Application report for application_<id>` lines ending with
  `final status: SUCCEEDED`.
- The driver stdout block (between `LogType:stdout` and `End of LogType:stdout`)
  containing:
  - `=== Loading Data ===`
  - `=== Task 5: Feature Engineering Pipeline ===` with the 5-row preview.
  - `=== Task 6: Train and Evaluate Three Models ===` with metrics +
    confusion matrices for Logistic Regression, Random Forest, and GBT.
  - `=== Model Comparison Table ===`.
  - `=== Task 7: Feature Importances & Interpretation ===` with the
    feature-importance bar chart and the written answers.

If any of these are missing, look at the raw
`output/spark_submit/yarn_logs_<appId>.txt` on the cluster.

## 5. Common failure modes

| Symptom in logs                                       | Fix                                                                                          |
|-------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `Cannot call methods on a stopped SparkContext`       | `--master yarn` was silently overridden to local. Make sure the script does not call `.master(...)` in code. |
| `Container killed by YARN for exceeding memory`       | Lower model hyperparameters (RF `numTrees`, GBT `maxIter`) or driver/executor memory.        |
| `ModuleNotFoundError: pyspark`                        | Wrong Python interpreter. Confirm `PYSPARK_PYTHON=python3.12` in both AM and executor envs.  |
| `Path does not exist: hdfs:///data/chicago_crimes_sample.csv` | Confirm the file is in HDFS: `hdfs dfs -ls /data/`.                                  |
| spark-submit hangs in `ACCEPTED`                      | Another group's job is running. Wait, or `yarn application -list -appStates RUNNING`.        |

## 6. Why Tasks 9 and 10 are omitted

Per group decision (with the instructor's confirmation that AI
assistance is allowed for this milestone), this submission covers
Task 11 only. Tasks 9 and 10 would have added local-mode and
`--deploy-mode client` evidence respectively; they are independent of
Task 11 and out of scope here.
