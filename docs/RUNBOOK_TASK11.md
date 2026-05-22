# Phase C -- Task 11 Runbook (spark-submit, YARN cluster mode)

**Author**: Abdulaziz AlSharif (ID: 230055)
**Group**: Crime Data Engineers
**Scope**: Task 11 only (Tasks 9 and 10 are out of scope for this submission).

This runbook is the exact sequence to run on the SE446 Hadoop cluster to
collect the evidence required for Task 11.

---

## 0. Prerequisites

- SSH access to the cluster (same credentials as Milestone 1).
- The Chicago Crimes CSV is available at `hdfs:///data/chicago_crimes.csv`
  (already there from M1).
- Python 3.12 is the cluster's `PYSPARK_PYTHON` (per the spec).

## 1. Copy the script + runner to the cluster

From your laptop (replace `<user>` and `<cluster-host>`):

```bash
scp m2_spark_ml.py run_task11_spark_submit.sh <user>@<cluster-host>:~/
```

## 2. SSH in and submit the job

```bash
ssh <user>@<cluster-host>
chmod +x run_task11_spark_submit.sh
mkdir -p output/spark_submit

./run_task11_spark_submit.sh
```

The runner script does three things automatically:

1. Calls `spark-submit --master yarn --deploy-mode cluster ...` with the
   exact flags from the milestone spec.
2. Parses the YARN `application_id` out of the submit output.
3. Calls `yarn logs -applicationId <appId>` and stitches everything into
   `output/spark_submit/run.log`.

**Why `--deploy-mode cluster`?** The milestone spec explicitly requires it.
The master VM is small (4 GB) and shared with Hadoop daemons; running the
driver on master in client mode is OOM-killed. With `cluster` mode the
driver runs on a worker.

**Why a 5% sample?** Phase B (Tasks 5-7) trains three classifiers; the
cluster's YARN max allocation is 1536 MB / 1 vCore per container.
`m2_spark_ml.py` samples 0.05 of the HDFS dataset with `seed=42`, exactly
as the spec allows.

## 3. Copy the evidence back to your laptop

```bash
# from your laptop
scp <user>@<cluster-host>:~/output/spark_submit/run.log \
    output/spark_submit/run.log
```

## 4. Verify the evidence

`output/spark_submit/run.log` should contain, in order:

- The `spark-submit` invocation banner (`--master yarn --deploy-mode cluster`).
- `Application report for application_<id>` lines with `FINAL STATUS: SUCCEEDED`.
- The driver's stdout block (between `LogType:stdout` and `End of LogType:stdout`)
  containing:
  - `Master : yarn-<cluster|client>` / `App ID : application_*`
  - `Task 5: Feature Engineering Preview` table.
  - `Task 6: Three-Model Comparison` table with AUC / Acc / F1 / Prec / Rec / Time.
  - `Task 7: Random Forest Feature Importances` bar chart.
  - `Best model by AUC: <name>` line.
  - `[save] best model saved to: hdfs:///user/<your_id>/project/m2/best_model`.

If any of these are missing, look at the raw
`output/spark_submit/yarn_logs_<appId>.txt`; the most common failure modes
are noted below.

## 5. Common failure modes

| Symptom in logs                                  | Fix                                                                                |
|--------------------------------------------------|-------------------------------------------------------------------------------------|
| `OutOfMemoryError` on driver                     | You used `--deploy-mode client`. Switch to `cluster` (this script already does).    |
| `Container killed by YARN for exceeding memory`  | Lower the ML sample fraction (e.g. `0.02`) in `m2_spark_ml.py::load_cluster_data`.  |
| `ModuleNotFoundError: pyspark`                   | Wrong Python interpreter. Confirm the two `PYSPARK_PYTHON=python3.12` confs.        |
| `Path does not exist: hdfs:///data/chicago_crimes.csv` | Confirm the CSV is in HDFS: `hdfs dfs -ls /data/`.                              |
| spark-submit hangs, no `application_id`          | YARN ResourceManager unreachable; check `yarn application -list -appStates RUNNING`. |

## 6. What to put in the group README for Task 11

Include in `README.md` under **Phase C -- Deployment Evidence**:

1. The exact `spark-submit` command (already saved in `output/spark_submit/submit.txt`).
2. The `application_id` and `FINAL STATUS: SUCCEEDED` line.
3. A fenced code block with the contents of `output/spark_submit/run.log`,
   or at minimum the Task 5 / 6 / 7 output blocks from inside it.
4. The HDFS path where the best model was saved.

## 7. Why Tasks 9 and 10 are omitted

Per group decision (with instructor's confirmation that AI assistance is
allowed for this milestone), this submission covers Task 11 only. Tasks 9
and 10 would have added local-mode and `--deploy-mode client` evidence
respectively; they are not prerequisites for Task 11 -- the
`m2_spark_ml.py` script auto-detects environment and is self-contained.
