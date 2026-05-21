# Group Name: Crime Data Engineers
# Group members:
- Abdulaziz AlSuwailim
- Sulaiman AlEiteibi
- Abdulaziz AlSharif
- Wadee Kharbat
- Abdulaziz AlSenani



## Task 2: Crime Type Distribution - Abdulaziz AlSharif

### Command Used
```bash
mapred streaming \
  -files mapper_task2.py,reducer.py \
  -mapper "python3 mapper_task2.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes_sample.csv \
  -output /user/abfalsharif/project/m1/task2

### Top 5 Results:
ARSON   21
ASSAULT 878
BATTERY 1728
BURGLARY        316
CONCEALED CARRY LICENSE VIOLATION       6

### Interpretation

The MapReduce job successfully computed the distribution of crime types in the dataset. The results show that certain crime categories, such as BATTERY and ASSAULT, have significantly higher frequencies compared to others like ARSON or CRIM SEXUAL ASSAULT.

This indicates that violent and property related crimes are more common in the dataset, while more severe but less frequent crimes occur at lower rates. The mapper correctly extracted the "Primary Type" field from each record, and the reducer accurately aggregated the counts for each crime type.

The successful execution on Hadoop, along with the correct aggregated output, confirms that the MapReduce implementation is functioning as expected and can scale to process large datasets efficiently.

### Execution Log:
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob5557700204849138800.jar tmpDir=null
2026-03-23 14:04:21,184 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-23 14:04:21,483 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-23 14:04:21,917 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/abfalsharif/.staging/job_1771402826595_0125
2026-03-23 14:04:23,775 INFO mapred.FileInputFormat: Total input files to process : 1
2026-03-23 14:04:24,478 INFO mapreduce.JobSubmitter: number of splits:2
2026-03-23 14:04:25,400 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0125
2026-03-23 14:04:25,400 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-03-23 14:04:25,749 INFO conf.Configuration: resource-types.xml not found
2026-03-23 14:04:25,750 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-03-23 14:04:25,876 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0125
2026-03-23 14:04:25,924 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0125/
2026-03-23 14:04:25,927 INFO mapreduce.Job: Running job: job_1771402826595_0125
2026-03-23 14:04:42,693 INFO mapreduce.Job: Job job_1771402826595_0125 running in uber mode : false
2026-03-23 14:04:42,696 INFO mapreduce.Job:  map 0% reduce 0%
2026-03-23 14:05:02,320 INFO mapreduce.Job:  map 100% reduce 0%
2026-03-23 14:05:14,487 INFO mapreduce.Job:  map 100% reduce 100%
2026-03-23 14:05:17,331 INFO mapreduce.Job: Job job_1771402826595_0125 completed successfully
2026-03-23 14:05:17,633 INFO mapreduce.Job: Counters: 54
File System Counters
FILE: Number of bytes read=159339
FILE: Number of bytes written=1261898
HDFS: Number of bytes read=2391502
HDFS: Number of bytes written=541
Map-Reduce Framework
Map input records=10001
Map output records=10000
Reduce input groups=29
Reduce output records=29
File Input Format Counters 
Bytes Read=2391290
File Output Format Counters 
Bytes Written=541
Output directory: /user/abfalsharif/project/m1/task2

<!--
   Paste this block into your group's main README.md under a heading like
   "Phase C: Deployment". It covers Task 11 only; Tasks 9 and 10 are out
   of scope for this submission.
-->

## Phase C: Deployment -- Task 11 (spark-submit, YARN cluster mode)

**Owner**: Abdulaziz AlSharif (ID: 230055)
**Scope**: Task 11 only. Tasks 9 (local mode) and 10 (YARN client mode)
are not part of this submission.

### Artifact

The standalone Spark ML script for Phase B (Tasks 5-7) is
[`m2_spark_ml.py`](./m2_spark_ml.py). It is fully self-contained: it builds
its own `SparkSession`, detects whether it is running under YARN via the
spark-submit env vars, loads the Chicago Crimes CSV from HDFS, samples 2%
for ML training (to fit the cluster's 1536 MB / 1 vCore container cap),
trains and evaluates Logistic Regression, Random Forest and GBT, prints
feature importances, and saves the best model.

### spark-submit command

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 1024m \
    --num-executors 1 \
    --executor-memory 1g \
    --executor-cores 1 \
    --conf spark.driver.maxResultSize=128m \
    --conf spark.yarn.am.memoryOverhead=256 \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3.12 \
    --conf spark.executorEnv.PYSPARK_PYTHON=python3.12 \
    m2_spark_ml.py
```

`--deploy-mode cluster` is mandatory: the master VM is small (~4 GB) and
shared with Hadoop daemons; running the driver on master in client mode is
OOM-killed by YARN. In cluster mode the driver runs on a worker.

The milestone spec template uses `--driver-memory 512m`. On this cluster
that was not enough -- GBT's driver-side `collectAsMap` of tree splits
OOMs the AM. The YARN max container is 1536 MB, so we use 1024 MB driver
+ 256 MB overhead = 1280 MB AM container (still under the cap) and GBT
trains cleanly.

### Memory budget

The full Chicago Crimes dataset (~7M rows) does not fit the ML pipeline
under a 1 GB executor budget. Following the spec, `m2_spark_ml.py`
samples the HDFS data before training. On the SE446 cluster we sampled
**2%** with `df.sample(fraction=0.02, seed=42)`, which yielded 15,954
working rows after `dropna()` -- the spec allows 5% or smaller as long as
the cluster budget is respected.

### Execution evidence

- **Application ID**: `application_1778738889964_0044`
- **Final status**: `SUCCEEDED`
- **ApplicationMaster host**: `worker-node-1`
- **Total runtime**: ~5 min 38 s (12:48:31 → 12:54:09 UTC, 2026-05-21)
- **Spark version**: 3.5.4
- **Master (driver-reported)**: `yarn` (confirms the spark-submit master
  was honored)

The full driver stdout and YARN logs are stitched into
[`output/spark_submit/run.log`](./output/spark_submit/run.log).

### Data

```
[load] reading data ...
[cluster] raw rows:    793,073      (from hdfs:///data/chicago_crimes.csv)
[load] working rows:    15,954      (after 2% sample and dropna)
[split] train=12,854   test=3,100
```

### Task 5 - Feature engineering preview

Vector layout = `[District, crime_index, Hour, domestic_index]`.

| PrimaryType         | crime_index | District | Hour | Domestic_str | domestic_index | features                | label |
|---------------------|------------:|---------:|-----:|:------------:|---------------:|:------------------------|------:|
| HOMICIDE            | 11.0        | 6        | 18   | false        | 0.0            | `[6.0, 11.0, 18.0, 0.0]` | 1     |
| CRIMINAL DAMAGE     | 2.0         | 16       | 12   | false        | 0.0            | `[16.0, 2.0, 12.0, 0.0]` | 0     |
| BATTERY             | 1.0         | 8        | 17   | false        | 0.0            | `[8.0, 1.0, 17.0, 0.0]`  | 0     |
| CRIMINAL DAMAGE     | 2.0         | 14       | 21   | false        | 0.0            | `[14.0, 2.0, 21.0, 0.0]` | 0     |
| MOTOR VEHICLE THEFT | 5.0         | 16       | 12   | false        | 0.0            | `[16.0, 5.0, 12.0, 0.0]` | 0     |

### Task 6 - Three-model comparison (cluster results)

| Model               |     AUC | Accuracy |      F1 | Precision |  Recall | Train time (s) |
|---------------------|--------:|---------:|--------:|----------:|--------:|---------------:|
| LogisticRegression  |  0.5987 |   0.7152 |  0.6225 |    0.6864 |  0.7152 |          17.44 |
| RandomForest        |  0.7975 |   0.8016 |  0.7629 |    0.8442 |  0.8016 |          14.56 |
| **GBT** (best AUC)  |  **0.8052** | **0.8361** | **0.8200** | **0.8441** | **0.8361** | **15.83** |

**Confusion matrices** (rows = true label, columns = predicted):

| Model              | TN   | FP | FN  | TP  |
|--------------------|-----:|---:|----:|----:|
| LogisticRegression | 2158 | 38 | 845 |  59 |
| RandomForest       | 2195 |  1 | 614 | 290 |
| GBT                | 2139 | 57 | 451 | 453 |

The dataset is highly imbalanced (~71% no-arrest vs. 29% arrest), which is
why Logistic Regression's accuracy looks reasonable at 0.72 but its true
positive rate is terrible (TP=59 vs FN=845): it almost always predicts
"no arrest". Tree-based models do far better at picking out the minority
class. GBT wins on both AUC and the TP/FN balance.

### Task 7 - Random Forest feature importances

```
crime_index        0.9665  ######################################
Hour               0.0132
domestic_index     0.0132
District           0.0072
```

`crime_index` dominates with 96.65% of the importance mass, which matches
the M1 / Task 4 arrest-rate analysis: arrest probability is mostly a
function of crime type (e.g. NARCOTICS / PROSTITUTION have very high
arrest rates, THEFT / BURGLARY very low). `Hour` and `Domestic` are
secondary; `District` is essentially noise at this sample size.

This also explains why Logistic Regression underperforms so badly:
arrest probability is a sharply non-linear function of crime category
(an ordinal-encoded feature). LR fits a single linear coefficient on
`crime_index`, which cannot express "NARCOTICS → very likely arrest;
THEFT → very unlikely arrest". Tree-based models split on `crime_index`
values directly and capture this immediately.

### Saved model

The best classifier (GBT, AUC = 0.8052) is persisted to HDFS at
`hdfs:///user/abfalsharif/project/m2/best_model` (matches Hint #9 in the
milestone spec).

### How to reproduce

```bash
# from your laptop
scp m2_spark_ml.py run_task11_spark_submit.sh abfalsharif@134.209.172.50:~/

ssh abfalsharif@134.209.172.50
chmod +x run_task11_spark_submit.sh
./run_task11_spark_submit.sh
```

Detailed steps and troubleshooting are in
[`RUNBOOK_TASK11.md`](./RUNBOOK_TASK11.md). The full evidence files are:

- [`output/spark_submit/run.log`](./output/spark_submit/run.log) -- combined
  spark-submit + driver stdout (the required evidence).
- [`output/spark_submit/submit.txt`](./output/spark_submit/submit.txt) --
  raw `spark-submit` terminal output.
- [`output/spark_submit/yarn_logs_application_1778738889964_0044.txt`](./output/spark_submit/) --
  full YARN logs (driver + executors).


<!--
   Paste this block into your group's main README.md under a heading like
   "Phase C: Deployment". It covers Task 11 only; Tasks 9 and 10 are out
   of scope for this submission.
-->

## Phase C: Deployment -- Task 11 (spark-submit, YARN cluster mode)

**Owner**: Abdulaziz AlSharif (ID: 230055)
**Scope**: Task 11 only. Tasks 9 (local mode) and 10 (YARN client mode)
are not part of this submission.

### Artifact

The standalone Spark ML script for Phase B (Tasks 5-7) is
[`m2_spark_ml.py`](./m2_spark_ml.py). It is fully self-contained: it builds
its own `SparkSession`, detects whether it is running under YARN via the
spark-submit env vars, loads the Chicago Crimes CSV from HDFS, samples 2%
for ML training (to fit the cluster's 1536 MB / 1 vCore container cap),
trains and evaluates Logistic Regression, Random Forest and GBT, prints
feature importances, and saves the best model.

### spark-submit command

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 1024m \
    --num-executors 1 \
    --executor-memory 1g \
    --executor-cores 1 \
    --conf spark.driver.maxResultSize=128m \
    --conf spark.yarn.am.memoryOverhead=256 \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3.12 \
    --conf spark.executorEnv.PYSPARK_PYTHON=python3.12 \
    m2_spark_ml.py
```

`--deploy-mode cluster` is mandatory: the master VM is small (~4 GB) and
shared with Hadoop daemons; running the driver on master in client mode is
OOM-killed by YARN. In cluster mode the driver runs on a worker.

The milestone spec template uses `--driver-memory 512m`. On this cluster
that was not enough -- GBT's driver-side `collectAsMap` of tree splits
OOMs the AM. The YARN max container is 1536 MB, so we use 1024 MB driver
+ 256 MB overhead = 1280 MB AM container (still under the cap) and GBT
trains cleanly.

### Memory budget

The full Chicago Crimes dataset (~7M rows) does not fit the ML pipeline
under a 1 GB executor budget. Following the spec, `m2_spark_ml.py`
samples the HDFS data before training. On the SE446 cluster we sampled
**2%** with `df.sample(fraction=0.02, seed=42)`, which yielded 15,954
working rows after `dropna()` -- the spec allows 5% or smaller as long as
the cluster budget is respected.

### Execution evidence

- **Application ID**: `application_1778738889964_0044`
- **Final status**: `SUCCEEDED`
- **ApplicationMaster host**: `worker-node-1`
- **Total runtime**: ~5 min 38 s (12:48:31 → 12:54:09 UTC, 2026-05-21)
- **Spark version**: 3.5.4
- **Master (driver-reported)**: `yarn` (confirms the spark-submit master
  was honored)

The full driver stdout and YARN logs are stitched into
[`output/spark_submit/run.log`](./output/spark_submit/run.log).

### Data

```
[load] reading data ...
[cluster] raw rows:    793,073      (from hdfs:///data/chicago_crimes.csv)
[load] working rows:    15,954      (after 2% sample and dropna)
[split] train=12,854   test=3,100
```

### Task 5 - Feature engineering preview

Vector layout = `[District, crime_index, Hour, domestic_index]`.

| PrimaryType         | crime_index | District | Hour | Domestic_str | domestic_index | features                | label |
|---------------------|------------:|---------:|-----:|:------------:|---------------:|:------------------------|------:|
| HOMICIDE            | 11.0        | 6        | 18   | false        | 0.0            | `[6.0, 11.0, 18.0, 0.0]` | 1     |
| CRIMINAL DAMAGE     | 2.0         | 16       | 12   | false        | 0.0            | `[16.0, 2.0, 12.0, 0.0]` | 0     |
| BATTERY             | 1.0         | 8        | 17   | false        | 0.0            | `[8.0, 1.0, 17.0, 0.0]`  | 0     |
| CRIMINAL DAMAGE     | 2.0         | 14       | 21   | false        | 0.0            | `[14.0, 2.0, 21.0, 0.0]` | 0     |
| MOTOR VEHICLE THEFT | 5.0         | 16       | 12   | false        | 0.0            | `[16.0, 5.0, 12.0, 0.0]` | 0     |

### Task 6 - Three-model comparison (cluster results)

| Model               |     AUC | Accuracy |      F1 | Precision |  Recall | Train time (s) |
|---------------------|--------:|---------:|--------:|----------:|--------:|---------------:|
| LogisticRegression  |  0.5987 |   0.7152 |  0.6225 |    0.6864 |  0.7152 |          17.44 |
| RandomForest        |  0.7975 |   0.8016 |  0.7629 |    0.8442 |  0.8016 |          14.56 |
| **GBT** (best AUC)  |  **0.8052** | **0.8361** | **0.8200** | **0.8441** | **0.8361** | **15.83** |

**Confusion matrices** (rows = true label, columns = predicted):

| Model              | TN   | FP | FN  | TP  |
|--------------------|-----:|---:|----:|----:|
| LogisticRegression | 2158 | 38 | 845 |  59 |
| RandomForest       | 2195 |  1 | 614 | 290 |
| GBT                | 2139 | 57 | 451 | 453 |

The dataset is highly imbalanced (~71% no-arrest vs. 29% arrest), which is
why Logistic Regression's accuracy looks reasonable at 0.72 but its true
positive rate is terrible (TP=59 vs FN=845): it almost always predicts
"no arrest". Tree-based models do far better at picking out the minority
class. GBT wins on both AUC and the TP/FN balance.

### Task 7 - Random Forest feature importances

```
crime_index        0.9665  ######################################
Hour               0.0132
domestic_index     0.0132
District           0.0072
```

`crime_index` dominates with 96.65% of the importance mass, which matches
the M1 / Task 4 arrest-rate analysis: arrest probability is mostly a
function of crime type (e.g. NARCOTICS / PROSTITUTION have very high
arrest rates, THEFT / BURGLARY very low). `Hour` and `Domestic` are
secondary; `District` is essentially noise at this sample size.

This also explains why Logistic Regression underperforms so badly:
arrest probability is a sharply non-linear function of crime category
(an ordinal-encoded feature). LR fits a single linear coefficient on
`crime_index`, which cannot express "NARCOTICS → very likely arrest;
THEFT → very unlikely arrest". Tree-based models split on `crime_index`
values directly and capture this immediately.

### Saved model

The best classifier (GBT, AUC = 0.8052) is persisted to HDFS at
`hdfs:///user/abfalsharif/project/m2/best_model` (matches Hint #9 in the
milestone spec).

### How to reproduce

```bash
# from your laptop
scp m2_spark_ml.py run_task11_spark_submit.sh abfalsharif@134.209.172.50:~/

ssh abfalsharif@134.209.172.50
chmod +x run_task11_spark_submit.sh
./run_task11_spark_submit.sh
```

Detailed steps and troubleshooting are in
[`RUNBOOK_TASK11.md`](./RUNBOOK_TASK11.md). The full evidence files are:

- [`output/spark_submit/run.log`](./output/spark_submit/run.log) -- combined
  spark-submit + driver stdout (the required evidence).
- [`output/spark_submit/submit.txt`](./output/spark_submit/submit.txt) --
  raw `spark-submit` terminal output.
- [`output/spark_submit/yarn_logs_application_1778738889964_0044.txt`](./output/spark_submit/) --
  full YARN logs (driver + executors).
