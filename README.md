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
```

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


## Phase C: Deployment -- Task 11 (spark-submit, YARN cluster mode)

**Owner**: Abdulaziz AlSharif (ID: 230055)
**Scope**: Task 11 only. Tasks 9 (local mode) and 10 (YARN client mode) are not part of this submission.

### Artifact

The standalone Spark ML script for Phase B (Tasks 5-7) is [`m2_spark_ml.py`](./m2_spark_ml.py). The Phase B logic in the script (load + feature pipeline + three-model training + evaluation + feature importances + interpretation) is Wadee Feras Kharbat's code from the group notebook, copied verbatim. Two small additions exist for Task 11 only:

1. An environment check using `SPARK_YARN_STAGING_DIR` / `YARN_CONF_DIR` / `CONTAINER_ID` (the env vars spark-submit always sets in cluster mode), with a hard guard that aborts if `spark.sparkContext.master` isn't `yarn` -- this prevents the AM container from silently running `master=local[*]` and OOM-ing.
2. Saving the best-by-AUC model to HDFS as run evidence (Hint #9 in the milestone spec).

No hyperparameters, transformers, splits, or evaluation calls have been changed; the numbers this script prints under spark-submit are the same numbers the group notebook prints.

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

`--deploy-mode cluster` is mandatory: the master VM is small (~4 GB) and shared with Hadoop daemons; running the driver on master in client mode is OOM-killed by YARN. In cluster mode the driver runs on a worker.

The milestone spec template uses `--driver-memory 512m`. With the group's full-spec hyperparameters (RF 100 trees / depth 5, GBT 50 iter / depth 5) the driver-side `collectAsMap` of tree splits OOMs the AM at 512m. YARN's max container is 1536 MB, so we use 1024 MB driver + 256 MB overhead = 1280 MB AM container (still under the cap).

### Data

Per the spec ("sample the data with `df.sample(0.05, seed=42)` or use `hdfs:///data/chicago_crimes_sample.csv` so training fits the cluster's memory budget"), this run uses the pre-sampled file:

- **Source**: `hdfs:///data/chicago_crimes_sample.csv`
- **Test rows**: 1,921 (after `dropna()`); train rows ~7,700 on the 80/20 `seed=42` split.

### Execution evidence

- **Application ID**: `application_1778738889964_0046`
- **Final status**: `SUCCEEDED`
- **ApplicationMaster host**: `worker-node-1`
- **Total runtime**: ~3 min 37 s (13:31:56 -> 13:35:33 UTC, 2026-05-21)
- **Spark version**: 3.5.4
- **Master (driver-reported)**: `yarn`

The full driver stdout and YARN logs are stitched into [`output/spark_submit/run.log`](./output/spark_submit/run.log).

### Task 5 - Feature engineering preview

Vector layout = `[District, crime_index, Hour, domestic_index]`.

| PrimaryType                  | Domestic_str | District | Hour | features                  | label |
|------------------------------|:------------:|---------:|-----:|:--------------------------|------:|
| OFFENSE INVOLVING CHILDREN   | false        | 10       | 3    | `[10.0, 12.0, 3.0, 0.0]`   | 1     |
| NARCOTICS                    | false        | 11       | 16   | `[11.0, 10.0, 16.0, 0.0]`  | 1     |
| ROBBERY                      | false        | 14       | 9    | `[14.0, 7.0, 9.0, 0.0]`    | 1     |
| CRIM SEXUAL ASSAULT          | false        | 1        | 10   | `[1.0, 25.0, 10.0, 0.0]`   | 0     |
| CRIMINAL DAMAGE              | false        | 1        | 17   | `[1.0, 2.0, 17.0, 0.0]`    | 0     |

### Task 6 - Three-model comparison (cluster results)

| Metric              | Random Forest | Logistic Reg | GBT          |
|---------------------|--------------:|-------------:|-------------:|
| AUC-ROC             | 0.7787        | 0.6654       | **0.7899**   |
| Accuracy            | 0.8943        | 0.8740       | **0.8969**   |
| F1 Score            | 0.8663        | 0.8206       | **0.8727**   |
| Precision           | 0.8850        | 0.8235       | **0.8865**   |
| Recall              | 0.8943        | 0.8740       | **0.8969**   |
| Training time (s)   | 13.8          | 17.2         | 58.2         |

**Confusion matrices** (rows = true label, columns = predicted):

| Model              | TN   | FP | FN  | TP |
|--------------------|-----:|---:|----:|---:|
| Logistic Regression| 1674 |  6 | 236 |  5 |
| Random Forest      | 1667 | 13 | 190 | 51 |
| GBT                | 1663 | 17 | 181 | 60 |

The test set is heavily imbalanced -- 1,680 no-arrest rows vs. 241 arrest rows (~87.5% / 12.5%). Logistic Regression posts 0.874 accuracy but only catches 5 of the 241 arrests (TP=5, FN=236); it's essentially predicting "no arrest" almost everywhere. Tree-based models recover real signal: RF lifts true positives to 51 and GBT to 60, while keeping false positives in single/low double digits. GBT wins on every metric except training time.

### Task 7 - Random Forest feature importances

```
crime_index        0.8898  ###################################
Hour               0.0514  ##
District           0.0362  #
domestic_index     0.0225
```

`crime_index` carries ~89% of the importance mass, which matches the M1 / Task 4 arrest-rate analysis: arrest probability is driven mostly by crime type (NARCOTICS, PROSTITUTION etc. arrest at very high rates; THEFT / BURGLARY very low). `Hour`, `District`, and `Domestic` are secondary signals.

This also explains why Logistic Regression underperforms: it treats the ordinal `crime_index` as a continuous, linearly-correlated feature, which it isn't. Tree models split on `crime_index` values directly and capture the non-linear, "lookup-table" shape of the arrest rate.

### Saved model

The best classifier (GBT, AUC = 0.7899) is persisted to HDFS at `hdfs:///user/abfalsharif/project/m2/best_model` (matches Hint #9 in the milestone spec).

### How to reproduce

```bash
# from your laptop
scp m2_spark_ml.py run_task11_spark_submit.sh abfalsharif@134.209.172.50:~/

ssh abfalsharif@134.209.172.50
chmod +x run_task11_spark_submit.sh
./run_task11_spark_submit.sh
```

Detailed steps and troubleshooting are in [`docs/RUNBOOK_TASK11.md`](./docs/RUNBOOK_TASK11.md). The evidence file [`output/spark_submit/run.log`](./output/spark_submit/run.log) contains the combined `spark-submit` terminal output plus the driver stdout extracted from `yarn logs -applicationId application_1778738889964_0046` — i.e. everything required by the spec.
