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

Top 5 Results:
ARSON   21
ASSAULT 878
BATTERY 1728
BURGLARY        316
CONCEALED CARRY LICENSE VIOLATION       6

Execution Log:
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