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

## TASK 4 SUMMARY FOR REPORT
Student Name: Wadee Kharbat Role: Task 4 (The Time Dimension)

### Command Used:

mapred streaming \
    -files mapper_task4.py,reducer_sum.py \
    -mapper "python3 mapper_task4.py" \
    -reducer "python3 reducer_sum.py" \
    -input /data/chicago_crimes.csv \
    -output /user/wkharbat/project/m1/task4

### Top 5 Results

2001    467301
2002    205267
2003    985
2004    915
2005    1031

### Interpretation: 

Based on the extracted years and aggregated totals, we can observe the annual volume of crimes reported, allowing us to determine whether crime rates are generally increasing or decreasing over the timeline.

### Execution Logs:
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob8528287240054164388.jar tmpDir=null
2026-03-23 21:50:42,576 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-23 21:50:42,955 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-23 21:50:43,421 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/wkharbat/.staging/job_1771402826595_0146
2026-03-23 21:50:45,240 INFO mapred.FileInputFormat: Total input files to process : 1
2026-03-23 21:50:45,272 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-03-23 21:50:45,273 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-03-23 21:50:45,945 INFO mapreduce.JobSubmitter: number of splits:2
2026-03-23 21:50:46,981 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0146
2026-03-23 21:50:46,982 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-03-23 21:50:47,310 INFO conf.Configuration: resource-types.xml not found
2026-03-23 21:50:47,311 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-03-23 21:50:47,430 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0146
2026-03-23 21:50:47,486 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0146/
2026-03-23 21:50:47,489 INFO mapreduce.Job: Running job: job_1771402826595_0146
2026-03-23 21:51:53,606 INFO mapreduce.Job: Job job_1771402826595_0146 running in uber mode : false
2026-03-23 21:51:53,608 INFO mapreduce.Job:  map 0% reduce 0%
2026-03-23 21:52:18,062 INFO mapreduce.Job:  map 100% reduce 0%
2026-03-23 21:52:33,926 INFO mapreduce.Job:  map 100% reduce 100%
2026-03-23 21:52:36,781 INFO mapreduce.Job: Job job_1771402826595_0146 completed successfully
2026-03-23 21:52:37,016 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=7137663
                FILE: Number of bytes written=15218456
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=181964998
                HDFS: Number of bytes written=245
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=86580
                Total time spent by all reduces in occupied slots (ms)=25832
                Total time spent by all map tasks (ms)=43290
                Total time spent by all reduce tasks (ms)=12916
                Total vcore-milliseconds taken by all map tasks=43290
                Total vcore-milliseconds taken by all reduce tasks=12916
                Total megabyte-milliseconds taken by all map tasks=22164480
                Total megabyte-milliseconds taken by all reduce tasks=6612992
        Map-Reduce Framework
                Map input records=793074
                Map output records=793073
                Map output bytes=5551511
                Map output materialized bytes=7137669
                Input split bytes=198
                Combine input records=0
                Combine output records=0
                Reduce input groups=25
                Reduce shuffle bytes=7137669
                Reduce input records=793073
                Reduce output records=25
                Spilled Records=1586146
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=758
                CPU time spent (ms)=8270
                Physical memory (bytes) snapshot=653885440
                Virtual memory (bytes) snapshot=6564540416
                Total committed heap usage (bytes)=347840512
                Peak Map Physical memory (bytes)=250384384
                Peak Map Virtual memory (bytes)=2185080832
                Peak Reduce Physical memory (bytes)=154152960
                Peak Reduce Virtual memory (bytes)=2195824640
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=181964800
        File Output Format Counters
                Bytes Written=245
2026-03-23 21:52:37,017 INFO streaming.StreamJob: Output directory: /user/wkharbat/project/m1/task4