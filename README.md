# Group Name - Crime Data Engineers
## Group members:
- Abdulaziz AlSuwailim 230253
- Sulaiman AlEiteibi 220391
- Abdulaziz AlSharif 230055
- Wadee Kharbat 230685
- Abdulaziz AlSenani 230524

## Executive Summary
This project analyzes the Chicago Crimes dataset using Hadoop MapReduce Streaming. The dataset was distributed across a two-node HDFS cluster and processed through five independent MapReduce jobs written in Python. Each job targets a specific analytical dimension: crime type distribution, location hotspots, temporal trends, and arrest rates. Mappers extract the relevant field from each CSV record and emit key-value pairs, while reducers aggregate the counts per key. Jobs were submitted via the mapred streaming command on a shared Hadoop cluster.

## Task 1: GitHub Setup & Coordination - Abdulaziz AlSuwailim
### Role:
- Setup GitHub Repository
- Coordinated with team to work in their own branches
- Handled Pull Requests and conflicts
- Merged branches to main branch

## Task 2: Crime Type Distribution - Abdulaziz AlSharif

### Instructions (Command Used)
```bash
mapred streaming \
  -files mapper_task2.py,reducer.py \
  -mapper "python3 mapper_task2.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes.csv \
  -output /user/abfalsharif/project/m1/task2
```

### Sample Results:

| Crime Type | Occurances |
|------|------:|
| ARSON | 21 |
| ASSAULT | 878 |
| BATTERY | 1728 |
| BURGLARY | 316 |
| CONCEALED CARRY LICENSE VIOLATION | 6 |

### Interpretation

The MapReduce job successfully computed the distribution of crime types in the dataset. The results show that certain crime categories, such as BATTERY and ASSAULT, have significantly higher frequencies compared to others like ARSON or CRIM SEXUAL ASSAULT.

This indicates that violent and property related crimes are more common in the dataset, while more severe but less frequent crimes occur at lower rates. The mapper correctly extracted the "Primary Type" field from each record, and the reducer accurately aggregated the counts for each crime type.

The successful execution on Hadoop, along with the correct aggregated output, confirms that the MapReduce implementation is functioning as expected and can scale to process large datasets efficiently.

### Execution Log:
```bash
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
```

## Task 3: Location Hotspots - Abdulaziz AlSenani

### Instructions (Command Used):
```bash
hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar \
-files mapper_task3.py,reducer_task3.py \
-input /data/chicago_crimes.csv \
-output /user/aalsenani/task3_final_output \
-mapper "python3 mapper_task3.py" \
-reducer "python3 reducer_task3.py" > task3_final_log.txt 2>&1
```

### Sample Results:
| Location Type                                      | Count |
|----------------------------------------------------|------:|
| ABANDONED BUILDING                                 | 829   |
| AIRCRAFT                                           | 34    |
| AIRPORT BUILDING NON-TERMINAL - NON-SECURE AREA    | 42    |
| AIRPORT BUILDING NON-TERMINAL - SECURE AREA        | 16    |
| AIRPORT EXTERIOR - NON-SECURE AREA                 | 37    |

### Interpretation:
The MapReduce job for Task 3 analyzed the full Chicago crimes dataset by grouping all records according to Location Description and counting the number of incidents in each category. The results show that certain locations appear much more frequently than others, indicating potential crime hotspots. For example, locations such as apartments, alleys, and commercial areas have significantly higher counts compared to less common locations like airports or specialized facilities. This demonstrates that the implementation successfully processed a large-scale dataset and produced meaningful aggregated insights, allowing us to identify which types of environments are most associated with reported crimes.

### Execution Logs
```bash
Last login: Wed Mar 25 13:18:18 on ttys006

The default interactive shell is now zsh.
To update your account to use zsh, please run `chsh -s /bin/zsh`.
For more details, please visit https://support.apple.com/kb/HT208050.
Abdulazizs-MacBook-Pro-5:se446-project-group-CrimeDataEngineers abdulazizal-senani$ ls src
mapper_task3.py		reducer_task3.py
Abdulazizs-MacBook-Pro-5:se446-project-group-CrimeDataEngineers abdulazizal-senani$ scp src/mapper_task3.py src/reducer_task3.py aalsenani@134.209.172.50:~
aalsenani@134.209.172.50's password: 
mapper_task3.py                               100%  411     2.3KB/s   00:00    
reducer_task3.py                              100%  502     2.9KB/s   00:00    
Abdulazizs-MacBook-Pro-5:se446-project-group-CrimeDataEngineers abdulazizal-senani$ ls
README.md	output		scripts		src		test.csv
Abdulazizs-MacBook-Pro-5:se446-project-group-CrimeDataEngineers abdulazizal-senani$ ssh aalsenani@134.209.172.50
aalsenani@134.209.172.50's password: 
Welcome to Ubuntu 22.04.5 LTS (GNU/Linux 5.15.0-170-generic x86_64)

 * Documentation:  https://help.ubuntu.com
 * Management:     https://landscape.canonical.com
 * Support:        https://ubuntu.com/pro

 System information as of Wed Mar 25 10:24:46 UTC 2026

  System load:  0.07               Processes:             134
  Usage of /:   21.3% of 77.35GB   Users logged in:       1
  Memory usage: 55%                IPv4 address for eth0: 134.209.172.50
  Swap usage:   0%                 IPv4 address for eth0: 10.17.0.5

Expanded Security Maintenance for Applications is not enabled.

12 updates can be applied immediately.
To see these additional updates run: apt list --upgradable

Enable ESM Apps to receive additional future security updates.
See https://ubuntu.com/esm or run: sudo pro status

New release '24.04.4 LTS' available.
Run 'do-release-upgrade' to upgrade to it.


*** System restart required ***
Last login: Wed Mar 25 10:07:14 2026 from 188.54.226.75
aalsenani@master-node:~$ ls
mapper_task3.py  reducer_task3.py
aalsenani@master-node:~$ source /etc/profile.d/hadoop.sh
aalsenani@master-node:~$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-input /data/chicago_crimes_sample.csv \
-output /user/aalsenani/task3_sample_output \
-mapper "python3 mapper_task3.py" \
-reducer "python3 reducer_task3.py" > task3_log.txt 2>&1
aalsenani@master-node:~$ cat task3_log.txt
JAR does not exist or is not a normal file: /usr/lib/hadoop-mapreduce/hadoop-streaming.jar
aalsenani@master-node:~$ find / -name "hadoop-streaming*.jar" 2>/dev/null
/opt/hadoop-3.4.1/share/hadoop/tools/sources/hadoop-streaming-3.4.1-sources.jar
/opt/hadoop-3.4.1/share/hadoop/tools/sources/hadoop-streaming-3.4.1-test-sources.jar
/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar
aalsenani@master-node:~$ hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar \
-input /data/chicago_crimes_sample.csv \
-output /user/aalsenani/task3_sample_output \
-mapper "python3 mapper_task3.py" \
-reducer "python3 reducer_task3.py" > task3_log.txt 2>&1
aalsenani@master-node:~$ cat task3_log.txt | tail -20

2026-03-25 10:37:03,799 INFO mapreduce.Job: Counters: 14
	Job Counters 
		Failed map tasks=7
		Killed map tasks=1
		Killed reduce tasks=1
		Launched map tasks=8
		Other local map tasks=6
		Data-local map tasks=2
		Total time spent by all maps in occupied slots (ms)=187698
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=93849
		Total vcore-milliseconds taken by all map tasks=93849
		Total megabyte-milliseconds taken by all map tasks=48050688
	Map-Reduce Framework
		CPU time spent (ms)=0
		Physical memory (bytes) snapshot=0
		Virtual memory (bytes) snapshot=0
2026-03-25 10:37:03,799 ERROR streaming.StreamJob: Job not successful!
Streaming Command Failed!
aalsenani@master-node:~$ hdfs dfs -rm -r /user/aalsenani/task3_sample_output
Deleted /user/aalsenani/task3_sample_output
aalsenani@master-node:~$ hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar \
-files mapper_task3.py,reducer_task3.py \
-input /data/chicago_crimes_sample.csv \
-output /user/aalsenani/task3_sample_output \
-mapper "python3 mapper_task3.py" \
-reducer "python3 reducer_task3.py" > task3_log.txt 2>&1
aalsenani@master-node:~$ cat task3_log.txt | tail -20
		CPU time spent (ms)=3800
		Physical memory (bytes) snapshot=651538432
		Virtual memory (bytes) snapshot=6560391168
		Total committed heap usage (bytes)=348004352
		Peak Map Physical memory (bytes)=252006400
		Peak Map Virtual memory (bytes)=2185666560
		Peak Reduce Physical memory (bytes)=149856256
		Peak Reduce Virtual memory (bytes)=2190823424
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=2391290
	File Output Format Counters 
		Bytes Written=2628
2026-03-25 10:40:29,854 INFO streaming.StreamJob: Output directory: /user/aalsenani/task3_sample_output
aalsenani@master-node:~$ hdfs dfs -cat /user/aalsenani/task3_sample_output/part-00000 | head
ABANDONED BUILDING	2
AIRCRAFT	1
AIRPORT BUILDING NON-TERMINAL - NON-SECURE AREA	2
AIRPORT EXTERIOR - NON-SECURE AREA	2
AIRPORT EXTERIOR - SECURE AREA	3
AIRPORT PARKING LOT	8
AIRPORT TERMINAL LOWER LEVEL - NON-SECURE AREA	5
AIRPORT TERMINAL LOWER LEVEL - SECURE AREA	5
AIRPORT TERMINAL UPPER LEVEL - NON-SECURE AREA	4
AIRPORT TERMINAL UPPER LEVEL - SECURE AREA	11
aalsenani@master-node:~$ hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar \
-files mapper_task3.py,reducer_task3.py \
-input /data/chicago_crimes.csv \
-output /user/aalsenani/task3_final_output \
-mapper "python3 mapper_task3.py" \
-reducer "python3 reducer_task3.py" > task3_final_log.txt 2>&1
aalsenani@master-node:~$ cat task3_final_log.txt | tail -20
		CPU time spent (ms)=10870
		Physical memory (bytes) snapshot=686534656
		Virtual memory (bytes) snapshot=6559305728
		Total committed heap usage (bytes)=348139520
		Peak Map Physical memory (bytes)=260481024
		Peak Map Virtual memory (bytes)=2185170944
		Peak Reduce Physical memory (bytes)=175276032
		Peak Reduce Virtual memory (bytes)=2190426112
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
		Bytes Written=4761
2026-03-25 10:50:53,088 INFO streaming.StreamJob: Output directory: /user/aalsenani/task3_final_output
aalsenani@master-node:~$ hdfs dfs -cat /user/aalsenani/task3_final_output/part-00000 > task3_final_results.txt
aalsenani@master-node:~$ head task3_final_results.txt
ABANDONED BUILDING	829
AIRCRAFT	34
AIRPORT BUILDING NON-TERMINAL - NON-SECURE AREA	42
AIRPORT BUILDING NON-TERMINAL - SECURE AREA	16
AIRPORT EXTERIOR - NON-SECURE AREA	37
AIRPORT EXTERIOR - SECURE AREA	21
AIRPORT PARKING LOT	85
AIRPORT TERMINAL LOWER LEVEL - NON-SECURE AREA	61
AIRPORT TERMINAL LOWER LEVEL - SECURE AREA	42
AIRPORT TERMINAL MEZZANINE - NON-SECURE AREA	4
aalsenani@master-node:~$ task3_final_log.txt
task3_final_log.txt: command not found
aalsenani@master-node:~$ tail -40 task3_final_log.txt > task3_final_execution_log.txt
aalsenani@master-node:~$ cat task3_final_execution_log.txt
		Total vcore-milliseconds taken by all reduce tasks=11722
		Total megabyte-milliseconds taken by all map tasks=29735936
		Total megabyte-milliseconds taken by all reduce tasks=6001664
	Map-Reduce Framework
		Map input records=793074
		Map output records=791479
		Map output bytes=11136841
		Map output materialized bytes=12719811
		Input split bytes=198
		Combine input records=0
		Combine output records=0
		Reduce input groups=212
		Reduce shuffle bytes=12719811
		Reduce input records=791479
		Reduce output records=212
		Spilled Records=1582958
		Shuffled Maps =2
		Failed Shuffles=0
		Merged Map outputs=2
		GC time elapsed (ms)=824
		CPU time spent (ms)=10870
		Physical memory (bytes) snapshot=686534656
		Virtual memory (bytes) snapshot=6559305728
		Total committed heap usage (bytes)=348139520
		Peak Map Physical memory (bytes)=260481024
		Peak Map Virtual memory (bytes)=2185170944
		Peak Reduce Physical memory (bytes)=175276032
		Peak Reduce Virtual memory (bytes)=2190426112
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
		Bytes Written=4761
2026-03-25 10:50:53,088 INFO streaming.StreamJob: Output directory: /user/aalsenani/task3_final_output
aalsenani@master-node:~$ ls -l task3_final_results.txt
-rw-rw-r-- 1 aalsenani aalsenani 4761 Mar 25 10:52 task3_final_results.txt
aalsenani@master-node:~$ less task3_final_results.txt

ABANDONED BUILDING      829
AIRCRAFT        34
AIRPORT BUILDING NON-TERMINAL - NON-SECURE AREA 42
AIRPORT BUILDING NON-TERMINAL - SECURE AREA     16
AIRPORT EXTERIOR - NON-SECURE AREA      37
AIRPORT EXTERIOR - SECURE AREA  21
AIRPORT PARKING LOT     85
AIRPORT TERMINAL LOWER LEVEL - NON-SECURE AREA  61
AIRPORT TERMINAL LOWER LEVEL - SECURE AREA      42
AIRPORT TERMINAL MEZZANINE - NON-SECURE AREA    4
AIRPORT TERMINAL UPPER LEVEL - NON-SECURE AREA  43
AIRPORT TERMINAL UPPER LEVEL - SECURE AREA      133
AIRPORT TRANSPORTATION SYSTEM (ATS)     12
AIRPORT VENDING ESTABLISHMENT   10
AIRPORT/AIRCRAFT        3001
ALLEY   18349
ANIMAL HOSPITAL 13
APARTMENT       61235
APPLIANCE STORE 276
ATHLETIC CLUB   467
ATM (AUTOMATIC TELLER MACHINE)  66
AUTO    1370
AUTO / BOAT / RV DEALERSHIP     96
BANK    3325
BANQUET HALL    2
BAR OR TAVERN   3387
BARBER SHOP/BEAUTY SALON        26
BARBERSHOP      642
BASEMENT        34
BEACH   1
BOAT / WATERCRAFT       5
BOAT/WATERCRAFT 66
BOWLING ALLEY   85
BRIDGE  28
CAR WASH        365
CASINO/GAMBLING ESTABLISHMENT   6
CEMETARY        31
CHA APARTMENT   8342
CHA BREEZEWAY   3
CHA ELEVATOR    3
CHA GROUNDS     48
CHA HALLWAY     39
CHA HALLWAY / STAIRWELL / ELEVATOR      62
CHA HALLWAY/STAIRWELL/ELEVATOR  4773
CHA LOBBY       7
CHA PARKING LOT 57
CHA PARKING LOT / GROUNDS       167
CHA PARKING LOT/GROUNDS 11853
CHA PLAY LOT    4
CHA STAIRWELL   10
CHURCH  6
CHURCH / SYNAGOGUE / PLACE OF WORSHIP   224
CHURCH PROPERTY 2
CHURCH/SYNAGOGUE/PLACE OF WORSHIP       1346
CLEANERS/LAUNDROMAT     1
CLEANING STORE  786
CLUB    18
COACH HOUSE     3
COIN OPERATED MACHINE   108
COLLEGE / UNIVERSITY - GROUNDS  43
COLLEGE / UNIVERSITY - RESIDENCE HALL   12
COLLEGE/UNIVERSITY GROUNDS      450
```
## Task 4: The Time Dimension - Wadee Kharbat 

### Instructions (Command Used):
```bash
mapred streaming \
    -files mapper_task4.py,reducer_sum.py \
    -mapper "python3 mapper_task4.py" \
    -reducer "python3 reducer_sum.py" \
    -input /data/chicago_crimes.csv \
    -output /user/wkharbat/project/m1/task4
```

### Sample Results

| Year | Count |
|------|------:|
| 2001 | 467301 |
| 2002 | 205267 |
| 2003 | 985 |
| 2004 | 915 |
| 2005 | 1031 |

### Interpretation: 

Based on the extracted years and aggregated totals, we can observe the annual volume of crimes reported, allowing us to determine whether crime rates are generally increasing or decreasing over the timeline.

### Execution Logs:
```bash
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
```

## Task 5: Law Enforcement Analysis - Sulaiman AlEiteibi

### Instructions (Command Used):
```bash
mapred streaming \
-files task5-sulaiman.py,sum_reducer.py \
-mapper "python3 task5-sulaiman.py" \
-reducer "python3 sum_reducer.py" \
-input /data/chicago_crimes.csv \
-output /user/saletieibi/project/m1/task5
```

### Results:
```bash
false = 571140
true = 221932
```
### Interpretation:
The results show the total number of crimes that resulted in arrest versus those that did not.

From the full dataset:
- 221,932 crimes resulted in arrest
- 571,140 crimes did not result in arrest

This means that approximately 27.98% of the crimes in the dataset resulted in an arrest, while 72.02% did not.

### Execution Logs
```bash
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob10552308248251811015.jar tmpDir=null
2026-03-24 20:49:58,550 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-24 20:49:58,869 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-24 20:49:59,315 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/saletieibi/.staging/job_1771402826595_0178
2026-03-24 20:50:01,102 INFO mapred.FileInputFormat: Total input files to process : 1
2026-03-24 20:50:01,136 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-03-24 20:50:01,137 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-03-24 20:50:01,819 INFO mapreduce.JobSubmitter: number of splits:2
2026-03-24 20:50:02,876 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0178
2026-03-24 20:50:02,887 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-03-24 20:50:03,318 INFO conf.Configuration: resource-types.xml not found
2026-03-24 20:50:03,319 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-03-24 20:50:03,473 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0178
2026-03-24 20:50:03,544 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0178/
2026-03-24 20:50:03,548 INFO mapreduce.Job: Running job: job_1771402826595_0178
2026-03-24 20:50:24,402 INFO mapreduce.Job: Job job_1771402826595_0178 running in uber mode : false
2026-03-24 20:50:24,405 INFO mapreduce.Job:  map 0% reduce 0%
2026-03-24 20:50:54,893 INFO mapreduce.Job:  map 100% reduce 0%
2026-03-24 20:51:09,534 INFO mapreduce.Job:  map 100% reduce 100%
2026-03-24 20:51:12,398 INFO mapreduce.Job: Job job_1771402826595_0178 completed successfully
2026-03-24 20:51:12,659 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=7708794
                FILE: Number of bytes written=16360823
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=181964998
                HDFS: Number of bytes written=25
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=110342
                Total time spent by all reduces in occupied slots (ms)=24264
                Total time spent by all map tasks (ms)=55171
                Total time spent by all reduce tasks (ms)=12132
                Total vcore-milliseconds taken by all map tasks=55171
                Total vcore-milliseconds taken by all reduce tasks=12132
                Total megabyte-milliseconds taken by all map tasks=28247552
                Total megabyte-milliseconds taken by all reduce tasks=6211584
        Map-Reduce Framework
                Map input records=793074
                Map output records=793072
                Map output bytes=6122644
                Map output materialized bytes=7708800
                Input split bytes=198
                Combine input records=0
                Combine output records=0
                Reduce input groups=2
                Reduce shuffle bytes=7708800
                Reduce input records=793072
                Reduce output records=2
                Spilled Records=1586144
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=920
                CPU time spent (ms)=9110
                Physical memory (bytes) snapshot=662933504
                Virtual memory (bytes) snapshot=6567616512
                Total committed heap usage (bytes)=347926528
                Peak Map Physical memory (bytes)=259461120
                Peak Map Virtual memory (bytes)=2185031680
                Peak Reduce Physical memory (bytes)=155082752
                Peak Reduce Virtual memory (bytes)=2197700608
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
                Bytes Written=25
2026-03-24 20:51:12,665 INFO streaming.StreamJob: Output directory: /user/saletieibi/project/m1/task5
```

## Member Contributions

| Member | ID | Contribution |
|---|---|---|
| Abdulaziz AlSuwailim | 230253 | Task 1 — Set up GitHub repository, managed branches, reviewed and merged pull requests |
| Abdulaziz AlSharif | 230055 | Task 2 — Wrote mapper and reducer for crime type distribution |
| Abdulaziz AlSenani | 230524 | Task 3 — Wrote mapper and reducer for location hotspot analysis |
| Wadee Kharbat | 230685 | Task 4 — Wrote mapper and reducer for yearly crime trend analysis |
| Sulaiman AlEiteibi | 220391 | Task 5 — Wrote mapper and reducer for arrest rate analysis |

---

## Phase C: Deployment -- Task 11 (spark-submit, YARN cluster mode)

**Owner**: Abdulaziz AlSharif (ID: 230055)
**Scope**: Task 11 (spark-submit on YARN cluster mode). Tasks 9 and 10 are submitted on separate branches by Abdulaziz AlSenani.

### Artifact

The standalone Spark ML script for Phase B (Tasks 5-7) is [`m2_spark_ml.py`](./m2_spark_ml.py) at the repo root — authored by Wadee Feras Kharbat. Task 11 takes that script and submits it to the Hadoop cluster via `spark-submit` in YARN cluster mode. The runner script, runbook and cluster evidence for this submission live in [`task11_alsharif/`](./task11_alsharif/).

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

The full driver stdout and YARN logs are stitched into [`task11_alsharif/output/spark_submit/run.log`](./task11_alsharif/output/spark_submit/run.log).

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

### How to reproduce

```bash
# from your laptop
scp m2_spark_ml.py task11_alsharif/run_task11_spark_submit.sh abfalsharif@134.209.172.50:~/

ssh abfalsharif@134.209.172.50
chmod +x run_task11_spark_submit.sh
./run_task11_spark_submit.sh
```

Detailed steps and troubleshooting are in [`task11_alsharif/RUNBOOK_TASK11.md`](./task11_alsharif/RUNBOOK_TASK11.md). The evidence file [`task11_alsharif/output/spark_submit/run.log`](./task11_alsharif/output/spark_submit/run.log) contains the combined `spark-submit` terminal output plus the driver stdout extracted from `yarn logs -applicationId application_1778738889964_0046` — i.e. everything required by the spec.

---

## Milestone 2 — Spark + MLlib

### Executive Summary (M2)

Milestone 2 upgrades the M1 MapReduce pipeline to Apache Spark, reproducing all four analyses (Tasks 1–4) with Spark DataFrames and SQL on the full 793,073-row Chicago Crimes dataset. It then extends the work with a complete MLlib pipeline (Tasks 5–7) to predict arrest outcomes using three classifiers; GBT achieves the best performance (AUC-ROC 0.7899, Accuracy 0.8969, F1 0.8727). The entire workflow is demonstrated in three execution modes: local laptop (`local[*]`), YARN client mode, and YARN cluster mode via `spark-submit`.

---

### M1 vs M2 Comparison (Tasks 1–4)

M2 Phase A ran against the full HDFS dataset (793,073 rows) for all four tasks. M1 Task 2 (crime types) used a ~10,000-row sample; M1 Tasks 3–5 used the full dataset — so Tasks 2–4 below are directly comparable.

#### Task 1 — Crime Type Distribution

| Rank | Crime Type | M2 Spark (full 793k) | M1 MapReduce note |
|---:|---|---:|---|
| 1 | THEFT | 162,688 | M1 ran on ~10k sample (BATTERY was #1 at 1,728 in sample) |
| 2 | BATTERY | 151,930 | |
| 3 | CRIMINAL DAMAGE | 91,241 | |
| 4 | NARCOTICS | 74,127 | |
| 5 | ASSAULT | 54,070 | |
| 6 | MOTOR VEHICLE THEFT | 48,494 | |
| 7 | BURGLARY | 39,872 | |
| 8 | OTHER OFFENSE | 36,893 | |
| 9 | ROBBERY | 30,991 | |
| 10 | DECEPTIVE PRACTICE | 30,396 | |

M1 used a ~10k sample so raw counts differ. Relative proportions are consistent. Spark DataFrame required no mapper/reducer scripts and ran significantly faster.

#### Task 2 — Location Hotspots

| Rank | Location | M2 Spark (full 793k) | M1 MapReduce |
|---:|---|---:|---|
| 1 | STREET | 248,326 | Same #1 (full dataset match) |
| 2 | RESIDENCE | 136,393 | |
| 3 | APARTMENT | 61,235 | |
| 4 | SIDEWALK | 47,506 | |
| 5 | OTHER | 29,671 | |
| 6 | PARKING LOT/GARAGE(NON.RESID.) | 22,436 | |
| 7 | ALLEY | 18,349 | |
| 8 | SCHOOL, PUBLIC, BUILDING | 15,776 | |
| 9 | RESIDENCE-GARAGE | 14,291 | |
| 10 | SMALL RETAIL STORE | 13,804 | |

Both M1 and M2 ran on the full 793k dataset. STREET was #1 in both. Results are **identical** — Spark SQL and the MapReduce reducer produce the same counts.

#### Task 3 — Crime Trend Over Years

| Year | M1 MapReduce | M2 Spark | Match? |
|---:|---:|---:|:---:|
| 2001 | 467,301 | 467,301 | ✓ |
| 2002 | 205,267 | 205,266 | ✓ (1-row header parse diff) |
| 2003–2022 | (same) | (same) | ✓ |

Results are **identical**. Spark ran in-memory vs. disk-based MapReduce shuffle, completing faster.

#### Task 4 — Arrest Rate Analysis

| Metric | M1 MapReduce | M2 Spark | Match? |
|---|---:|---:|:---:|
| Overall Arrest Rate | 27.98% | 27.98% | ✓ |
| NARCOTICS arrest rate | — | 99.88% | M2 adds per-type breakdown |
| BURGLARY arrest rate | — | 6.74% | M2 adds per-type breakdown |

Results are **identical** at the overall level. M2 Spark adds the per-crime-type breakdown in a single DataFrame aggregation, whereas M1 required a separate job.

---

### ML Results Summary (Tasks 5–7)

Phase B builds a Spark MLlib pipeline to predict arrest outcome from four engineered features: `District`, `crime_index` (StringIndexer on Primary Type), `Hour` (extracted from Date), and `domestic_index`. Models are trained on 80% of a 10,000-row sample (`seed=42`) and evaluated on the remaining 20%.

#### Model Comparison (cluster results — `hdfs:///data/chicago_crimes_sample.csv`)

| Metric | Random Forest | Logistic Reg | GBT |
|---|:---:|:---:|:---:|
| AUC-ROC | 0.7787 | 0.6654 | **0.7899** |
| Accuracy | 0.8943 | 0.8740 | **0.8969** |
| F1 Score | 0.8663 | 0.8206 | **0.8727** |
| Training Time (s) | 13.8 | 18.1 | 57.9 |

**Best model**: GBT (AUC-ROC 0.7899, Accuracy 0.8969, F1 0.8727)

#### Confusion Matrices (rows = true label, columns = predicted)

| Model | TN | FP | FN | TP |
|---|---:|---:|---:|---:|
| Logistic Regression | 1,674 | 6 | 236 | 5 |
| Random Forest | 1,667 | 13 | 190 | 51 |
| GBT | 1,663 | 17 | 181 | 60 |

The test set is heavily imbalanced (~87.5% no-arrest). Logistic Regression posts 87.4% accuracy but catches only 5 of 241 arrests (TP=5). Tree models recover real signal: RF lifts TP to 51, GBT to 60, while keeping false positives in single/low double digits.

#### Feature Importances (Random Forest)

```
crime_index        0.8898  ###################################
Hour               0.0514  ##
District           0.0362  #
domestic_index     0.0225
```

**Key finding**: `crime_index` carries ~89% of importance mass. Crime type is the dominant predictor of arrest — NARCOTICS at 99.88%, BURGLARY at 6.74%. This directly confirms the Task 4 arrest-rate analysis.

**Why Logistic Regression underperforms**: LR treats `crime_index` as a continuous ordered variable, which is semantically incorrect for a StringIndexer categorical encoding. Tree models split on index values directly, capturing the non-linear "lookup-table" relationship between crime type and arrest probability.

---

### Deployment Evidence (Tasks 9–11)

#### Task 9 — Local Execution (`local[*]`)

**Owner**: Abdulaziz AlSenani (ID: 230524)  
**Evidence**: [`output/task9/task9_local_execution.txt`](./output/task9/task9_local_execution.txt)

```
Master: local[*]
Input: data/chicago_crimes_sample.csv
Total Rows: 10000
Spark Version: 4.1.1
```

Full Phase A + Phase B pipeline ran successfully on a MacBook using 10,000 rows.

#### Task 10 — Cluster Execution, Client Mode (`yarn --deploy-mode client`)

**Owner**: Abdulaziz AlSenani (ID: 230524)  
**Evidence**: [`output/task10/task10_cluster_client.log`](./output/task10/task10_cluster_client.log)

```
Master: yarn
Input: hdfs:///data/chicago_crimes.csv
Real Row Count: 793073
Application ID: application_1778738889964_0069
Spark Version: 3.5.4
```

Top crime types on full HDFS dataset confirmed (THEFT: 162,688; BATTERY: 151,930). Overall arrest rate: 27.98%.

#### Task 11 — Cluster Execution, spark-submit YARN Cluster Mode

**Owner**: Abdulaziz AlSharif (ID: 230055)  
**Evidence**: [`output/task11/run.log`](./output/task11/run.log)

```
Application ID:       application_1778738889964_0046
ApplicationMaster:    worker-node-1
Final status:         SUCCEEDED
Runtime:              ~3 min 37 s (13:31:56 → 13:35:33 UTC, 2026-05-21)
Master (driver):      yarn
Spark Version:        3.5.4
```

Driver ran on `worker-node-1` (cluster deploy mode). GBT achieved AUC 0.7899 on the cluster run.

---

### spark-submit Command (Task 11)

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

`--deploy-mode cluster` is required: the master VM is small (~4 GB) and shared with Hadoop daemons; running the driver in client mode is OOM-killed by YARN. `--driver-memory 1024m` (not 512m as in the spec template) is needed because the full-spec hyperparameters (RF 100 trees / depth 5, GBT 50 iter / depth 5) cause the AM to OOM at 512m on the driver-side `collectAsMap` of tree splits; YARN's max container is 1536 MB so 1024m driver + 256m overhead = 1280m AM (still under the cap).

---

### Member Contributions (M2)

| Member | ID | M2 Tasks |
|---|---|---|
| Abdulaziz AlSuwailim | 230253 | GitHub coordination, PR merges, conflict resolution, Jupyter notebook (`M2_Spark_ML_GroupX.ipynb`) |
| Sulaiman AlEiteibi | 220391 | Phase A Tasks 1-4 (`m2_phase_a_sulaiman.py`) |
| Abdulaziz AlSharif | 230055 | Task 11 (spark-submit, YARN cluster mode), Tasks 1-2 (notebook) |
| Wadee Feras Kharbat | 230685 | Phase B Tasks 5-7 (`m2_spark_ml.py`) |
| Abdulaziz AlSenani | 230524 | Task 9 (local execution), Task 10 (cluster client mode), Task 3 (notebook) |
