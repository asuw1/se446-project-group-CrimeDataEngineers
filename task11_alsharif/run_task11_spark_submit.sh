#!/usr/bin/env bash
# ============================================================================
# SE446 Milestone 2 -- Phase C, Task 11
# spark-submit in YARN cluster mode
#
# Author: Abdulaziz AlSharif (ID: 230055)
#
# Usage (on the cluster, after copying m2_spark_ml.py to ~):
#   chmod +x run_task11_spark_submit.sh
#   ./run_task11_spark_submit.sh 2>&1 | tee output/spark_submit/run.log
#
# The script captures:
#   - the full spark-submit terminal output (job submission + appId)
#   - the YARN application logs (real driver/executor output)
# into output/spark_submit/run.log -- which is the evidence required by the
# milestone spec.
# ============================================================================

set -e
set -o pipefail

mkdir -p output/spark_submit

echo "=============================================================="
echo "Task 11 -- spark-submit (YARN cluster mode)"
echo "Started: $(date)"
echo "=============================================================="

# --- 1. Submit the job ----------------------------------------------------
SUBMIT_LOG="output/spark_submit/submit.txt"

# Note on driver memory: the milestone spec template uses --driver-memory 512m,
# but on this cluster GBT's driver-side broadcast OOM-kills the AM at 512m.
# YARN's max container is 1536 MB. We use 1024m driver + 256 MB overhead =
# 1280 MB AM container, still under the cap, and GBT now fits.
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
    m2_spark_ml.py 2>&1 | tee "$SUBMIT_LOG"

# --- 2. Pull the YARN appId out of the submit log ------------------------
APP_ID=$(grep -oE 'application_[0-9]+_[0-9]+' "$SUBMIT_LOG" | head -1)

if [[ -z "$APP_ID" ]]; then
    echo "ERROR: could not find a YARN application id in $SUBMIT_LOG"
    exit 1
fi

echo
echo "Application id: $APP_ID"

# --- 3. Fetch the YARN logs (the real driver output) ---------------------
echo
echo "Fetching YARN logs for $APP_ID ..."
yarn logs -applicationId "$APP_ID" > "output/spark_submit/yarn_logs_${APP_ID}.txt"

# --- 4. Stitch evidence into one file ------------------------------------
{
    echo "================================================================"
    echo "SE446 M2 -- Task 11 evidence"
    echo "Author : Abdulaziz AlSharif (ID: 230055)"
    echo "Date   : $(date)"
    echo "AppId  : $APP_ID"
    echo "================================================================"
    echo
    echo "----- spark-submit terminal output -----"
    cat "$SUBMIT_LOG"
    echo
    echo "----- yarn logs -applicationId $APP_ID  (excerpt: driver stdout) -----"
    # Extract just the driver's stdout block; full logs remain in yarn_logs_*.txt
    awk '/LogType:stdout/,/End of LogType:stdout/' \
        "output/spark_submit/yarn_logs_${APP_ID}.txt"
} > output/spark_submit/run.log

echo
echo "Done. Evidence saved to:"
echo "  - output/spark_submit/run.log                 <-- include in README"
echo "  - output/spark_submit/submit.txt              <-- raw spark-submit output"
echo "  - output/spark_submit/yarn_logs_${APP_ID}.txt <-- full YARN logs"
