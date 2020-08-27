#!/bin/bash
set -x

# Capture input parameters

for key in "$@"
do
    #key="${key#*=}"
    case "${key}" in
        --executor-cores=*)
        NUM_EXECUTOR_CORES=${key#*=}
        shift
        ;;
        --num-executors=*)
        NUM_EXECUTORS=${key#*=}
        shift
        ;;
        --executor-memory=*)
        EXECUTOR_MEMORY="${key#*=}"
        shift
        ;;
        --driver-cores=*)
        NUM_DRIVER_CORES=${key#*=}
        shift
        ;;
        --python-script=*)
        PYTHON_SCRIPT=${key#*=}
        shift
        ;;
        --partitions=*)
        PARTITIONS=${key#*=}
        shift
        ;;
        --hdfs-input-dir=*)
        HDFS_INPUT_DIR=${key#*=}
        shift
        ;;
        --hdfs-input-file=*)
        HDFS_INPUT_FILE=${key#*=}
        shift
        ;;
        --hdfs-output-dir=*)
        HDFS_OUTPUT_DIR=${key#*=}
        shift
        ;;
        *)
        # Do whatever you want with extra options
        echo "Unknown option '$key'"
	exit 3
        ;;
    esac
done

# Spark cluster parameters
CLUSTER_TYPE="yarn"
CLUSTER_OPTIONS="--deploy-mode cluster"

# Log parameters
LOGDIR=$HOME/backup/log
DATE=`date +"%Y%m%d"`
DATETIME=`date +"%Y%m%d_%H%M%S"`
SCRIPT_NAME=`basename $0`
JOB_NAME=$DATETIME\--$PYTHON_SCRIPT
RAW_LOG_NAME=$JOB_NAME\--raw.log
YARN_LOG_NAME=$JOB_NAME\--yarn.log
RESULTS_LOG_NAME=$JOB_NAME\--results.log

# Clean HDFS output directory from previous runs
if hdfs dfs -ls $HDFS_OUTPUT_DIR;then
	echo "DELETE output directory"
	hdfs dfs -rm -R $HDFS_OUTPUT_DIR
else
	echo "output directory OK"
fi

# spark-submit

spark-submit --master $CLUSTER_TYPE $CLUSTER_OPTIONS \
	 --executor-cores $NUM_EXECUTOR_CORES --num-executors $NUM_EXECUTORS --executor-memory $EXECUTOR_MEMORY \
	 --conf spark.driver.cores=$NUM_DRIVER_CORES \
	 --conf spark.yarn.jars="file:///home/cluster/shared/vms/spark-current/jars/*.jar" \
	 $PYTHON_SCRIPT --partition $PARTITIONS --input $HDFS_INPUT_DIR/$HDFS_INPUT_FILE --output $HDFS_OUTPUT_DIR > $LOGDIR/$RAW_LOG_NAME 2>&1

retVal=$?

if [ $retVal -ne 0 ]; then
	echo "Error"
	# Extract applicationId from the logs
        AppId=`grep "Submitted application application_" logs.txt | cut --delimiter=' ' --fields=7`
        # Get the yarn logs
        yarn logs -applicationId $AppId > $LOGDIR/$YARN_LOG_NAME
        # Extract the results
	echo "CLUSTER_TYPE="$CLUSTER_TYPE > $LOGDIR/$RESULTS_LOG_NAME
	echo "CLUSTER_OPTIONS="$CLUSTER_OPTIONS >> $LOGDIR/$RESULTS_LOG_NAME
  echo "NUM_EXECUTORS="$NUM_EXECUTORS >> $LOGDIR/$RESULTS_LOG_NAME
	echo "NUM_EXECUTOR_CORES="$NUM_EXECUTOR_CORES >> $LOGDIR/$RESULTS_LOG_NAME
	echo "EXECUTOR_MEMORY="$EXECUTOR_MEMORY >> $LOGDIR/$RESULTS_LOG_NAME
	echo "NUM_DRIVER_CORES="$NUM_DRIVER_CORES >> $LOGDIR/$RESULTS_LOG_NAME
	echo "PYTHON_SCRIPT="$PYTHON_SCRIPT >> $LOGDIR/$RESULTS_LOG_NAME
	echo "PARTITIONS="$PARTITIONS >> $LOGDIR/$RESULTS_LOG_NAME
	echo "HDFS_INPUT_DIR="$HDFS_INPUT_DIR >> $LOGDIR/$RESULTS_LOG_NAME
	echo "HDFS_INPUT_FILE="$HDFS_INPUT_FILE >> $LOGDIR/$RESULTS_LOG_NAME
	echo "HDFS_OUTPUT_DIR="$HDFS_OUTPUT_DIR >> $LOGDIR/$RESULTS_LOG_NAME
	echo "--------------------------------------------------------------------------------------"
        grep "WARN" $LOGDIR/$YARN_LOG_NAME >> $LOGDIR/$RESULTS_LOG_NAME
        grep "ERROR" $LOGDIR/$YARN_LOG_NAME >> $LOGDIR/$RESULTS_LOG_NAME
else
	echo "Success"
	# Extract applicationId from the logs
	AppId=`grep "Submitted application application_" $LOGDIR/$RAW_LOG_NAME | cut --delimiter=' ' --fields=7`
	# Get the yarn logs
	yarn logs -applicationId $AppId > $LOGDIR/$YARN_LOG_NAME
	# Extract the results
	echo "CLUSTER_TYPE="$CLUSTER_TYPE > $LOGDIR/$RESULTS_LOG_NAME
	echo "CLUSTER_OPTIONS="$CLUSTER_OPTIONS >> $LOGDIR/$RESULTS_LOG_NAME
  echo "NUM_EXECUTORS="$NUM_EXECUTORS >> $LOGDIR/$RESULTS_LOG_NAME
	echo "NUM_EXECUTOR_CORES="$NUM_EXECUTOR_CORES >> $LOGDIR/$RESULTS_LOG_NAME
	echo "EXECUTOR_MEMORY="$EXECUTOR_MEMORY >> $LOGDIR/$RESULTS_LOG_NAME
	echo "NUM_DRIVER_CORES="$NUM_DRIVER_CORES >> $LOGDIR/$RESULTS_LOG_NAME
	echo "PYTHON_SCRIPT="$PYTHON_SCRIPT >> $LOGDIR/$RESULTS_LOG_NAME
	echo "PARTITIONS="$PARTITIONS >> $LOGDIR/$RESULTS_LOG_NAME
	echo "HDFS_INPUT_DIR="$HDFS_INPUT_DIR >> $LOGDIR/$RESULTS_LOG_NAME
	echo "HDFS_INPUT_FILE="$HDFS_INPUT_FILE >> $LOGDIR/$RESULTS_LOG_NAME
	echo "HDFS_OUTPUT_DIR="$HDFS_OUTPUT_DIR >> $LOGDIR/$RESULTS_LOG_NAME
	echo "--------------------------------------------------------------------------------------"
	hdfs dfs -ls $HDFS_OUTPUT_DIR >> $LOGDIR/$RESULTS_LOG_NAME
	echo "--------------------------------------------------------------------------------------"
	grep "WARN" $LOGDIR/$YARN_LOG_NAME >> $LOGDIR/$RESULTS_LOG_NAME
	ITERATION=`grep Iteration $LOGDIR/$RESULTS_LOG_NAME | wc -l`
	PROCESS_TIME=`grep "Process time" $LOGDIR/$RESULTS_LOG_NAME | cut --delimiter=' ' --fields=8`
	echo "Nb Iteration" $ITERATION
	echo "Process time" $PROCESS_TIME
fi
