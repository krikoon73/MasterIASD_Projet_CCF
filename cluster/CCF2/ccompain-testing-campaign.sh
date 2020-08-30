#!/bin/bash
# Author : S. Cohen (August 2020)
# Modified by : C. Compain (30/08/2020)
# Global launcher for CCF testing campaing executions
# Please copy this file for launching your own testing campaing by updating INPUT and OUTPUT Directory
#
set -x
NUM_EXECUTOR_CORES=4
#NUM_EXECUTORS=8
EXECUTOR_MEMORY=4g
EXECUTOR_MEMORY_OVERHEAD=2g
#NUM_DRIVER_CORES=1
DRIVER_MEMORY=4g
#SCRIPT_NAME=$1 #CCF_DF-python2.py
PARTITIONS=128
HDFS_INPUT_DIR=/user/user345/input
HDFS_INPUT_FILE=web-Google_nohead.txt
HDFS_OUTPUT_DIR=/user/user345/output

# Global campaing
# SCRIPTS_LIST="CCF_RDD.py CCF_DF.py CCF_RDD_SS.py CCF_DF_SS.py CCF_RDD_V0.py CCF_DF_V0.py"
# RDD Only Campaing
# SCRIPTS_LIST="CCF_RDD.py CCF_RDD_SS.py"
# DF Only Campaing
SCRIPTS_LIST="CCF_DF.py CCF_DF_SS.py"

for SCRIPT_NAME in $SCRIPTS_LIST
do
	echo "########################################"
	echo $SCRIPT_NAME
	for NUM_EXECUTORS in 2 4 6 8
	do
		for NUM_DRIVER_CORES in 1 2
		do
			sh launcher-v2.sh --executor-cores=$NUM_EXECUTOR_CORES --num-executors=$NUM_EXECUTORS --executor-memory=$EXECUTOR_MEMORY --executor-memory-overhead=$EXECUTOR_MEMORY_OVERHEAD --driver-cores=$NUM_DRIVER_CORES --driver-memory=$DRIVER_MEMORY --python-script=$SCRIPT_NAME --partitions=$PARTITIONS --hdfs-input-dir=$HDFS_INPUT_DIR --hdfs-input-file=$HDFS_INPUT_FILE --hdfs-output-dir=$HDFS_OUTPUT_DIR
		done
	done
	echo "########################################"
done
exit
