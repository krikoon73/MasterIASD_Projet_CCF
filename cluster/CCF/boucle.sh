#!/bin/bash


#NUM_EXECUTORS=8
#EXECUTOR_MEMORY=2g
#NUM_DRIVER_CORES=1
SCRIPT_NAME=$1 #CCF_DF-python2.py
#PARTITIONS=4
HDFS_INPUT_DIR=/user/user341/input
HDFS_INPUT_FILE=web-Google_nohead.txt
HDFS_OUTPUT_DIR=/user/user341/output


for NUM_EXECUTOR_CORES in 2 4
do
     for NUM_EXECUTORS in 2 4 6 8
     do
          for EXECUTOR_MEMORY in 1g 2g 4g
          do
               for NUM_DRIVER_CORES in 1 2
               do
                    for PARTITIONS in 4 8 16 24 32
                    do
                         sh launcher.sh --executor-cores=$NUM_EXECUTOR_CORES \
                            --num-executors=$NUM_EXECUTORS --executor-memory=$EXECUTOR_MEMORY \
                            --driver-cores=$NUM_DRIVER_CORES --python-script=$SCRIPT_NAME \
                            --partitions=$PARTITIONS --hdfs-input-dir=$HDFS_INPUT_DIR --hdfs-input-file=$HDFS_INPUT_FILE --hdfs-output-dir=$HDFS_OUTPUT_DIR
                    done
               done
          done
     done
done
exit
