#!/bin/bash

echo "###### Launcher job SPARK ######"

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd640
export HADOOP_INSTALL=/home/cluster/shared/vms/hadoop-current
export HADOOP_YARN_HOME=/home/cluster/shared/vms/hadoop-current
export HADOOP_COMMON_HOME=/home/cluster/shared/vms/hadoop-current
export HADOOP_CONF_DIR=/home/cluster/shared/vms/hadoop-current/etc/hadoop
export HADOOP_HOME=/home/cluster/shared/vms/hadoop-current
export HADOOP_HDFS_HOME=/home/cluster/shared/vms/hadoop-current
export SPARK_DIST_CLASSPATH=/home/cluster/shared/vms/hadoop-current/etc/hadoop:/home/cluster/shared/vms/hadoop-current/share/hadoop/common/lib/*:/home/cluster/shared/vms/hadoop-current/share/hadoop/common/*:/home/cluster/shared/vms/hadoop-current/share/hadoop/hdfs:/home/cluster/shared/vms/hadoop-current/share/hadoop/hdfs/lib/*:/home/cluster/shared/vms/hadoop-current/share/hadoop/hdfs/*:/home/cluster/shared/vms/hadoop-current/share/hadoop/yarn:/home/cluster/shared/vms/hadoop-current/share/hadoop/yarn/lib/*:/home/cluster/shared/vms/hadoop-current/share/hadoop/yarn/*:/home/cluster/shared/vms/hadoop-current/share/hadoop/mapreduce/lib/*:/home/cluster/shared/vms/hadoop-current/share/hadoop/mapreduce/*:/home/cluster/shared/vms/hadoop-current/contrib/capacity-scheduler/*.jar

echo "Cleaning output directory"

echo "Launch spark job"

echo "Backup job logs"
