#!/bin/bash

export JAVA_HOME="/home/anna/jdk18"
export HADOOP_HOME="/home/anna/hadoop-3.1.2"
export HADOOP_CONFIG="$HADOOP_HOME/etc/hadoop"
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export HBASE_HOME="/home/anna/hbase-1.4.10"
export HBASE_CONF="$HBASE_HOME/conf"
export PATH=$HBASE_HOME/bin:$PATH

exec $SHELL -i
