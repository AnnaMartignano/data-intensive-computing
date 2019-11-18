#!/bin/bash

export JAVA_HOME="/home/anna/jdk18"
export SPARK_HOME="/home/anna/spark-2.4.3"
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

exec $SHELL -i
