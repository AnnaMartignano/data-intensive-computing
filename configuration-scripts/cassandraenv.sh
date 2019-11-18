#!/bin/bash

export CASSANDRA_HOME="/home/anna/apache-cassandra-3.11.2"
export PYTHONPATH="//home/anna/anaconda2/bin/python"
export PATH=$PYTHONPATH/bin:$CASSANDRA_HOME/bin:$PATH

exec $SHELL -i
