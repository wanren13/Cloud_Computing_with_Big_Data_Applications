#!/bin/bash
# Usage : ./sample.sh input output percentage [distributed]
# Local FS Example:
# ./sample_pig.sh part-m-00003 output.txt 0.05 0
# HDFS Example: 
# ./sample_pig.sh /dualcore/oders/part-m-00003 output.txt 0.05 1

if [ $# -lt 3 ]
then
  echo "Usage: $0 input output percentage [distributed]"
  exit 1
fi

if [[ $# -eq 4 && $4 -ne 0 ]]
then
    pig -p INPUT=$1 -p OUTPUT=$2 -p PERCENT=$3 sample.pig
    hdfs dfs -get $2
    hdfs dfs -rm -r -f $2
else
    pig -p INPUT=$1 -p OUTPUT=$2 -p PERCENT=$3 -x local sample.pig
fi

# sample.pig:
#
#
# data = LOAD '$INPUT';
# subset = SAMPLE data $PERCENT;
# STORE subset INTO '$OUTPUT';