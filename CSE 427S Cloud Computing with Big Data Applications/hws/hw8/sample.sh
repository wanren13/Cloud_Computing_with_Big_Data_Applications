#!/bin/bash
# Usage : ./sample.sh input output percentage [distributed]
# Local FS Example:
# ./sample.sh part-m-00003 output.txt 0.05 0
# HDFS Example: 
# ./sample.sh /dualcore/oders/part-m-00003 output.txt 0.05 1

if [ $# -lt 3 ]
then
  echo "Usage: $0 input output percentage [distributed]"
  exit 1
fi

distributed=0
input="sample.tmp"

if [ $# -eq 4 ]
then
    distributed=$4
fi

# Create Local file to count lines and take samples
if [ $distributed -ne 0 ]
then
    if [ -f $input ];
    then
        rm -f $input
    fi
    hdfs dfs -get $1 $input
else
    input=$1
fi

# Count and sample
total_lines=`wc -l $input | sed 's/^\([0-9]*\).*$/\1/'`
echo "Total lines of $1 is $total_lines"
calculation="scale=0;$total_lines*$3/1"
lines_to_sample=`bc <<< $calculation`
echo "Sampled $lines_to_sample lines in $2"

# Write output
cat $input | shuf -n $lines_to_sample > $2

# Delete tmp file
if [ $distributed -ne 0 ]
then
    rm -f $input
fi
