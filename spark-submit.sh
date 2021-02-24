#!/bin/bash
n=$1
num=${n:-1}
most_recent=$2
recent_yes=${most_recent:-y}
echo "Number of visitors to display for each day (integer) is ${num}"
echo "Should data be sorted according to most recent data (y or n) > ${recent_yes}"


echo "Running spark-submit with arguments ${num} and ${recent_yes}"
spark-submit --class find_visitors_pipeline --master local /project/code_jar/Secureworks_scala_coding_challenge-assembly-0.1.jar ${num} ${recent_yes}

