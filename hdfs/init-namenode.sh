#!/bin/bash
set -e

# # Format namenode if not already formatted
# if [ ! -d /hadoop/dfs/name/current ]; then
#   echo "Formatting namenode"
#   hdfs namenode -format -force
# fi

# sleep 5

echo "Creating HDFS directories"
hdfs dfs -mkdir -p /flights/airline_stats
hdfs dfs -mkdir -p /flights/route_stats
hdfs dfs -mkdir -p /flights/geo_analysis
hdfs dfs -chmod -R 777 /flights

echo "HDFS directories created successfully"
hdfs dfs -ls /flights