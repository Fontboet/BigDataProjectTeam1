#!/bin/bash
set -e

if hdfs dfs -ls /flights > /dev/null 2>&1; then
  echo "HDFS is already initiated."
  exit 0
fi

echo "Creating HDFS directories ..."
hdfs dfs -mkdir -p /checkpoints/airline_stats
hdfs dfs -mkdir -p /checkpoints/route_stats
hdfs dfs -mkdir -p /checkpoints/geo_analysis
hdfs dfs -mkdir -p /checkpoints/flights
hdfs dfs -mkdir -p /flights
hdfs dfs -chmod -R 777 /checkpoints/
hdfs dfs -chmod -R 777 /flights

echo "HDFS directories created successfully."
hdfs dfs -ls /