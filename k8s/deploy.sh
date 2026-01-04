#!/bin/bash
# filepath: /home/anhtu77/Coding/BigDataProjectTeam1/k8s/deploy.sh

set -e

NAMESPACE="bigdata"

echo "=== Deploying Big Data Pipeline to Minikube ==="

# Create namespace
echo "Creating namespace..."
kubectl apply -f namespace.yml

# Function to wait for pod
wait_for_pod() {
    local label=$1
    local timeout=$2
    echo "Waiting for pod with label $label..."
    kubectl wait --for=condition=ready pod -l $label -n $NAMESPACE --timeout=${timeout}s || {
        echo "Timeout waiting for $label"
        kubectl get pods -n $NAMESPACE
        exit 1
    }
}

# Deploy Zookeeper
echo "Deploying Zookeeper..."
kubectl apply -f kafka/zookeeper.yml
wait_for_pod "app=zookeeper" 120

# Deploy Kafka
echo "Deploying Kafka..."
kubectl apply -f kafka/kafka.yml
wait_for_pod "app=kafka" 180

# Initialize Kafka
echo "Initializing Kafka topics..."
kubectl apply -f kafka/kafka-init-job.yml
sleep 10

# Deploy Cassandra
echo "Deploying Cassandra..."
kubectl apply -f cassandra/cassandra.yml
wait_for_pod "app=cassandra" 180

# Initialize Cassandra
echo "Initializing Cassandra schema..."
kubectl apply -f cassandra/cassandra-init-job.yml
sleep 15

# Deploy HDFS
echo "Deploying HDFS..."
kubectl apply -f hdfs/hadoop-config.yml
kubectl apply -f hdfs/namenode.yml
wait_for_pod "app=namenode" 120
kubectl apply -f hdfs/datanode.yml
wait_for_pod "app=datanode" 120

# Deploy Spark
echo "Deploying Spark..."
kubectl apply -f spark/spark-master.yml
wait_for_pod "app=spark-master" 120
kubectl apply -f spark/spark-worker.yml
wait_for_pod "app=spark-worker" 120

# Deploy Spark Submit job
echo "Deploying Spark streaming job..."
kubectl apply -f spark/spark-submit.yml

# Deploy Grafana
echo "Deploying Grafana..."
kubectl apply -f grafana.yml
wait_for_pod "app=grafana" 60

echo ""
echo "=== Deployment Complete ==="
echo ""
kubectl get pods -n $NAMESPACE
echo ""
echo "To start the Kafka producer:"
echo "  kubectl apply -f kafka/kafka-producer.yml"
echo ""
echo "To access Grafana:"
echo "  minikube service grafana -n $NAMESPACE"
echo ""
echo "To access Spark UI:"
echo "  minikube service spark-master -n $NAMESPACE"