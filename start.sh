#!/usr/bin/env bash

cleanup() {
  echo ""
  echo "❌ Interruption : stopping kubectl and deleting namespace..."
  kubectl delete namespace bigdata --ignore-not-found
  exit 0
}

trap cleanup SIGINT

if ! minikube status | grep -q "host: Running"; then
  echo "⚠️  Please start Minikube first with 'minikube start' command."
  exit 1
else
  echo "✅ Minikube is already running, continuing..."
fi

if [ -f "./kafka/flights.csv" ]; then
    echo "✅ flights.csv file found, continuing..."
else
    echo "⚠️  Please download the flights.csv file from Kaggle link in the README and place it in the ./kafka/ directory."
    exit 1
fi

echo ""
echo "🔄 Enabling Minikube addons..."
echo ""

echo ""
echo "Loading namespace and setting default context..."
kubectl apply -f k8s/namespace.yml
kubectl config set-context --current --namespace=bigdata

echo ""
echo "Building Kafka producer Docker image..."
eval $(minikube docker-env)
docker build -t kafka-producer:latest ./kafka

echo ""
echo "Loading Kubernetes configurations..."

# Delete existing configmaps first to allow updates
kubectl delete configmap spark-scripts -n bigdata --ignore-not-found
kubectl delete configmap grafana-dashboard -n bigdata --ignore-not-found
kubectl delete configmap cassandra-init-script -n bigdata --ignore-not-found
kubectl delete configmap csv-data -n bigdata --ignore-not-found

kubectl create configmap spark-scripts \
  --from-file=streaming.py=./spark/streaming.py \
  -n bigdata

kubectl create configmap grafana-dashboard \
  --from-file=dashboard.json=./grafana/dashboard.json \
  -n bigdata

kubectl create configmap cassandra-init-script \
  --from-file=init.cql=./cassandra/init_cassandra.cql \
  -n bigdata

kubectl create configmap csv-data \
  --from-file=airports.csv=./data/airports.csv \
  --from-file=airlines.csv=./data/airlines.csv \
  -n bigdata

echo ""
echo "Applying Kubernetes configurations (excluding HDFS)..."

# Apply individual directories, skipping hdfs
kubectl apply -f k8s/namespace.yml
kubectl apply -f k8s/cassandra/
kubectl apply -f k8s/kafka/
kubectl apply -f k8s/spark/
kubectl apply -f k8s/grafana/

echo ""
echo "==============================="
echo " Big Data Project"
echo "==============================="
echo ""
echo "Waiting for pods to be ready..."
echo ""

# Wait for critical services
echo "Waiting for Zookeeper..."
kubectl wait --for=condition=ready pod -l app=zookeeper -n bigdata --timeout=120s || echo "Zookeeper not ready yet"

echo "Waiting for Kafka..."
kubectl wait --for=condition=ready pod -l app=kafka -n bigdata --timeout=180s || echo "Kafka not ready yet"

echo "Waiting for Cassandra..."
kubectl wait --for=condition=ready pod -l app=cassandra -n bigdata --timeout=180s || echo "Cassandra not ready yet"

echo ""
echo "Instructions:"
echo "---"
echo "- Grafana dashboard access :"
echo "  Open http://localhost:3000"
echo "  Default credentials: admin / admin"
echo "⚠️  Keep this terminal open!"
echo "---"
echo ""
echo "To stop use Ctrl+C"
echo ""

echo "Starting port-forwarding for Grafana..."
for i in {1..24}; do
  kubectl port-forward -n bigdata svc/grafana 3000:3000 >/dev/null 2>&1 &
  FWD_PID=$!
  sleep 5
  if kill -0 $FWD_PID 2>/dev/null; then
    echo "Port-forward Grafana OK (PID $FWD_PID)"
    wait $FWD_PID
    break
  else
    echo "Attempt $i failed, retrying..."
    kill $FWD_PID 2>/dev/null
  fi
done

echo "Grafana port-forward failed after 2 minutes."
echo "Please start it manually with:"
echo "kubectl port-forward -n bigdata svc/grafana 3000:3000"
echo "If you want to restart the environment, use:"
echo "'kubectl delete namespace bigdata' and run this script again."