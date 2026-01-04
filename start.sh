#!/usr/bin/env bash

cleanup() {
  echo ""
  echo "❌ Interruption : stopping kubectl and deleting namespace..."
  kubectl delete namespace bigdata
  exit 0
}

trap cleanup SIGINT

if ! minikube status | grep -q "host: Running"; then
  echo "⚠️  Please start Minikube first with 'minikube start' command."
  exit 1
else
  echo "✅ Minikube is already running, continuing..."
fi


# echo "📁 Mounting project directory..."
# minikube mount "$(pwd)":/mnt/myproject &
# MOUNT_PID=$!

echo ""
echo "🔄 Enabling Minikube addons..."
echo ""

echo ""
echo "Loading namespace and setting default context..."
kubectl apply -f k8s/namespace.yml
kubectl config set-context --current --namespace=bigdata
echo ""
echo "Loading Kubernetes configurations..."
kubectl create configmap spark-scripts \
  --from-file=streaming.py=./spark/streaming.py \
  -n bigdata
kubectl create configmap kafka-producer-script \
  --from-file=kafka_producer.py=./kafka/kafka_producer.py \
  -n bigdata
kubectl create configmap grafana-dashboard \
  --from-file=./grafana/dashboard.json \
  -n bigdata
kubectl create configmap cassandra-init-script \
  --from-file=init.cql=./cassandra/init_cassandra.cql \
  -n bigdata
kubectl create configmap csv-data \
  --from-file=airports.csv=./data/smallcsv/airports.csv \
  --from-file=airlines.csv=./data/smallcsv/airlines.csv \
  -n bigdata
kubectl apply -R -f k8s/
echo ""
echo "==============================="
echo " Big Data Project"
echo "==============================="
echo ""
echo "Instructions:"
echo "---"
echo "- Grafana dashboard acess :"
# echo "  Run kubectl port-forward svc/grafana 3000:3000"
echo "  Open http://localhost:3000"
echo "  Default credentials: admin / admin"
# echo "- Note that you can forward other services similarly using this command"
# echo "- If the auto-forwarding does not work, you can run this command :"
# echo "  kubectl port-forward svc/grafana 3000:3000"
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
    echo "Port-forward Grafana OK"
    wait $FWD_PID
  fi
done

echo "Grafana port-forward failed after 2 minutes."
echo "Please start it manually with:"
echo "kubectl port-forward -n bigdata svc/grafana 3000:3000"
echo "If you want to restart the environmnent, use :  "
echo "'kubectl delete namespace bigdata' and run this script again."