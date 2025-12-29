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


echo "📁 Mounting project directory..."
minikube mount "$(pwd)":/mnt/myproject &
MOUNT_PID=$!

echo ""
echo "Loading namespace..."
kubectl apply -f k8s/namespace.yml
echo ""
echo "Loading Kubernetes configurations..."
kubectl apply -f k8s/

echo "✅ Mount started (PID: $MOUNT_PID)"
echo ""
echo "==============================="
echo " Big Data Project"
echo "==============================="
echo "To access the Grafana dashboard, run:"
echo "kubectl port-forward -n bigdata svc/grafana 3000:3000"
echo "Then open http://localhost:3000 in your browser."
echo "Default credentials: admin / admin"
echo "⚠️  Keep this terminal open!"
echo ""
echo "To stop: Ctrl+C"

wait $MOUNT_PID