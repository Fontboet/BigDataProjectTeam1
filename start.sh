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
echo "🔄 Enabling Minikube addons..."
minikube addons enable metrics-server
minikube addons enable dashboard
echo ""

echo ""
echo "Loading namespace and setting default context..."
kubectl apply -f k8s/namespace.yml
kubectl config set-context --current --namespace=bigdata
echo ""
echo "Loading Kubernetes configurations..."
kubectl apply -R -f k8s/

echo "✅ Mount started (PID: $MOUNT_PID)"
echo ""
echo "==============================="
echo " Big Data Project"
echo "==============================="
echo ""
echo "Instructions:"
echo "---"
echo "- Manually start the producer :"
echo "  kubectl apply -f k8s/kafka/kafka-producer-job.yml"
echo "To access the Grafana dashboard, run:"
echo "  kubectl port-forward -n bigdata svc/grafana 3000:3000"
echo "  -Then open http://localhost:3000 in your browser."
echo "  Default credentials: admin / admin"
echo "  Note that you can forward other services similarly by changing the service name and ports."
echo ""
echo "⚠️  Keep this terminal open!"
echo ""
echo "To stop: Ctrl+C"


wait $MOUNT_PID