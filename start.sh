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
echo "- Grafana dashboard acess :"
# echo "  Run kubectl port-forward svc/grafana 3000:3000"
echo "  Open http://localhost:3000"
echo "  Default credentials: admin / admin"
echo "- Note that you can forward other services similarly using this command"
echo "---"
echo ""
echo "⚠️  Keep this terminal open!"
echo ""
echo "To stop: Ctrl+C"

sleep 15 && kubectl port-forward svc/grafana 3000:3000 &

wait $MOUNT_PID