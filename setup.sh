#!/usr/bin/env bash
set -e  # Exit on error

# Name of the environment (change if needed)
ENV_NAME="datalab"

# Check if requirements.txt exists
if [ ! -f "requirements.txt" ]; then
  echo "❌ requirements.txt not found in current directory!"
  exit 1
fi

# Check if conda is installed
if ! command -v conda &> /dev/null; then
  echo "❌ Conda not found. Please install Miniconda or Mambaforge first."
  exit 1
fi

# Check if environment exists, create if not
if conda env list | grep -q "^${ENV_NAME}\s"; then
  echo "✅ Environment '${ENV_NAME}' already exists."
else
  echo "⚙️  Creating environment '${ENV_NAME}'..."
  conda create -y -n "${ENV_NAME}" python=3.11
fi

# Activate environment
echo "🚀 Activating environment '${ENV_NAME}'..."
# shellcheck disable=SC1091
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "${ENV_NAME}"

# Install pip if missing
conda install -y pip

# Install all Python requirements
echo "📦 Installing packages from requirements.txt..."
pip install -r requirements.txt

echo "✅ All packages installed successfully into '${ENV_NAME}'"

# Set up Cassandra using Docker
# echo "🚀 Setting up Cassandra using Docker..."

# echo "Creating keyspace and tables..."
# docker exec -i cassandra_project cqlsh < cassandra/init.cql

# echo "Cassandra initialization complete!"

# # Verify tables
# docker exec -i cassandra_project cqlsh -e "DESCRIBE flights_db;"