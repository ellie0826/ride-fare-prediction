#!/bin/bash

echo "🚗 Starting Ride Fare Prediction System"
echo "========================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running."
    echo ""
    echo "To fix this:"
    echo "1. Open Docker Desktop application"
    echo "2. Wait for Docker to fully start (whale icon steady in menu bar)"
    echo "3. Or run: open -a Docker (on macOS)"
    echo "4. Then run this script again"
    echo ""
    exit 1
fi

# Clean up any existing containers
echo "🧹 Cleaning up existing containers..."
docker-compose down 2>/dev/null || true

# Force remove specific containers that might be stuck
echo "🔧 Force removing any stuck containers..."
docker rm -f zookeeper kafka redis mlflow prediction-service user-driver-service traffic-weather-service frontend 2>/dev/null || true

# Clean up any remaining containers
docker container prune -f > /dev/null 2>&1 || true

# Check and free up required ports
echo "🔌 Checking for processes using required ports..."
PORTS=(5001 8000 8501 9092 6379 2181)
for port in "${PORTS[@]}"; do
    PID=$(lsof -ti:$port 2>/dev/null)
    if [ ! -z "$PID" ]; then
        echo "⚠️  Port $port is in use by process $PID. Attempting to free it..."
        kill -9 $PID 2>/dev/null || true
        sleep 1
    fi
done

# Check if model exists
if [ ! -d "models" ] || [ -z "$(ls -A models 2>/dev/null)" ]; then
    echo "⚠️  No trained model found. Training model first..."
    
    # Install Python dependencies if needed
    if [ ! -d "venv" ]; then
        echo "📦 Creating virtual environment..."
        python3 -m venv venv
        source venv/bin/activate
        pip install -r requirements.txt
    else
        source venv/bin/activate
    fi
    
    # Train the model
    echo "🤖 Training ML model..."
    python train_model_docker.py
    
    if [ $? -ne 0 ]; then
        echo "❌ Model training failed. Please check the logs."
        exit 1
    fi
    
    deactivate
fi

# Create directories
echo "📁 Creating directories..."
mkdir -p models mlflow logs

# Start system
echo "🚀 Starting all services..."
docker-compose up --build

echo "✅ System started successfully!"
echo ""
echo "🌐 Access points:"
echo "  - Dashboard: http://localhost:8501"
echo "  - API: http://localhost:8000"
echo "  - MLflow: http://localhost:5001"
echo ""
echo "Press Ctrl+C to stop all services"
