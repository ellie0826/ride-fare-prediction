# 🚗 Ride Fare Prediction MLOps System

A comprehensive, cloud-ready, real-time ride fare prediction system with live streaming data capabilities, built with modern MLOps practices.

## 🏗️ System Architecture

### Core Components

1. **🤖 ML Pipeline**
   - Data preprocessing with synthetic traffic/weather features
   - XGBoost/Random Forest model training
   - Model registry with MLflow
   - Feature engineering and validation

2. **📡 Real-time Data Streaming**
   - **User/Driver Service**: Simulates ride requests and driver availability
   - **Traffic/Weather Service**: Real-time traffic conditions and weather data
   - Kafka-based event streaming architecture
   - Redis caching for fast data access

3. **🔮 Prediction Service**
   - FastAPI-based REST API
   - Real-time fare prediction
   - Multi-factor pricing (surge, weather, traffic)
   - Confidence scoring

4. **📊 Frontend Dashboard**
   - Interactive Streamlit web interface
   - Real-time condition monitoring
   - Fare prediction interface
   - Analytics and insights

5. **🐳 Infrastructure**
   - Docker containerization
   - Multi-city configuration support
   - Scalable microservices architecture
   - Production-ready deployment

### 🔄 Data Flow Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Simulated     │    │    Kafka     │    │   Prediction    │
│ Users & Drivers │───▶│   Streams    │───▶│    Service      │
└─────────────────┘    └──────────────┘    └─────────────────┘
                              │                       │
┌─────────────────┐           │              ┌─────────────────┐
│ Traffic/Weather │───────────┘              │   Streamlit     │
│   Simulation    │                          │   Dashboard     │
└─────────────────┘                          └─────────────────┘
                                                      │
┌─────────────────┐    ┌──────────────┐              │
│   ML Training   │    │   MLflow     │              │
│    Pipeline     │───▶│   Registry   │──────────────┘
└─────────────────┘    └──────────────┘
```

## 🚀 Quick Start

### Option 1: Automated Setup (Recommended)
```bash
# Clone the repository
git clone https://github.com/ellie0826/ride-fare-prediction.git
cd ride-fare-prediction

# Run the automated setup script
./start_system.sh
```

### Option 2: Manual Setup
```bash
# 1. Train the ML model (Docker-based - no local dependencies needed)
python train_model_docker.py

# 2. Start all services
docker-compose up --build
```

### Option 3: Local Development Setup
```bash
# 1. Install Python dependencies
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# 2. Train the ML model locally
python train_model.py

# 3. Start all services
docker-compose up --build
```

## 🌐 Access Points

Once the system is running, access these endpoints:

- **📊 Dashboard**: http://localhost:8501
- **🔮 API**: http://localhost:8000
- **📈 MLflow**: http://localhost:5000
- **📚 API Docs**: http://localhost:8000/docs

## 🏙️ Multi-City Support

The system supports multiple cities with configurable parameters:

### Supported Cities
- **🗽 New York City** (default)
- **🌉 San Francisco**
- **🏢 Chicago**
- **➕ Custom regions** (configurable)

### City Configuration
Each city includes:
- Geographic boundaries
- Base fare rates
- Traffic zones with multipliers
- Surge pricing ranges
- Weather impact factors

## 📋 Features

### ✅ Core Features
- **Real-time fare prediction** with ML models
- **Multi-city support** with city-specific pricing
- **Live traffic simulation** based on time patterns
- **Weather impact modeling** with seasonal variations
- **Driver density tracking** for surge pricing
- **Model versioning** with MLflow
- **Interactive dashboard** with real-time updates

### ✅ Technical Features
- **Microservices architecture** with Docker
- **Event-driven streaming** with Kafka
- **Fast caching** with Redis
- **RESTful API** with FastAPI
- **Model monitoring** and metrics
- **Cloud deployment ready**
- **Horizontal scaling support**

## 🛠️ System Components

### ML Pipeline
- **Data Preprocessor**: Cleans and engineers features from Uber dataset
- **Model Trainer**: Trains XGBoost/Random Forest models
- **Feature Engineering**: Distance calculation, time features, synthetic traffic/weather
- **Model Registry**: MLflow for model versioning and tracking

### Streaming Services
- **User/Driver Stream**: Simulates ride requests and driver locations
- **Traffic/Weather Stream**: Generates realistic traffic and weather conditions
- **Kafka Topics**: `user_requests`, `driver_locations`, `traffic_updates`, `weather_updates`

### Prediction Service
- **FastAPI Application**: REST API for fare predictions
- **Real-time Processing**: Consumes Kafka streams for live data
- **Multi-factor Pricing**: Considers base fare, distance, time, surge, weather, traffic
- **Caching Layer**: Redis for fast data retrieval

### Frontend Dashboard
- **Interactive Interface**: Streamlit-based web application
- **Real-time Monitoring**: Live conditions and system status
- **Prediction Interface**: Easy-to-use fare estimation
- **Analytics Dashboard**: Historical trends and insights

## 📊 Data Sources

### Primary Dataset
- **Uber Fare Dataset**: 200,000 NYC taxi rides
- **Features**: Pickup/dropoff coordinates, datetime, passenger count, fare amount

### Synthetic Features
- **Traffic Data**: Time-based traffic patterns with zone-specific factors
- **Weather Data**: Seasonal weather conditions with pricing impact
- **Driver Density**: Simulated driver availability for surge pricing

## 🔧 Configuration

### Environment Variables
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_URL=redis://localhost:6379
MLFLOW_TRACKING_URI=http://localhost:5000
```

### City Configuration (`config/cities.yaml`)
```yaml
cities:
  nyc:
    name: "New York City"
    bounds: { min_lat: 40.4774, max_lat: 40.9176, ... }
    base_fare: 2.50
    per_mile_rate: 1.75
    traffic_zones: [...]
```

## 🧪 Development

### Running Tests
```bash
pytest tests/
```

### Code Formatting
```bash
black .
flake8 .
```

### Adding New Cities
1. Update `config/cities.yaml` with city parameters
2. Add geographic boundaries and pricing rules
3. Configure traffic zones and weather patterns

## 📈 Monitoring & Observability

### MLflow Tracking
- Model performance metrics
- Feature importance analysis
- Model versioning and comparison
- Experiment tracking

### System Metrics
- API response times
- Prediction accuracy
- Stream processing rates
- Cache hit rates

## 🚀 Deployment

### Local Development
```bash
docker-compose up
```

### Production Deployment
- **Kubernetes**: Use provided Helm charts
- **Cloud Platforms**: AWS ECS, Google Cloud Run, Azure Container Instances
- **Monitoring**: Prometheus + Grafana integration
- **Scaling**: Horizontal pod autoscaling based on load

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Uber for the public dataset
- Open source community for the amazing tools
- MLOps best practices from industry leaders

---

**Built with ❤️ using**: Python, FastAPI, Streamlit, Kafka, Redis, MLflow, Docker, XGBoost, Plotly
