import json
import logging
import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import joblib
import redis
import yaml
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from kafka import KafkaConsumer
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RideRequest(BaseModel):
    pickup_latitude: float
    pickup_longitude: float
    dropoff_latitude: float
    dropoff_longitude: float
    passenger_count: int
    city: str = "nyc"


class FarePrediction(BaseModel):
    predicted_fare: float
    base_fare: float
    distance_fare: float
    time_fare: float
    surge_multiplier: float
    weather_factor: float
    traffic_factor: float
    confidence_score: float
    prediction_timestamp: str


class PredictionService:
    """
    Real-time fare prediction service
    """
    
    def __init__(self):
        # Load configuration
        with open("config/cities.yaml", 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Redis for caching
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        try:
            self.redis_client = redis.from_url(redis_url)
            # Test connection
            self.redis_client.ping()
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Running without caching.")
            self.redis_client = None
        
        # Load ML model
        self.model = None
        self.feature_names = None
        self.load_model()
        
        # Initialize streaming data cache
        self.current_weather = {}
        self.current_traffic = {}
        self.driver_density = {}
        
        # Start Kafka consumers in background
        self.start_kafka_consumers()
        
        logger.info("PredictionService initialized")
    
    def load_model(self):
        """
        Load the trained ML model
        """
        try:
            # Try to load the latest model
            if os.path.exists("models"):
                model_files = [f for f in os.listdir("models") if f.endswith("_model.joblib")]
                if model_files:
                    # Get the most recent model
                    latest_model = max(model_files, key=lambda x: os.path.getctime(f"models/{x}"))
                    model_name = latest_model.replace("_model.joblib", "")
                    
                    self.model = joblib.load(f"models/{latest_model}")
                    self.feature_names = joblib.load(f"models/{model_name}_features.joblib")
                    
                    logger.info(f"Loaded model: {model_name}")
                    logger.info(f"Feature names: {len(self.feature_names)} features")
                else:
                    logger.warning("No trained model found. Please train a model first.")
            else:
                logger.warning("Models directory not found. Please train a model first.")
        except Exception as e:
            logger.error(f"Error loading model: {e}")
    
    def start_kafka_consumers(self):
        """
        Start Kafka consumers for streaming data
        """
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        # Weather consumer
        def consume_weather():
            try:
                consumer = KafkaConsumer(
                    'weather_updates',
                    bootstrap_servers=[kafka_servers],
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest'
                )
                
                for message in consumer:
                    weather_data = message.value
                    city = weather_data['city']
                    self.current_weather[city] = weather_data
                    
                    # Cache in Redis
                    if self.redis_client:
                        try:
                            self.redis_client.setex(
                                f"weather:{city}", 
                                300,  # 5 minutes TTL
                                json.dumps(weather_data)
                            )
                        except Exception as e:
                            logger.warning(f"Redis cache error: {e}")
                    
                    logger.debug(f"Updated weather for {city}: {weather_data['condition']}")
            except Exception as e:
                logger.error(f"Weather consumer error: {e}")
        
        # Traffic consumer
        def consume_traffic():
            try:
                consumer = KafkaConsumer(
                    'traffic_updates',
                    bootstrap_servers=[kafka_servers],
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest'
                )
                
                for message in consumer:
                    traffic_data = message.value
                    city = traffic_data['city']
                    zone = traffic_data['zone']
                    
                    if city not in self.current_traffic:
                        self.current_traffic[city] = {}
                    
                    self.current_traffic[city][zone] = traffic_data
                    
                    # Cache in Redis
                    if self.redis_client:
                        try:
                            self.redis_client.setex(
                                f"traffic:{city}:{zone}",
                                180,  # 3 minutes TTL
                                json.dumps(traffic_data)
                            )
                        except Exception as e:
                            logger.warning(f"Redis cache error: {e}")
                    
                    logger.debug(f"Updated traffic for {city}/{zone}: {traffic_data['traffic_level']}")
            except Exception as e:
                logger.error(f"Traffic consumer error: {e}")
        
        # Driver density consumer
        def consume_driver_density():
            try:
                consumer = KafkaConsumer(
                    'driver_density',
                    bootstrap_servers=[kafka_servers],
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest'
                )
                
                for message in consumer:
                    density_data = message.value
                    city = density_data['city']
                    self.driver_density[city] = density_data
                    
                    # Cache in Redis
                    if self.redis_client:
                        try:
                            self.redis_client.setex(
                                f"driver_density:{city}",
                                120,  # 2 minutes TTL
                                json.dumps(density_data)
                            )
                        except Exception as e:
                            logger.warning(f"Redis cache error: {e}")
                    
                    logger.debug(f"Updated driver density for {city}: {density_data['available_drivers']} drivers")
            except Exception as e:
                logger.error(f"Driver density consumer error: {e}")
        
        # Start consumers in separate threads
        try:
            threading.Thread(target=consume_weather, daemon=True).start()
            threading.Thread(target=consume_traffic, daemon=True).start()
            threading.Thread(target=consume_driver_density, daemon=True).start()
            logger.info("Kafka consumers started")
        except Exception as e:
            logger.warning(f"Failed to start Kafka consumers: {e}")
    
    def get_current_weather(self, city: str) -> Dict:
        """
        Get current weather data for a city
        """
        # Try memory cache first
        if city in self.current_weather:
            return self.current_weather[city]
        
        # Try Redis cache
        if self.redis_client:
            try:
                cached_data = self.redis_client.get(f"weather:{city}")
                if cached_data:
                    return json.loads(cached_data)
            except Exception as e:
                logger.error(f"Redis error: {e}")
        
        # Default weather if no data available
        return {
            'condition': 'clear',
            'temperature': 20.0,
            'weather_factor': 1.0
        }
    
    def get_current_traffic(self, city: str, pickup_lat: float, pickup_lon: float) -> Dict:
        """
        Get current traffic data for a location
        """
        city_config = self.config['cities'].get(city, self.config['cities']['nyc'])
        
        # Find the traffic zone for the pickup location
        if 'traffic_zones' in city_config:
            for zone in city_config['traffic_zones']:
                zone_bounds = zone['bounds']
                if (zone_bounds[0] <= pickup_lat <= zone_bounds[1] and
                    zone_bounds[2] <= pickup_lon <= zone_bounds[3]):
                    
                    zone_name = zone['name']
                    
                    # Try memory cache
                    if city in self.current_traffic and zone_name in self.current_traffic[city]:
                        return self.current_traffic[city][zone_name]
                    
                    # Try Redis cache
                    if self.redis_client:
                        try:
                            cached_data = self.redis_client.get(f"traffic:{city}:{zone_name}")
                            if cached_data:
                                return json.loads(cached_data)
                        except Exception as e:
                            logger.error(f"Redis error: {e}")
        
        # Default traffic if no data available
        return {
            'traffic_level': 'moderate',
            'traffic_factor': 1.15,
            'congestion_score': 0.5
        }
    
    def get_driver_density(self, city: str) -> Dict:
        """
        Get current driver density for a city
        """
        # Try memory cache first
        if city in self.driver_density:
            return self.driver_density[city]
        
        # Try Redis cache
        if self.redis_client:
            try:
                cached_data = self.redis_client.get(f"driver_density:{city}")
                if cached_data:
                    return json.loads(cached_data)
            except Exception as e:
                logger.error(f"Redis error: {e}")
        
        # Default density if no data available
        return {
            'available_drivers': 50,
            'density_score': 1.0
        }
    
    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between two points using Haversine formula
        """
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        # Earth radius in kilometers
        r = 6371
        return c * r
    
    def prepare_features(self, request: RideRequest) -> pd.DataFrame:
        """
        Prepare features for model prediction
        """
        current_time = datetime.now()
        
        # Get real-time data
        weather_data = self.get_current_weather(request.city)
        traffic_data = self.get_current_traffic(request.city, request.pickup_latitude, request.pickup_longitude)
        density_data = self.get_driver_density(request.city)
        
        # Calculate distance
        distance_km = self.calculate_distance(
            request.pickup_latitude, request.pickup_longitude,
            request.dropoff_latitude, request.dropoff_longitude
        )
        
        # Extract time features
        hour = current_time.hour
        day_of_week = current_time.weekday()
        month = current_time.month
        is_weekend = 1 if day_of_week >= 5 else 0
        is_rush_hour = 1 if (7 <= hour <= 9) or (17 <= hour <= 19) else 0
        
        # Create time of day dummies
        if 0 <= hour < 6:
            time_category = 'night'
        elif 6 <= hour < 12:
            time_category = 'morning'
        elif 12 <= hour < 18:
            time_category = 'afternoon'
        else:
            time_category = 'evening'
        
        # Prepare feature dictionary
        features = {
            'pickup_latitude': request.pickup_latitude,
            'pickup_longitude': request.pickup_longitude,
            'dropoff_latitude': request.dropoff_latitude,
            'dropoff_longitude': request.dropoff_longitude,
            'passenger_count': request.passenger_count,
            'distance_km': distance_km,
            'hour': hour,
            'day_of_week': day_of_week,
            'month': month,
            'is_weekend': is_weekend,
            'is_rush_hour': is_rush_hour,
            'traffic_factor': traffic_data.get('traffic_factor', 1.15),
            'weather_factor': weather_data.get('weather_factor', 1.0),
            'driver_density': density_data.get('density_score', 1.0),
            'time_afternoon': 1 if time_category == 'afternoon' else 0,
            'time_evening': 1 if time_category == 'evening' else 0,
            'time_morning': 1 if time_category == 'morning' else 0,
            'time_night': 1 if time_category == 'night' else 0
        }
        
        # Create DataFrame with correct feature order
        if self.feature_names:
            # Ensure all features are present
            for feature in self.feature_names:
                if feature not in features:
                    features[feature] = 0
            
            # Create DataFrame with correct column order
            df = pd.DataFrame([features])[self.feature_names]
        else:
            df = pd.DataFrame([features])
        
        return df, weather_data, traffic_data, density_data
    
    def predict_fare(self, request: RideRequest) -> FarePrediction:
        """
        Predict fare for a ride request
        """
        try:
            # Prepare features
            features_df, weather_data, traffic_data, density_data = self.prepare_features(request)
            
            # Calculate fare components
            city_config = self.config['cities'].get(request.city, self.config['cities']['nyc'])
            
            distance_km = features_df['distance_km'].iloc[0]
            base_fare = city_config['base_fare']
            distance_fare = distance_km * city_config['per_mile_rate'] * 0.621371  # Convert km to miles
            
            # Estimate time based on distance and traffic
            avg_speed = traffic_data.get('average_speed_kmh', 30)
            estimated_time_minutes = (distance_km / avg_speed) * 60
            time_fare = estimated_time_minutes * city_config['per_minute_rate']
            
            # Calculate surge multiplier based on driver density
            density_score = density_data.get('density_score', 1.0)
            surge_range = city_config['surge_multiplier_range']
            if density_score < 0.5:
                surge_multiplier = surge_range[1]  # High surge when low density
            elif density_score > 2.0:
                surge_multiplier = surge_range[0]  # Low surge when high density
            else:
                # Linear interpolation
                surge_multiplier = surge_range[1] - (density_score - 0.5) * (surge_range[1] - surge_range[0]) / 1.5
            
            # Apply factors
            weather_factor = weather_data.get('weather_factor', 1.0)
            traffic_factor = traffic_data.get('traffic_factor', 1.0)
            
            # Make prediction
            if self.model is not None:
                # Use ML model if available
                predicted_fare = self.model.predict(features_df)[0]
                confidence_score = min(0.95, max(0.6, 
                    0.8 + 0.1 * (1 - abs(predicted_fare - (base_fare + distance_fare + time_fare)) / predicted_fare)
                ))
            else:
                # Fallback: rule-based prediction
                logger.warning("Using fallback prediction - ML model not available")
                predicted_fare = (base_fare + distance_fare + time_fare) * surge_multiplier * weather_factor * traffic_factor
                confidence_score = 0.7  # Lower confidence for rule-based prediction
            
            return FarePrediction(
                predicted_fare=round(max(predicted_fare, base_fare), 2),
                base_fare=round(base_fare, 2),
                distance_fare=round(distance_fare, 2),
                time_fare=round(time_fare, 2),
                surge_multiplier=round(surge_multiplier, 2),
                weather_factor=round(weather_factor, 2),
                traffic_factor=round(traffic_factor, 2),
                confidence_score=round(confidence_score, 2),
                prediction_timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


# Initialize FastAPI app
app = FastAPI(title="Ride Fare Prediction API", version="1.0.0")
prediction_service = PredictionService()


@app.get("/")
async def root():
    return {"message": "Ride Fare Prediction API", "status": "running"}


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "model_loaded": prediction_service.model is not None,
        "redis_connected": prediction_service.redis_client is not None,
        "timestamp": datetime.now().isoformat()
    }


@app.post("/predict", response_model=FarePrediction)
async def predict_fare(request: RideRequest):
    """
    Predict fare for a ride request
    """
    return prediction_service.predict_fare(request)


@app.get("/current-conditions/{city}")
async def get_current_conditions(city: str):
    """
    Get current weather, traffic, and driver conditions for a city
    """
    return {
        "city": city,
        "weather": prediction_service.get_current_weather(city),
        "driver_density": prediction_service.get_driver_density(city),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/cities")
async def get_supported_cities():
    """
    Get list of supported cities
    """
    return {
        "cities": list(prediction_service.config['cities'].keys()),
        "default_city": prediction_service.config['default_city']
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
