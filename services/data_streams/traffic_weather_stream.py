import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import yaml
from kafka import KafkaProducer, KafkaConsumer
import redis
import os
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TrafficWeatherStreamService:
    """
    Simulates traffic and weather data streams
    """
    
    def __init__(self, config_path: str = "config/cities.yaml"):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Kafka producer
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Initialize Redis for accessing driver data
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        try:
            self.redis_client = redis.from_url(redis_url)
            self.redis_client.ping()  # Test connection
            logger.info("Connected to Redis for driver data access")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Traffic will use time-based patterns only.")
            self.redis_client = None
        
        # Initialize simulation state
        self.current_city = self.config['default_city']
        self.weather_state = self._initialize_weather_state()
        self.traffic_state = self._initialize_traffic_state()
        
        logger.info(f"TrafficWeatherStreamService initialized for city: {self.current_city}")
    
    def _initialize_weather_state(self) -> Dict:
        """
        Initialize weather state with realistic patterns
        """
        current_hour = datetime.now().hour
        current_month = datetime.now().month
        
        # Base weather on season and time
        if current_month in [12, 1, 2]:  # Winter
            base_conditions = ['clear', 'light_rain', 'snow']
            base_weights = [0.4, 0.3, 0.3]
        elif current_month in [6, 7, 8]:  # Summer
            base_conditions = ['clear', 'light_rain']
            base_weights = [0.8, 0.2]
        else:  # Spring/Fall
            base_conditions = ['clear', 'light_rain', 'moderate_rain']
            base_weights = [0.6, 0.3, 0.1]
        
        return {
            'current_condition': random.choices(base_conditions, weights=base_weights)[0],
            'temperature': self._generate_temperature(),
            'humidity': random.uniform(30, 80),
            'wind_speed': random.uniform(0, 15),
            'visibility': random.uniform(5, 15),
            'last_updated': datetime.now()
        }
    
    def _initialize_traffic_state(self) -> Dict:
        """
        Initialize traffic state for different zones
        """
        city_config = self.config['cities'][self.current_city]
        traffic_state = {}
        
        if 'traffic_zones' in city_config:
            for zone in city_config['traffic_zones']:
                zone_name = zone['name']
                traffic_state[zone_name] = {
                    'current_level': 'moderate',
                    'congestion_score': random.uniform(0.3, 0.7),
                    'average_speed': random.uniform(25, 45),
                    'incident_count': 0,
                    'last_updated': datetime.now()
                }
        
        return traffic_state
    
    def _generate_temperature(self) -> float:
        """
        Generate realistic temperature based on season and time
        """
        current_month = datetime.now().month
        current_hour = datetime.now().hour
        
        # Base temperature by season (Celsius)
        if current_month in [12, 1, 2]:  # Winter
            base_temp = random.uniform(-5, 10)
        elif current_month in [6, 7, 8]:  # Summer
            base_temp = random.uniform(20, 35)
        elif current_month in [3, 4, 5]:  # Spring
            base_temp = random.uniform(10, 25)
        else:  # Fall
            base_temp = random.uniform(5, 20)
        
        # Daily temperature variation
        daily_variation = 5 * math.sin((current_hour - 6) * math.pi / 12)
        
        return round(base_temp + daily_variation, 1)
    
    def generate_weather_update(self) -> Dict:
        """
        Generate weather update with realistic transitions
        """
        # Weather transitions (simplified Markov chain)
        current_condition = self.weather_state['current_condition']
        
        transition_probs = {
            'clear': {'clear': 0.8, 'light_rain': 0.15, 'moderate_rain': 0.05},
            'light_rain': {'clear': 0.3, 'light_rain': 0.5, 'moderate_rain': 0.15, 'heavy_rain': 0.05},
            'moderate_rain': {'light_rain': 0.4, 'moderate_rain': 0.4, 'heavy_rain': 0.15, 'storm': 0.05},
            'heavy_rain': {'moderate_rain': 0.5, 'heavy_rain': 0.3, 'storm': 0.2},
            'snow': {'clear': 0.2, 'snow': 0.7, 'heavy_rain': 0.1},
            'storm': {'heavy_rain': 0.6, 'moderate_rain': 0.3, 'storm': 0.1}
        }
        
        if current_condition in transition_probs:
            conditions = list(transition_probs[current_condition].keys())
            weights = list(transition_probs[current_condition].values())
            new_condition = random.choices(conditions, weights=weights)[0]
        else:
            new_condition = current_condition
        
        # Update weather state
        self.weather_state['current_condition'] = new_condition
        self.weather_state['temperature'] = self._generate_temperature()
        self.weather_state['humidity'] = max(20, min(100, 
            self.weather_state['humidity'] + random.uniform(-5, 5)))
        self.weather_state['wind_speed'] = max(0, min(30,
            self.weather_state['wind_speed'] + random.uniform(-2, 2)))
        self.weather_state['last_updated'] = datetime.now()
        
        # Create weather update message
        weather_update = {
            'city': self.current_city,
            'condition': new_condition,
            'temperature': self.weather_state['temperature'],
            'humidity': self.weather_state['humidity'],
            'wind_speed': self.weather_state['wind_speed'],
            'visibility': self._calculate_visibility(new_condition),
            'weather_factor': self.config['weather_factors'].get(new_condition, 1.0),
            'timestamp': datetime.now().isoformat()
        }
        
        return weather_update
    
    def _calculate_visibility(self, condition: str) -> float:
        """
        Calculate visibility based on weather condition
        """
        visibility_map = {
            'clear': random.uniform(12, 15),
            'light_rain': random.uniform(8, 12),
            'moderate_rain': random.uniform(5, 8),
            'heavy_rain': random.uniform(2, 5),
            'snow': random.uniform(3, 7),
            'storm': random.uniform(1, 3)
        }
        
        return round(visibility_map.get(condition, 10), 1)
    
    def generate_traffic_update(self) -> List[Dict]:
        """
        Generate traffic updates for all zones
        """
        city_config = self.config['cities'][self.current_city]
        traffic_updates = []
        
        if 'traffic_zones' not in city_config:
            return traffic_updates
        
        current_hour = datetime.now().hour
        is_weekend = datetime.now().weekday() >= 5
        
        for zone in city_config['traffic_zones']:
            zone_name = zone['name']
            zone_traffic_factor = zone.get('traffic_factor', 1.0)
            
            # Calculate traffic level dynamically based on current driver density
            traffic_level = self._calculate_traffic_level(current_hour, is_weekend, zone_traffic_factor, zone_name)
            
            # Update traffic state
            if zone_name in self.traffic_state:
                current_state = self.traffic_state[zone_name]
                
                # Smooth transitions
                current_score = current_state['congestion_score']
                target_score = self._traffic_level_to_score(traffic_level)
                new_score = current_score + (target_score - current_score) * 0.3
                
                self.traffic_state[zone_name].update({
                    'current_level': traffic_level,
                    'congestion_score': new_score,
                    'average_speed': self._calculate_average_speed(new_score),
                    'last_updated': datetime.now()
                })
            
            # Create traffic update message
            traffic_update = {
                'city': self.current_city,
                'zone': zone_name,
                'traffic_level': traffic_level,
                'congestion_score': round(self.traffic_state[zone_name]['congestion_score'], 2),
                'average_speed_kmh': round(self.traffic_state[zone_name]['average_speed'], 1),
                'incident_count': self.traffic_state[zone_name]['incident_count'],
                'traffic_factor': self.config['traffic_factors'].get(traffic_level, 1.0),
                'timestamp': datetime.now().isoformat()
            }
            
            traffic_updates.append(traffic_update)
        
        return traffic_updates
    
    def _calculate_traffic_level(self, hour: int, is_weekend: bool, zone_factor: float, zone_name: str = None) -> str:
        """
        Calculate traffic level dynamically based on current driver density in the zone
        """
        # Try to get real-time driver data if Redis is available
        if self.redis_client and zone_name:
            try:
                driver_density_ratio = self._get_zone_driver_density(zone_name)
                if driver_density_ratio is not None:
                    return self._driver_density_to_traffic_level(driver_density_ratio, hour, is_weekend)
            except Exception as e:
                logger.debug(f"Failed to get driver density for {zone_name}: {e}")
        
        # Fallback to time-based patterns if Redis unavailable or no driver data
        return self._calculate_time_based_traffic_level(hour, is_weekend, zone_factor)
    
    def _get_zone_driver_density(self, zone_name: str) -> float:
        """
        Get current driver density ratio for a specific zone from Redis
        """
        try:
            # Get zone bounds from config
            city_config = self.config['cities'][self.current_city]
            zone_bounds = None
            
            for zone in city_config.get('traffic_zones', []):
                if zone['name'] == zone_name:
                    zone_bounds = zone['bounds']
                    break
            
            if not zone_bounds:
                return None
            
            # Get all available drivers from Redis
            driver_keys = self.redis_client.keys(f"driver_location:{self.current_city}:*")
            if not driver_keys:
                return None
            
            total_drivers = 0
            zone_drivers = 0
            
            for key in driver_keys:
                driver_data = self.redis_client.get(key)
                if driver_data:
                    driver = json.loads(driver_data)
                    if driver.get('status') == 'available':
                        total_drivers += 1
                        
                        # Check if driver is in this zone
                        driver_lat = driver.get('latitude', 0)
                        driver_lon = driver.get('longitude', 0)
                        
                        if (zone_bounds[0] <= driver_lat <= zone_bounds[1] and
                            zone_bounds[2] <= driver_lon <= zone_bounds[3]):
                            zone_drivers += 1
            
            if total_drivers == 0:
                return None
            
            # Calculate density ratio (drivers in zone / total drivers)
            density_ratio = zone_drivers / total_drivers
            logger.debug(f"Zone {zone_name}: {zone_drivers}/{total_drivers} drivers, ratio: {density_ratio:.3f}")
            
            return density_ratio
            
        except Exception as e:
            logger.error(f"Error calculating driver density for {zone_name}: {e}")
            return None
    
    def _driver_density_to_traffic_level(self, density_ratio: float, hour: int, is_weekend: bool) -> str:
        """
        Convert driver density ratio to traffic level
        Logic: More drivers = less traffic congestion (inverse relationship)
        """
        # Base traffic level on driver density
        # High density = low traffic, Low density = high traffic
        
        if density_ratio >= 0.20:  # Very high driver density (20%+ of all drivers in this zone)
            traffic_level = 'low'
        elif density_ratio >= 0.12:  # High driver density (12-20%)
            traffic_level = 'moderate'  
        elif density_ratio >= 0.06:  # Medium driver density (6-12%)
            traffic_level = 'high'
        else:  # Low driver density (<6%)
            traffic_level = 'severe'
        
        # Apply time-based adjustments
        rush_hour_multiplier = 1.0
        if not is_weekend and (7 <= hour <= 9 or 17 <= hour <= 19):
            rush_hour_multiplier = 1.3  # Increase congestion during rush hours
        elif 22 <= hour <= 5:
            rush_hour_multiplier = 0.7  # Decrease congestion late night/early morning
        
        # Adjust traffic level based on time
        if rush_hour_multiplier > 1.2:
            # Upgrade traffic level during rush hours
            level_upgrade = {
                'low': 'moderate',
                'moderate': 'high', 
                'high': 'severe',
                'severe': 'severe'
            }
            traffic_level = level_upgrade.get(traffic_level, traffic_level)
        elif rush_hour_multiplier < 0.8:
            # Downgrade traffic level during quiet hours
            level_downgrade = {
                'severe': 'high',
                'high': 'moderate',
                'moderate': 'low',
                'low': 'low'
            }
            traffic_level = level_downgrade.get(traffic_level, traffic_level)
        
        logger.debug(f"Driver density {density_ratio:.3f} → {traffic_level} traffic (rush_multiplier: {rush_hour_multiplier})")
        return traffic_level
    
    def _calculate_time_based_traffic_level(self, hour: int, is_weekend: bool, zone_factor: float) -> str:
        """
        Fallback method: Calculate traffic level based on time patterns only
        """
        # Base traffic probability on hour and day
        if not is_weekend:
            if 7 <= hour <= 9 or 17 <= hour <= 19:  # Rush hours
                base_probs = [0.1, 0.3, 0.4, 0.2]  # low, moderate, high, severe
            elif 10 <= hour <= 16:  # Business hours
                base_probs = [0.3, 0.4, 0.2, 0.1]
            elif 20 <= hour <= 23:  # Evening
                base_probs = [0.4, 0.4, 0.15, 0.05]
            else:  # Night/early morning
                base_probs = [0.7, 0.25, 0.05, 0.0]
        else:  # Weekend
            if 12 <= hour <= 18:  # Weekend afternoon
                base_probs = [0.2, 0.4, 0.3, 0.1]
            elif 19 <= hour <= 22:  # Weekend evening
                base_probs = [0.3, 0.4, 0.2, 0.1]
            else:
                base_probs = [0.6, 0.3, 0.1, 0.0]
        
        # Adjust probabilities based on zone traffic factor
        if zone_factor > 1.2:  # High traffic zone
            base_probs = [p * 0.7 for p in base_probs[:2]] + [p * 1.3 for p in base_probs[2:]]
        elif zone_factor < 1.1:  # Low traffic zone
            base_probs = [p * 1.3 for p in base_probs[:2]] + [p * 0.7 for p in base_probs[2:]]
        
        # Normalize probabilities
        total = sum(base_probs)
        base_probs = [p / total for p in base_probs]
        
        return random.choices(['low', 'moderate', 'high', 'severe'], weights=base_probs)[0]
    
    def _traffic_level_to_score(self, level: str) -> float:
        """
        Convert traffic level to congestion score
        """
        level_scores = {
            'low': random.uniform(0.1, 0.3),
            'moderate': random.uniform(0.3, 0.6),
            'high': random.uniform(0.6, 0.8),
            'severe': random.uniform(0.8, 1.0)
        }
        return level_scores.get(level, 0.5)
    
    def _calculate_average_speed(self, congestion_score: float) -> float:
        """
        Calculate average speed based on congestion score
        """
        # Inverse relationship: higher congestion = lower speed
        max_speed = 60  # km/h
        min_speed = 10  # km/h
        
        speed = max_speed - (congestion_score * (max_speed - min_speed))
        return max(min_speed, speed)
    
    def generate_traffic_incidents(self) -> List[Dict]:
        """
        Generate random traffic incidents
        """
        incidents = []
        
        # Random chance of incidents (higher during rush hours)
        current_hour = datetime.now().hour
        incident_probability = 0.1
        
        if 7 <= current_hour <= 9 or 17 <= current_hour <= 19:
            incident_probability = 0.3
        
        if random.random() < incident_probability:
            city_config = self.config['cities'][self.current_city]
            
            if 'traffic_zones' in city_config:
                zone = random.choice(city_config['traffic_zones'])
                zone_bounds = zone['bounds']
                
                incident = {
                    'incident_id': f"incident_{int(time.time())}_{random.randint(100, 999)}",
                    'city': self.current_city,
                    'zone': zone['name'],
                    'latitude': random.uniform(zone_bounds[0], zone_bounds[1]),
                    'longitude': random.uniform(zone_bounds[2], zone_bounds[3]),
                    'type': random.choice(['accident', 'construction', 'road_closure', 'weather_related']),
                    'severity': random.choices(['minor', 'moderate', 'major'], weights=[0.6, 0.3, 0.1])[0],
                    'estimated_duration': random.randint(15, 120),  # minutes
                    'reported_at': datetime.now().isoformat()
                }
                
                incidents.append(incident)
                
                # Update incident count for the zone
                if zone['name'] in self.traffic_state:
                    self.traffic_state[zone['name']]['incident_count'] += 1
        
        return incidents
    
    def run_simulation(self, duration_minutes: int = 60):
        """
        Run the traffic/weather simulation
        """
        logger.info(f"Starting traffic/weather simulation for {duration_minutes} minutes")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        while time.time() < end_time:
            try:
                # Generate weather updates (every 2-5 minutes)
                if int(time.time()) % random.randint(120, 300) == 0:
                    weather_update = self.generate_weather_update()
                    self.producer.send('weather_updates', value=weather_update, key=self.current_city)
                    logger.info(f"Weather update: {weather_update['condition']} at {weather_update['temperature']}°C")
                
                # Generate traffic updates (every 1-3 minutes)
                if int(time.time()) % random.randint(60, 180) == 0:
                    traffic_updates = self.generate_traffic_update()
                    for update in traffic_updates:
                        self.producer.send('traffic_updates', value=update, key=f"{self.current_city}_{update['zone']}")
                    
                    if traffic_updates:
                        avg_congestion = sum(u['congestion_score'] for u in traffic_updates) / len(traffic_updates)
                        logger.info(f"Traffic updates: {len(traffic_updates)} zones, avg congestion: {avg_congestion:.2f}")
                
                # Generate traffic incidents (random)
                incidents = self.generate_traffic_incidents()
                for incident in incidents:
                    self.producer.send('traffic_incidents', value=incident, key=incident['incident_id'])
                    logger.info(f"Traffic incident: {incident['type']} in {incident['zone']}")
                
                # Wait before next iteration
                time.sleep(random.uniform(30, 90))
                
            except Exception as e:
                logger.error(f"Error in simulation loop: {e}")
                time.sleep(5)
        
        logger.info("Traffic/weather simulation completed")


def main():
    """
    Main entry point for the traffic/weather streaming service
    """
    service = TrafficWeatherStreamService()
    
    try:
        # Run simulation indefinitely (or until interrupted)
        while True:
            service.run_simulation(duration_minutes=60)
            logger.info("Restarting simulation...")
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user")
    except Exception as e:
        logger.error(f"Simulation error: {e}")
    finally:
        service.producer.close()


if __name__ == "__main__":
    main()
