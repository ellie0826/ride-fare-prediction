import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import yaml
from kafka import KafkaProducer
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserDriverStreamService:
    """
    Simulates user ride requests and driver availability data
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
        
        # Initialize simulation state
        self.active_users = {}
        self.active_drivers = {}
        self.ride_requests = {}
        self.current_city = self.config['default_city']
        
        logger.info(f"UserDriverStreamService initialized for city: {self.current_city}")
    
    def generate_user_request(self) -> Dict:
        """
        Generate a simulated user ride request
        """
        city_config = self.config['cities'][self.current_city]
        bounds = city_config['bounds']
        
        # Generate random pickup and dropoff locations within city bounds
        pickup_lat = random.uniform(bounds['min_lat'], bounds['max_lat'])
        pickup_lon = random.uniform(bounds['min_lon'], bounds['max_lon'])
        dropoff_lat = random.uniform(bounds['min_lat'], bounds['max_lat'])
        dropoff_lon = random.uniform(bounds['min_lon'], bounds['max_lon'])
        
        # Generate user request
        user_id = f"user_{random.randint(1000, 9999)}"
        request_id = f"req_{int(time.time())}_{random.randint(100, 999)}"
        
        request = {
            'request_id': request_id,
            'user_id': user_id,
            'city': self.current_city,
            'pickup_latitude': pickup_lat,
            'pickup_longitude': pickup_lon,
            'dropoff_latitude': dropoff_lat,
            'dropoff_longitude': dropoff_lon,
            'passenger_count': random.choices([1, 2, 3, 4], weights=[0.6, 0.25, 0.1, 0.05])[0],
            'requested_at': datetime.now().isoformat(),
            'ride_type': random.choices(['standard', 'premium', 'shared'], weights=[0.7, 0.2, 0.1])[0],
            'status': 'requested'
        }
        
        self.ride_requests[request_id] = request
        return request
    
    def generate_driver_location(self) -> Dict:
        """
        Generate a simulated driver location update
        """
        city_config = self.config['cities'][self.current_city]
        bounds = city_config['bounds']
        
        # Generate driver data
        driver_id = f"driver_{random.randint(1000, 9999)}"
        
        # Simulate driver movement patterns (some clustering around traffic zones)
        if random.random() < 0.3 and 'traffic_zones' in city_config:
            # Driver in a traffic zone
            zone = random.choice(city_config['traffic_zones'])
            zone_bounds = zone['bounds']
            lat = random.uniform(zone_bounds[0], zone_bounds[1])
            lon = random.uniform(zone_bounds[2], zone_bounds[3])
        else:
            # Driver anywhere in the city
            lat = random.uniform(bounds['min_lat'], bounds['max_lat'])
            lon = random.uniform(bounds['min_lon'], bounds['max_lon'])
        
        driver_data = {
            'driver_id': driver_id,
            'city': self.current_city,
            'latitude': lat,
            'longitude': lon,
            'status': random.choices(['available', 'busy', 'offline'], weights=[0.6, 0.3, 0.1])[0],
            'rating': round(random.uniform(4.0, 5.0), 1),
            'vehicle_type': random.choices(['sedan', 'suv', 'compact'], weights=[0.6, 0.3, 0.1])[0],
            'timestamp': datetime.now().isoformat()
        }
        
        self.active_drivers[driver_id] = driver_data
        return driver_data
    
    def calculate_driver_density(self) -> Dict:
        """
        Calculate current driver density in different areas
        """
        city_config = self.config['cities'][self.current_city]
        available_drivers = [d for d in self.active_drivers.values() if d['status'] == 'available']
        
        density_data = {
            'city': self.current_city,
            'total_drivers': len(self.active_drivers),
            'available_drivers': len(available_drivers),
            'density_score': len(available_drivers) / max(len(self.ride_requests), 1),
            'timestamp': datetime.now().isoformat()
        }
        
        # Calculate zone-specific densities
        if 'traffic_zones' in city_config:
            zone_densities = {}
            for zone in city_config['traffic_zones']:
                zone_name = zone['name']
                zone_bounds = zone['bounds']
                
                # Count drivers in this zone
                drivers_in_zone = 0
                for driver in available_drivers:
                    if (zone_bounds[0] <= driver['latitude'] <= zone_bounds[1] and
                        zone_bounds[2] <= driver['longitude'] <= zone_bounds[3]):
                        drivers_in_zone += 1
                
                zone_densities[zone_name] = drivers_in_zone
            
            density_data['zone_densities'] = zone_densities
        
        return density_data
    
    def simulate_ride_matching(self) -> List[Dict]:
        """
        Simulate matching ride requests with drivers
        """
        matches = []
        available_drivers = [d for d in self.active_drivers.values() if d['status'] == 'available']
        pending_requests = [r for r in self.ride_requests.values() if r['status'] == 'requested']
        
        for request in pending_requests[:len(available_drivers)]:
            if available_drivers:
                driver = random.choice(available_drivers)
                available_drivers.remove(driver)
                
                # Calculate estimated time and distance (simplified)
                estimated_time = random.randint(3, 15)  # minutes
                estimated_distance = random.uniform(0.5, 20.0)  # km
                
                match = {
                    'match_id': f"match_{int(time.time())}_{random.randint(100, 999)}",
                    'request_id': request['request_id'],
                    'driver_id': driver['driver_id'],
                    'estimated_pickup_time': estimated_time,
                    'estimated_distance': estimated_distance,
                    'matched_at': datetime.now().isoformat(),
                    'status': 'matched'
                }
                
                # Update request and driver status
                request['status'] = 'matched'
                driver['status'] = 'busy'
                
                matches.append(match)
        
        return matches
    
    def run_simulation(self, duration_minutes: int = 60):
        """
        Run the user/driver simulation
        """
        logger.info(f"Starting user/driver simulation for {duration_minutes} minutes")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        while time.time() < end_time:
            try:
                # Generate user requests (every 10-30 seconds)
                if random.random() < 0.7:
                    user_request = self.generate_user_request()
                    self.producer.send('user_requests', value=user_request, key=user_request['request_id'])
                    logger.info(f"Generated user request: {user_request['request_id']}")
                
                # Generate driver location updates (every 5-15 seconds)
                if random.random() < 0.8:
                    driver_location = self.generate_driver_location()
                    self.producer.send('driver_locations', value=driver_location, key=driver_location['driver_id'])
                
                # Calculate and send driver density (every 60 seconds)
                if int(time.time()) % 60 == 0:
                    density_data = self.calculate_driver_density()
                    self.producer.send('driver_density', value=density_data, key=self.current_city)
                    logger.info(f"Driver density: {density_data['available_drivers']} available drivers")
                
                # Simulate ride matching (every 30 seconds)
                if int(time.time()) % 30 == 0:
                    matches = self.simulate_ride_matching()
                    for match in matches:
                        self.producer.send('ride_matches', value=match, key=match['match_id'])
                        logger.info(f"Ride matched: {match['match_id']}")
                
                # Clean up old data (every 5 minutes)
                if int(time.time()) % 300 == 0:
                    self._cleanup_old_data()
                
                # Wait before next iteration
                time.sleep(random.uniform(5, 15))
                
            except Exception as e:
                logger.error(f"Error in simulation loop: {e}")
                time.sleep(5)
        
        logger.info("User/driver simulation completed")
    
    def _cleanup_old_data(self):
        """
        Clean up old requests and driver data
        """
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(minutes=30)
        
        # Clean up old ride requests
        old_requests = [
            req_id for req_id, req in self.ride_requests.items()
            if datetime.fromisoformat(req['requested_at']) < cutoff_time
        ]
        for req_id in old_requests:
            del self.ride_requests[req_id]
        
        # Clean up old driver data
        old_drivers = [
            driver_id for driver_id, driver in self.active_drivers.items()
            if datetime.fromisoformat(driver['timestamp']) < cutoff_time
        ]
        for driver_id in old_drivers:
            del self.active_drivers[driver_id]
        
        if old_requests or old_drivers:
            logger.info(f"Cleaned up {len(old_requests)} old requests and {len(old_drivers)} old drivers")


def main():
    """
    Main entry point for the user/driver streaming service
    """
    service = UserDriverStreamService()
    
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
