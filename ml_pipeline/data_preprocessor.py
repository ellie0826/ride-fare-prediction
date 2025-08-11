import pandas as pd
import numpy as np
from datetime import datetime
import yaml
from typing import Tuple, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataPreprocessor:
    """
    Preprocesses the Uber dataset and adds synthetic traffic and weather features
    """
    
    def __init__(self, config_path: str = "config/cities.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
    def load_and_preprocess_data(self, data_path: str) -> pd.DataFrame:
        """
        Load the Uber dataset and add synthetic features
        """
        logger.info(f"Loading data from {data_path}")
        
        # Load the dataset
        df = pd.read_csv(data_path)
        
        # Basic data cleaning
        df = self._clean_data(df)
        
        # Feature engineering
        df = self._engineer_features(df)
        
        # Add synthetic traffic and weather data
        df = self._add_synthetic_features(df)
        
        logger.info(f"Preprocessed dataset shape: {df.shape}")
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the dataset by removing outliers and invalid data
        """
        logger.info("Cleaning data...")
        
        # Remove rows with missing values in critical columns
        critical_columns = ['fare_amount', 'pickup_latitude', 'pickup_longitude', 
                           'dropoff_latitude', 'dropoff_longitude', 'passenger_count']
        df = df.dropna(subset=critical_columns)
        
        # Remove fare outliers (negative fares, extremely high fares)
        df = df[(df['fare_amount'] > 0) & (df['fare_amount'] < 200)]
        
        # Remove coordinate outliers (focus on NYC area for now)
        nyc_bounds = self.config['cities']['nyc']['bounds']
        df = df[
            (df['pickup_latitude'] >= nyc_bounds['min_lat']) &
            (df['pickup_latitude'] <= nyc_bounds['max_lat']) &
            (df['pickup_longitude'] >= nyc_bounds['min_lon']) &
            (df['pickup_longitude'] <= nyc_bounds['max_lon']) &
            (df['dropoff_latitude'] >= nyc_bounds['min_lat']) &
            (df['dropoff_latitude'] <= nyc_bounds['max_lat']) &
            (df['dropoff_longitude'] >= nyc_bounds['min_lon']) &
            (df['dropoff_longitude'] <= nyc_bounds['max_lon'])
        ]
        
        # Remove passenger count outliers
        df = df[(df['passenger_count'] > 0) & (df['passenger_count'] <= 6)]
        
        logger.info(f"Data cleaned. Remaining rows: {len(df)}")
        return df
    
    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer features from existing data
        """
        logger.info("Engineering features...")
        
        # Convert pickup_datetime to datetime
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        
        # Extract time-based features
        df['hour'] = df['pickup_datetime'].dt.hour
        df['day_of_week'] = df['pickup_datetime'].dt.dayofweek
        df['month'] = df['pickup_datetime'].dt.month
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # Calculate distance using Haversine formula
        df['distance_km'] = self._calculate_distance(
            df['pickup_latitude'], df['pickup_longitude'],
            df['dropoff_latitude'], df['dropoff_longitude']
        )
        
        # Create rush hour indicator
        df['is_rush_hour'] = (
            ((df['hour'] >= 7) & (df['hour'] <= 9)) |
            ((df['hour'] >= 17) & (df['hour'] <= 19))
        ).astype(int)
        
        # Create time of day categories
        df['time_of_day'] = pd.cut(
            df['hour'], 
            bins=[0, 6, 12, 18, 24], 
            labels=['night', 'morning', 'afternoon', 'evening'],
            include_lowest=True
        )
        
        return df
    
    def _calculate_distance(self, lat1, lon1, lat2, lon2):
        """
        Calculate the great circle distance between two points on earth
        """
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        # Radius of earth in kilometers
        r = 6371
        return c * r
    
    def _add_synthetic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add synthetic traffic and weather features for training
        """
        logger.info("Adding synthetic traffic and weather features...")
        
        np.random.seed(42)  # For reproducibility
        
        # Traffic level based on time and location
        df['traffic_level'] = self._generate_traffic_level(df)
        
        # Weather conditions
        df['weather_condition'] = self._generate_weather_condition(df)
        
        # Driver density (affects surge pricing)
        df['driver_density'] = self._generate_driver_density(df)
        
        # Convert categorical features to numerical
        weather_mapping = {
            'clear': 1.0,
            'light_rain': 1.1,
            'moderate_rain': 1.25,
            'heavy_rain': 1.5,
            'snow': 1.4,
            'storm': 1.8
        }
        df['weather_factor'] = df['weather_condition'].map(weather_mapping)
        
        traffic_mapping = {
            'low': 1.0,
            'moderate': 1.15,
            'high': 1.35,
            'severe': 1.6
        }
        df['traffic_factor'] = df['traffic_level'].map(traffic_mapping)
        
        return df
    
    def _generate_traffic_level(self, df: pd.DataFrame) -> pd.Series:
        """
        Generate synthetic traffic levels based on time and location patterns
        """
        traffic_levels = []
        
        for _, row in df.iterrows():
            # Base traffic probability on hour and day
            hour = row['hour']
            is_weekend = row['is_weekend']
            
            # Higher traffic during rush hours on weekdays
            if not is_weekend and (7 <= hour <= 9 or 17 <= hour <= 19):
                traffic_probs = [0.1, 0.3, 0.4, 0.2]  # low, moderate, high, severe
            elif not is_weekend and (10 <= hour <= 16):
                traffic_probs = [0.3, 0.4, 0.2, 0.1]
            elif is_weekend and (12 <= hour <= 18):
                traffic_probs = [0.2, 0.4, 0.3, 0.1]
            else:
                traffic_probs = [0.6, 0.3, 0.1, 0.0]
            
            traffic_level = np.random.choice(
                ['low', 'moderate', 'high', 'severe'], 
                p=traffic_probs
            )
            traffic_levels.append(traffic_level)
        
        return pd.Series(traffic_levels)
    
    def _generate_weather_condition(self, df: pd.DataFrame) -> pd.Series:
        """
        Generate synthetic weather conditions with seasonal patterns
        """
        weather_conditions = []
        
        for _, row in df.iterrows():
            month = row['month']
            
            # Seasonal weather patterns
            if month in [12, 1, 2]:  # Winter
                weather_probs = [0.4, 0.2, 0.15, 0.1, 0.1, 0.05]
            elif month in [6, 7, 8]:  # Summer
                weather_probs = [0.7, 0.15, 0.1, 0.03, 0.01, 0.01]
            elif month in [3, 4, 5]:  # Spring
                weather_probs = [0.5, 0.25, 0.15, 0.07, 0.02, 0.01]
            else:  # Fall
                weather_probs = [0.6, 0.2, 0.12, 0.05, 0.02, 0.01]
            
            weather_condition = np.random.choice(
                ['clear', 'light_rain', 'moderate_rain', 'heavy_rain', 'snow', 'storm'],
                p=weather_probs
            )
            weather_conditions.append(weather_condition)
        
        return pd.Series(weather_conditions)
    
    def _generate_driver_density(self, df: pd.DataFrame) -> pd.Series:
        """
        Generate synthetic driver density affecting surge pricing
        """
        # Driver density affects surge pricing (lower density = higher surge)
        driver_densities = np.random.exponential(scale=2.0, size=len(df))
        # Normalize to 0.5-3.0 range (inverse relationship with surge)
        driver_densities = 0.5 + (driver_densities / np.max(driver_densities)) * 2.5
        
        return pd.Series(driver_densities)
    
    def prepare_features_target(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare features and target for model training
        """
        # Select features for training
        feature_columns = [
            'pickup_latitude', 'pickup_longitude',
            'dropoff_latitude', 'dropoff_longitude',
            'passenger_count', 'distance_km',
            'hour', 'day_of_week', 'month',
            'is_weekend', 'is_rush_hour',
            'traffic_factor', 'weather_factor', 'driver_density'
        ]
        
        # One-hot encode time_of_day
        time_dummies = pd.get_dummies(df['time_of_day'], prefix='time')
        
        X = pd.concat([df[feature_columns], time_dummies], axis=1)
        y = df['fare_amount']
        
        # Final check for NaN values and handle them
        if X.isnull().any().any():
            logger.warning("Found NaN values in features. Filling with appropriate defaults...")
            
            # Fill numeric columns with median
            numeric_columns = X.select_dtypes(include=[np.number]).columns
            X[numeric_columns] = X[numeric_columns].fillna(X[numeric_columns].median())
            
            # Fill any remaining NaN values with 0
            X = X.fillna(0)
        
        # Ensure no infinite values
        X = X.replace([np.inf, -np.inf], 0)
        
        logger.info(f"Features prepared. Shape: {X.shape}")
        logger.info(f"Feature columns: {list(X.columns)}")
        logger.info(f"NaN values in features: {X.isnull().sum().sum()}")
        
        return X, y
