import streamlit as st
import requests
import json
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import yaml
import random

# Page configuration
st.set_page_config(
    page_title="Ride Fare Prediction Dashboard",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load configuration
@st.cache_data
def load_config():
    with open("config/cities.yaml", 'r') as f:
        return yaml.safe_load(f)

config = load_config()

# API endpoints
import os
PREDICTION_SERVICE_URL = os.getenv('PREDICTION_SERVICE_URL', 'http://prediction-service:8000')

def make_prediction_request(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon, passengers, city):
    """Make a prediction request to the API"""
    try:
        response = requests.post(
            f"{PREDICTION_SERVICE_URL}/predict",
            json={
                "pickup_latitude": pickup_lat,
                "pickup_longitude": pickup_lon,
                "dropoff_latitude": dropoff_lat,
                "dropoff_longitude": dropoff_lon,
                "passenger_count": passengers,
                "city": city
            },
            timeout=10
        )
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API Error: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Connection error: {e}")
        return None

def get_current_conditions(city):
    """Get current conditions for a city"""
    try:
        response = requests.get(
            f"{PREDICTION_SERVICE_URL}/current-conditions/{city}",
            timeout=5
        )
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except requests.exceptions.RequestException:
        return None

def check_api_health():
    """Check if the prediction API is healthy"""
    try:
        response = requests.get(f"{PREDICTION_SERVICE_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False

# Sidebar
st.sidebar.title("🚗 Ride Fare Prediction")
st.sidebar.markdown("---")

# API Health Check
api_healthy = check_api_health()
if api_healthy:
    st.sidebar.success("✅ Prediction Service Online")
else:
    st.sidebar.error("❌ Prediction Service Offline")

# City Selection
cities = list(config['cities'].keys())
city_names = [config['cities'][city]['name'] for city in cities]
selected_city_name = st.sidebar.selectbox("Select City", city_names)
selected_city = cities[city_names.index(selected_city_name)]

# Get city configuration
city_config = config['cities'][selected_city]
bounds = city_config['bounds']

st.sidebar.markdown("---")

# Main content
st.title("🚗 Real-Time Ride Fare Prediction System")
st.markdown(f"**Current City:** {city_config['name']}")

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs(["🎯 Fare Prediction", "📊 Live Conditions", "📈 Analytics", "⚙️ System Status"])

with tab1:
    st.header("Fare Prediction")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Pickup Location")
        pickup_lat = st.number_input(
            "Pickup Latitude", 
            min_value=bounds['min_lat'], 
            max_value=bounds['max_lat'],
            value=(bounds['min_lat'] + bounds['max_lat']) / 2,
            format="%.6f"
        )
        pickup_lon = st.number_input(
            "Pickup Longitude", 
            min_value=bounds['min_lon'], 
            max_value=bounds['max_lon'],
            value=(bounds['min_lon'] + bounds['max_lon']) / 2,
            format="%.6f"
        )
        
        # Quick location buttons for pickup
        if 'traffic_zones' in city_config:
            st.write("Quick Locations:")
            for zone in city_config['traffic_zones']:
                if st.button(f"📍 {zone['name']}", key=f"pickup_{zone['name']}"):
                    zone_bounds = zone['bounds']
                    pickup_lat = (zone_bounds[0] + zone_bounds[1]) / 2
                    pickup_lon = (zone_bounds[2] + zone_bounds[3]) / 2
                    st.experimental_rerun()
    
    with col2:
        st.subheader("Dropoff Location")
        dropoff_lat = st.number_input(
            "Dropoff Latitude", 
            min_value=bounds['min_lat'], 
            max_value=bounds['max_lat'],
            value=(bounds['min_lat'] + bounds['max_lat']) / 2 + 0.01,
            format="%.6f"
        )
        dropoff_lon = st.number_input(
            "Dropoff Longitude", 
            min_value=bounds['min_lon'], 
            max_value=bounds['max_lon'],
            value=(bounds['min_lon'] + bounds['max_lon']) / 2 + 0.01,
            format="%.6f"
        )
        
        # Quick location buttons for dropoff
        if 'traffic_zones' in city_config:
            st.write("Quick Locations:")
            for zone in city_config['traffic_zones']:
                if st.button(f"📍 {zone['name']}", key=f"dropoff_{zone['name']}"):
                    zone_bounds = zone['bounds']
                    dropoff_lat = (zone_bounds[0] + zone_bounds[1]) / 2
                    dropoff_lon = (zone_bounds[2] + zone_bounds[3]) / 2
                    st.experimental_rerun()
    
    # Passenger count
    passengers = st.selectbox("Number of Passengers", [1, 2, 3, 4, 5, 6], index=0)
    
    # Predict button
    if st.button("🎯 Predict Fare", type="primary", disabled=not api_healthy):
        if api_healthy:
            with st.spinner("Predicting fare..."):
                prediction = make_prediction_request(
                    pickup_lat, pickup_lon, dropoff_lat, dropoff_lon, passengers, selected_city
                )
                
                if prediction:
                    # Display prediction results
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric(
                            "💰 Predicted Fare", 
                            f"${prediction['predicted_fare']:.2f}",
                            help="Total predicted fare including all factors"
                        )
                    
                    with col2:
                        st.metric(
                            "🚀 Surge Multiplier", 
                            f"{prediction['surge_multiplier']:.2f}x",
                            help="Dynamic pricing based on demand"
                        )
                    
                    with col3:
                        st.metric(
                            "🌤️ Weather Factor", 
                            f"{prediction['weather_factor']:.2f}x",
                            help="Weather impact on pricing"
                        )
                    
                    with col4:
                        st.metric(
                            "🚦 Traffic Factor", 
                            f"{prediction['traffic_factor']:.2f}x",
                            help="Traffic impact on pricing"
                        )
                    
                    # Fare breakdown
                    st.subheader("Fare Breakdown")
                    breakdown_data = {
                        'Component': ['Base Fare', 'Distance Fare', 'Time Fare'],
                        'Amount': [prediction['base_fare'], prediction['distance_fare'], prediction['time_fare']]
                    }
                    
                    fig = px.bar(
                        breakdown_data, 
                        x='Component', 
                        y='Amount',
                        title="Fare Components",
                        color='Component'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Confidence and timestamp
                    col1, col2 = st.columns(2)
                    with col1:
                        st.info(f"🎯 Confidence Score: {prediction['confidence_score']:.2f}")
                    with col2:
                        st.info(f"⏰ Predicted at: {prediction['prediction_timestamp']}")
        else:
            st.error("Prediction service is not available. Please check the system status.")

with tab2:
    st.header("Live Conditions")
    
    # Auto-refresh toggle
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
    
    if auto_refresh:
        # Auto-refresh every 30 seconds
        time.sleep(30)
        st.experimental_rerun()
    
    # Manual refresh button
    if st.button("🔄 Refresh Conditions"):
        st.experimental_rerun()
    
    # Get current conditions
    conditions = get_current_conditions(selected_city)
    
    if conditions:
        # Weather conditions
        st.subheader("🌤️ Current Weather")
        weather = conditions['weather']
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Condition", weather.get('condition', 'Unknown').title())
        with col2:
            st.metric("Temperature", f"{weather.get('temperature', 0):.1f}°C")
        with col3:
            st.metric("Humidity", f"{weather.get('humidity', 0):.1f}%")
        with col4:
            st.metric("Wind Speed", f"{weather.get('wind_speed', 0):.1f} km/h")
        
        # Driver density
        st.subheader("🚗 Driver Availability")
        density = conditions['driver_density']
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Available Drivers", density.get('available_drivers', 0))
        with col2:
            st.metric("Total Drivers", density.get('total_drivers', 0))
        with col3:
            st.metric("Density Score", f"{density.get('density_score', 0):.2f}")
        
        # Zone-specific data if available
        if 'zone_densities' in density:
            st.subheader("📍 Zone-wise Driver Density")
            zone_data = []
            for zone, count in density['zone_densities'].items():
                zone_data.append({'Zone': zone, 'Available Drivers': count})
            
            if zone_data:
                df = pd.DataFrame(zone_data)
                fig = px.bar(df, x='Zone', y='Available Drivers', title="Drivers by Zone")
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Unable to fetch current conditions. The streaming services may not be running.")

with tab3:
    st.header("Analytics & Insights")
    
    # Simulated historical data for demonstration
    st.subheader("📈 Fare Trends (Simulated)")
    
    # Generate sample data
    hours = list(range(24))
    base_fares = []
    surge_multipliers = []
    
    for hour in hours:
        # Simulate rush hour patterns
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            base_fare = city_config['base_fare'] * random.uniform(1.2, 1.8)
            surge = random.uniform(1.5, 3.0)
        elif 22 <= hour <= 5:
            base_fare = city_config['base_fare'] * random.uniform(0.8, 1.1)
            surge = random.uniform(1.0, 1.3)
        else:
            base_fare = city_config['base_fare'] * random.uniform(1.0, 1.4)
            surge = random.uniform(1.0, 2.0)
        
        base_fares.append(base_fare)
        surge_multipliers.append(surge)
    
    # Create trend charts
    col1, col2 = st.columns(2)
    
    with col1:
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=hours, y=base_fares, mode='lines+markers', name='Average Fare'))
        fig1.update_layout(title="Average Fare by Hour", xaxis_title="Hour", yaxis_title="Fare ($)")
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=hours, y=surge_multipliers, mode='lines+markers', name='Surge Multiplier', line=dict(color='red')))
        fig2.update_layout(title="Surge Multiplier by Hour", xaxis_title="Hour", yaxis_title="Multiplier")
        st.plotly_chart(fig2, use_container_width=True)
    
    # Weather impact analysis
    st.subheader("🌦️ Weather Impact on Fares")
    weather_conditions = list(config['weather_factors'].keys())
    weather_factors = list(config['weather_factors'].values())
    
    fig3 = px.bar(
        x=weather_conditions, 
        y=weather_factors,
        title="Weather Impact Factors",
        labels={'x': 'Weather Condition', 'y': 'Price Multiplier'}
    )
    st.plotly_chart(fig3, use_container_width=True)
    
    # Traffic impact analysis
    st.subheader("🚦 Traffic Impact on Fares")
    traffic_levels = list(config['traffic_factors'].keys())
    traffic_factors = list(config['traffic_factors'].values())
    
    fig4 = px.bar(
        x=traffic_levels, 
        y=traffic_factors,
        title="Traffic Impact Factors",
        labels={'x': 'Traffic Level', 'y': 'Price Multiplier'},
        color=traffic_factors,
        color_continuous_scale='Reds'
    )
    st.plotly_chart(fig4, use_container_width=True)

with tab4:
    st.header("System Status")
    
    # Service health checks
    st.subheader("🔧 Service Health")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Prediction service
        if api_healthy:
            st.success("✅ Prediction Service: Online")
        else:
            st.error("❌ Prediction Service: Offline")
        
        # Try to get service info
        try:
            response = requests.get(f"{PREDICTION_SERVICE_URL}/health", timeout=5)
            if response.status_code == 200:
                health_data = response.json()
                st.json(health_data)
        except:
            st.warning("Unable to fetch detailed health information")
    
    with col2:
        # System configuration
        st.info("📋 System Configuration")
        st.write(f"**Default City:** {config['default_city']}")
        st.write(f"**Supported Cities:** {len(config['cities'])}")
        st.write(f"**Weather Factors:** {len(config['weather_factors'])}")
        st.write(f"**Traffic Factors:** {len(config['traffic_factors'])}")
    
    # City details
    st.subheader(f"🏙️ {city_config['name']} Configuration")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Base Fare", f"${city_config['base_fare']:.2f}")
        st.metric("Per Mile Rate", f"${city_config['per_mile_rate']:.2f}")
    
    with col2:
        st.metric("Per Minute Rate", f"${city_config['per_minute_rate']:.2f}")
        surge_range = city_config['surge_multiplier_range']
        st.metric("Surge Range", f"{surge_range[0]:.1f}x - {surge_range[1]:.1f}x")
    
    with col3:
        if 'traffic_zones' in city_config:
            st.metric("Traffic Zones", len(city_config['traffic_zones']))
            for zone in city_config['traffic_zones']:
                st.write(f"• {zone['name']}: {zone['traffic_factor']:.1f}x factor")
    
    # Instructions
    st.subheader("🚀 Getting Started")
    st.markdown("""
    **To use this system:**
    
    1. **Start the services:** Run `docker-compose up` to start all services
    2. **Train the model:** Run the ML pipeline to train the fare prediction model
    3. **Make predictions:** Use the Fare Prediction tab to get real-time fare estimates
    4. **Monitor conditions:** Check the Live Conditions tab for current weather and traffic
    
    **System Components:**
    - 🤖 **ML Model:** XGBoost/Random Forest for fare prediction
    - 📡 **Kafka Streams:** Real-time data streaming
    - 🌤️ **Weather Service:** Simulated weather data
    - 🚦 **Traffic Service:** Simulated traffic conditions
    - 🚗 **Driver Service:** Simulated driver availability
    - 📊 **Redis Cache:** Fast data access
    - 📈 **MLflow:** Model registry and tracking
    """)

# Footer
st.markdown("---")
st.markdown("**Ride Fare Prediction System** | Built with Streamlit, FastAPI, Kafka, and MLflow")
