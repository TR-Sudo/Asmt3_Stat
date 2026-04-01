#!/usr/bin/env python3
"""
Weather Data Ingestion Script
Fetches historical weather data from Open-Meteo API and saves to bronze layer
"""

import requests
import json
from datetime import datetime
from pathlib import Path


def ingest_weather():
    """Fetch historical weather data and save to bronze layer"""
    
    # Define cities to ingest (latitude, longitude)
    cities = {
        "toronto": {"lat": 43.65, "lon": -79.38},
        "oshawa": {"lat": 43.89, "lon": -79.05},
        "barrie": {"lat": 44.39, "lon": -79.69},
    }
    
    # Weather variables to fetch
    daily_vars = "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
    
    # Historical data: last 7 days
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = datetime.now().replace(day=datetime.now().day - 7).strftime("%Y-%m-%d")
    
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    
    # Create bronze/weather directory if it doesn't exist
    weather_dir = Path("data/bronze/weather")
    weather_dir.mkdir(parents=True, exist_ok=True)
    
    # Timestamp for this ingestion run
    ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    
    for city, coords in cities.items():
        try:
            # API parameters
            params = {
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "start_date": start_date,
                "end_date": end_date,
                "daily": daily_vars,
                "timezone": "auto"
            }
            
            print(f"Fetching weather data for {city}...")
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Save to file
            filename = weather_dir / f"{city}_weather_{ts}.json"
            with open(filename, "w") as f:
                json.dump(data, f, indent=2)
            
            print(f"✓ Saved {filename}")
            
        except Exception as e:
            print(f"✗ Error fetching data for {city}: {e}")


if __name__ == "__main__":
    ingest_weather()
    print("\nIngestion complete!")
