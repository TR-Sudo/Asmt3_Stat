#!/usr/bin/env python3
"""
Weather Data Transformation Script
Converts raw JSON from Open-Meteo API into clean tabular format
Bronze → Silver
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime


def transform_weather():
    """Transform weather Bronze files into Silver layer"""
    
    bronze_dir = Path("data/bronze/weather")
    silver_dir = Path("data/silver")
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    # Collect all weather data
    all_data = []
    
    # Process each JSON file
    for json_file in sorted(bronze_dir.glob("*.json")):
        city = json_file.stem.split("_weather_")[0]  # Extract city from filename
        
        print(f"Processing {city}...")
        
        with open(json_file, "r") as f:
            raw = json.load(f)
        
        # Extract daily data arrays
        daily_data = raw.get("daily", {})
        times = daily_data.get("time", [])
        temps_max = daily_data.get("temperature_2m_max", [])
        temps_min = daily_data.get("temperature_2m_min", [])
        precip = daily_data.get("precipitation_sum", [])
        wind_speed = daily_data.get("wind_speed_10m_max", [])
        
        # Create records for each day
        for i, date_str in enumerate(times):
            record = {
                "date": date_str,
                "city": city,
                "latitude": raw.get("latitude"),
                "longitude": raw.get("longitude"),
                "timezone": raw.get("timezone"),
                "temp_max_celsius": temps_max[i] if i < len(temps_max) else None,
                "temp_min_celsius": temps_min[i] if i < len(temps_min) else None,
                "precipitation_mm": precip[i] if i < len(precip) else None,
                "wind_speed_max_kmh": wind_speed[i] if i < len(wind_speed) else None,
                "source_file": json_file.name,
            }
            all_data.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(all_data)
    
    # Data type conversions
    df["date"] = pd.to_datetime(df["date"])
    df["temp_max_celsius"] = pd.to_numeric(df["temp_max_celsius"], errors="coerce")
    df["temp_min_celsius"] = pd.to_numeric(df["temp_min_celsius"], errors="coerce")
    df["precipitation_mm"] = pd.to_numeric(df["precipitation_mm"], errors="coerce")
    df["wind_speed_max_kmh"] = pd.to_numeric(df["wind_speed_max_kmh"], errors="coerce")
    
    # Document missing value handling
    print("\n🔍 Missing Values Report:")
    missing = df.isnull().sum()
    if missing.sum() > 0:
        print(missing[missing > 0])
    else:
        print("✓ No missing values found")
    
    # Calculate temperature range
    df["temp_range_celsius"] = df["temp_max_celsius"] - df["temp_min_celsius"]
    
    # Sort by date and city for consistency
    df = df.sort_values(["date", "city"]).reset_index(drop=True)
    
    # Save to CSV
    output_file = silver_dir / "weather_daily_clean.csv"
    df.to_csv(output_file, index=False)
    print(f"\n✓ Saved {output_file}")
    
    # Summary statistics
    print("\n📊 Summary Statistics:")
    print(f"  Rows: {len(df)}")
    print(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    print(f"  Cities: {df['city'].unique().tolist()}")
    print(f"  Temp range: {df['temp_max_celsius'].min():.1f}°C to {df['temp_max_celsius'].max():.1f}°C")
    print(f"  Max precipitation: {df['precipitation_mm'].max():.1f}mm")
    
    return df


if __name__ == "__main__":
    df = transform_weather()
    print("\n✓ Transformation complete!")
