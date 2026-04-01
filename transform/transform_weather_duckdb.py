#!/usr/bin/env python3
"""
Weather Data Transformation using DuckDB
Alternative approach using SQL for transformation
Bronze → Silver
"""

import json
import duckdb
from pathlib import Path


def transform_with_duckdb():
    """Transform weather data using DuckDB SQL"""
    
    bronze_dir = Path("data/bronze/weather")
    silver_dir = Path("data/silver")
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize DuckDB connection
    conn = duckdb.connect(':memory:')
    
    # Create a list of all raw records
    all_records = []
    
    for json_file in sorted(bronze_dir.glob("*.json")):
        city = json_file.stem.split("_weather_")[0]
        
        with open(json_file, "r") as f:
            raw = json.load(f)
        
        daily_data = raw.get("daily", {})
        times = daily_data.get("time", [])
        temps_max = daily_data.get("temperature_2m_max", [])
        temps_min = daily_data.get("temperature_2m_min", [])
        precip = daily_data.get("precipitation_sum", [])
        wind_speed = daily_data.get("wind_speed_10m_max", [])
        
        for i, date_str in enumerate(times):
            all_records.append({
                "date": date_str,
                "city": city,
                "latitude": raw.get("latitude"),
                "longitude": raw.get("longitude"),
                "timezone": raw.get("timezone"),
                "temp_max_celsius": temps_max[i],
                "temp_min_celsius": temps_min[i],
                "precipitation_mm": precip[i],
                "wind_speed_max_kmh": wind_speed[i],
                "source_file": json_file.name,
            })
    
    # Create table from records
    import pandas as pd
    df_raw = pd.DataFrame(all_records)
    raw_table = conn.from_df(df_raw)
    
    # SQL transformation
    sql_query = """
    SELECT
        CAST(date AS DATE) as date,
        CAST(city AS VARCHAR) as city,
        latitude,
        longitude,
        timezone,
        ROUND(CAST(temp_max_celsius AS FLOAT), 1) as temp_max_celsius,
        ROUND(CAST(temp_min_celsius AS FLOAT), 1) as temp_min_celsius,
        ROUND(CAST(temp_max_celsius AS FLOAT) - CAST(temp_min_celsius AS FLOAT), 1) as temp_range_celsius,
        ROUND(CAST(precipitation_mm AS FLOAT), 1) as precipitation_mm,
        ROUND(CAST(wind_speed_max_kmh AS FLOAT), 1) as wind_speed_max_kmh,
        source_file,
        CURRENT_TIMESTAMP as processed_at
    FROM raw_table
    WHERE 
        temp_max_celsius IS NOT NULL
        AND temp_min_celsius IS NOT NULL
    ORDER BY 
        date,
        city
    """
    
    print("🔄 Running DuckDB SQL transformation...")
    transformed = conn.sql(sql_query).to_df()
    
    # Save to CSV
    output_file = silver_dir / "weather_daily_clean_duckdb.csv"
    transformed.to_csv(output_file, index=False)
    print(f"✓ Saved {output_file}")
    
    # Save to JSON for gold layer
    output_json = silver_dir / "weather_daily_clean_duckdb.json"
    transformed.to_json(output_json, orient="records", indent=2)
    print(f"✓ Saved {output_json}")
    
    # Data quality checks using SQL
    print("\n📊 Data Quality Report (DuckDB SQL):")
    
    quality_query = """
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT city) as cities,
        COUNT(DISTINCT date) as dates,
        MIN(date) as earliest_date,
        MAX(date) as latest_date,
        COUNT(CASE WHEN temp_max_celsius IS NULL THEN 1 END) as missing_temp_max,
        COUNT(CASE WHEN precipitation_mm IS NULL THEN 1 END) as missing_precip,
        ROUND(AVG(temp_max_celsius), 1) as avg_max_temp,
        ROUND(MIN(temp_min_celsius), 1) as min_temp,
        ROUND(MAX(temp_max_celsius), 1) as max_temp,
        ROUND(MAX(precipitation_mm), 1) as max_precip_mm
    FROM raw_table
    """
    
    quality = conn.sql(quality_query).to_df()
    for col in quality.columns:
        print(f"  {col}: {quality[col].iloc[0]}")
    
    print("\n✓ DuckDB transformation complete!")
    return transformed


if __name__ == "__main__":
    transform_with_duckdb()
