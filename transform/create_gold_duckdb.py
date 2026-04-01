#!/usr/bin/env python3
"""
Gold Layer Creation using DuckDB SQL
Demonstrates SQL-based aggregation and derived columns
"""

import duckdb
from pathlib import Path


def create_gold_layer_duckdb():
    """Create Gold layer using DuckDB SQL"""
    
    silver_dir = Path("data/silver")
    gold_dir = Path("data/gold")
    gold_dir.mkdir(parents=True, exist_ok=True)
    
    # Register silver CSV as DuckDB table
    conn = duckdb.connect(':memory:')
    silver_file = str(silver_dir / "weather_daily_clean.csv")
    
    print("🔄 Running DuckDB SQL transformations...")
    
    # SQL Query: Deduplication + Derived Columns
    sql_gold = f"""
    WITH deduplicated AS (
        -- Deduplication Strategy: Keep latest source_file per (date, city)
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY date, city ORDER BY source_file DESC) as rn
        FROM read_csv_auto('{silver_file}')
    ),
    deduplicated_clean AS (
        SELECT
            CAST(date AS DATE) as date,
            city,
            latitude,
            longitude,
            timezone,
            temp_max_celsius,
            temp_min_celsius,
            temp_range_celsius,
            precipitation_mm,
            wind_speed_max_kmh
        FROM deduplicated
        WHERE rn = 1
    ),
    with_derived AS (
        SELECT
            *,
            -- Temperature-based indicators
            CASE WHEN temp_min_celsius < 0 THEN 1 ELSE 0 END as is_cold_day,
            CASE WHEN temp_max_celsius > 20 THEN 1 ELSE 0 END as is_hot_day,
            CASE WHEN temp_min_celsius <= -10 THEN 1 ELSE 0 END as is_freezing,
            CASE WHEN temp_range_celsius > 15 THEN 1 ELSE 0 END as extreme_temp_range,
            -- Precipitation-based indicators
            CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END as is_rainy_day,
            CASE WHEN precipitation_mm > 5 THEN 1 ELSE 0 END as is_wet_day,
            CASE WHEN precipitation_mm > 10 THEN 1 ELSE 0 END as is_heavy_rain,
            -- Wind-based indicators
            CASE WHEN wind_speed_max_kmh > 30 THEN 1 ELSE 0 END as is_windy_day,
            -- Temporal indicators
            dayname(date) as day_of_week,
            CASE WHEN dayofweek(date) IN (6, 7) THEN 1 ELSE 0 END as is_weekend,
            day(date) as day_of_month
        FROM deduplicated_clean
    )
    SELECT * FROM with_derived
    ORDER BY date, city
    """
    
    gold_df = conn.sql(sql_gold).to_df()
    
    # Save comprehensive Gold dataset
    gold_file = gold_dir / "weather_analysis_ready_duckdb.csv"
    gold_df.to_csv(gold_file, index=False)
    print(f"✓ Saved {gold_file}")
    
    # SQL Query: Daily Summary Aggregation
    sql_daily = f"""
    WITH deduplicated AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY date, city ORDER BY source_file DESC) as rn
        FROM read_csv_auto('{silver_file}')
        WHERE rn = 1 OR rn IS NULL
    ),
    daily_summary AS (
        SELECT
            CAST(date AS DATE) as date,
            ROUND(AVG(temp_max_celsius), 1) as avg_temp_max_celsius,
            ROUND(AVG(temp_min_celsius), 1) as avg_temp_min_celsius,
            ROUND(SUM(precipitation_mm), 1) as total_precipitation_mm,
            ROUND(MAX(wind_speed_max_kmh), 1) as max_wind_speed_kmh,
            COUNT(DISTINCT city) as num_cities_reported,
            MAX(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END) as any_city_rainy,
            MAX(CASE WHEN temp_min_celsius < 0 THEN 1 ELSE 0 END) as any_city_cold,
            COUNT(*) as records_count
        FROM read_csv_auto('{silver_file}')
        GROUP BY date
    )
    SELECT * FROM daily_summary
    ORDER BY date
    """
    
    daily_df = conn.sql(sql_daily).to_df()
    
    # Save daily summary dataset
    daily_file = gold_dir / "weather_daily_summary_duckdb.csv"
    daily_df.to_csv(daily_file, index=False)
    print(f"✓ Saved {daily_file}")
    
    # Data quality report via SQL
    sql_quality = f"""
    SELECT
        COUNT(DISTINCT date) as dates,
        COUNT(DISTINCT city) as cities,
        ROUND(AVG(temp_max_celsius), 1) as avg_max_temp_c,
        ROUND(MIN(temp_min_celsius), 1) as min_temp_c,
        ROUND(MAX(temp_max_celsius), 1) as max_temp_c,
        ROUND(SUM(precipitation_mm), 1) as total_precip_mm,
        SUM(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END) as rainy_instances,
        SUM(CASE WHEN temp_min_celsius < 0 THEN 1 ELSE 0 END) as cold_instances
    FROM read_csv_auto('{silver_file}')
    """
    
    print("\n📈 Gold Layer Summary (via SQL):")
    quality = conn.sql(sql_quality).to_df()
    for col in quality.columns:
        val = quality[col].iloc[0]
        print(f"  {col}: {val}")
    
    print("\n✓ DuckDB Gold layer creation complete!")
    return gold_df, daily_df


if __name__ == "__main__":
    create_gold_layer_duckdb()
