#!/usr/bin/env python3
"""
Gold Layer Dataset Creation
Reads Silver data and creates analysis-ready table with derived columns
"""

import pandas as pd
from pathlib import Path


def create_gold_layer():
    """Create analysis-ready Gold dataset from Silver"""
    
    silver_dir = Path("data/silver")
    gold_dir = Path("data/gold")
    gold_dir.mkdir(parents=True, exist_ok=True)
    
    # Read Silver dataset
    print("📖 Reading Silver layer data...")
    silver_csv = silver_dir / "weather_daily_clean.csv"
    df = pd.read_csv(silver_csv)
    df["date"] = pd.to_datetime(df["date"])
    
    # Since we ran ingestion twice, we have duplicates
    # Strategy: Group by (date, city) and take the latest source_file
    # This keeps the most recent ingestion run
    print("\n🔍 Deduplication Strategy:")
    print("  Problem: Two ingestion runs on same date → duplicate records")
    print("  Solution: Group by (date, city) → keep latest source_file")
    print("  Rationale: Latest run has most recent data")
    
    # Sort by date descending, then keep first (most recent) for each group
    df_deduplicated = df.sort_values("source_file", ascending=False).drop_duplicates(
        subset=["date", "city"], keep="first"
    ).sort_values(["date", "city"]).reset_index(drop=True)
    
    print(f"  Records before dedup: {len(df)}")
    print(f"  Records after dedup: {len(df_deduplicated)}")
    
    # Create derived columns for analysis
    print("\n🔨 Creating derived columns...")
    
    # Temperature-based indicators
    df_deduplicated["is_cold_day"] = (df_deduplicated["temp_min_celsius"] < 0).astype(int)
    df_deduplicated["is_hot_day"] = (df_deduplicated["temp_max_celsius"] > 20).astype(int)
    df_deduplicated["is_freezing"] = (df_deduplicated["temp_min_celsius"] <= -10).astype(int)
    df_deduplicated["extreme_temp_range"] = (
        df_deduplicated["temp_range_celsius"] > 15
    ).astype(int)
    
    # Precipitation-based indicators
    df_deduplicated["is_rainy_day"] = (df_deduplicated["precipitation_mm"] > 0).astype(int)
    df_deduplicated["is_wet_day"] = (df_deduplicated["precipitation_mm"] > 5).astype(int)
    df_deduplicated["is_heavy_rain"] = (df_deduplicated["precipitation_mm"] > 10).astype(int)
    
    # Wind-based indicators
    df_deduplicated["is_windy_day"] = (df_deduplicated["wind_speed_max_kmh"] > 30).astype(int)
    
    # Temporal indicators
    df_deduplicated["day_of_week"] = df_deduplicated["date"].dt.day_name()
    df_deduplicated["is_weekend"] = df_deduplicated["date"].dt.weekday.isin([5, 6]).astype(int)
    df_deduplicated["day_of_month"] = df_deduplicated["date"].dt.day
    
    # Aggregate metrics per day (across all cities)
    print("\n📊 Computing daily metrics across all cities...")
    daily_agg = df_deduplicated.groupby("date").agg({
        "temp_max_celsius": "mean",
        "temp_min_celsius": "mean",
        "precipitation_mm": "sum",
        "wind_speed_max_kmh": "max",
        "is_rainy_day": "max",
        "is_cold_day": "max",
    }).reset_index()
    
    daily_agg.columns = [
        "date", 
        "avg_temp_max_celsius", 
        "avg_temp_min_celsius",
        "total_precipitation_mm",
        "max_wind_speed_kmh",
        "any_city_rainy",
        "any_city_cold"
    ]
    
    # Create comprehensive Gold table
    gold_df = df_deduplicated[[
        "date",
        "city",
        "latitude",
        "longitude",
        "temp_max_celsius",
        "temp_min_celsius",
        "temp_range_celsius",
        "precipitation_mm",
        "wind_speed_max_kmh",
        "is_cold_day",
        "is_hot_day",
        "is_freezing",
        "extreme_temp_range",
        "is_rainy_day",
        "is_wet_day",
        "is_heavy_rain",
        "is_windy_day",
        "day_of_week",
        "is_weekend",
        "day_of_month",
    ]].copy()
    
    # Save comprehensive Gold dataset
    gold_file = gold_dir / "weather_analysis_ready.csv"
    gold_df.to_csv(gold_file, index=False)
    print(f"✓ Saved {gold_file}")
    
    # Save aggregated daily dataset
    daily_file = gold_dir / "weather_daily_summary.csv"
    daily_agg.to_csv(daily_file, index=False)
    print(f"✓ Saved {daily_file}")
    
    # Summary statistics
    print("\n📈 Gold Layer Summary:")
    print(f"  Records: {len(gold_df)}")
    print(f"  Date range: {gold_df['date'].min().date()} to {gold_df['date'].max().date()}")
    print(f"  Cities: {gold_df['city'].unique().tolist()}")
    
    print("\n🏷️ Derived Columns Created:")
    print(f"  Temperature: is_cold_day, is_hot_day, is_freezing, extreme_temp_range")
    print(f"  Precipitation: is_rainy_day, is_wet_day, is_heavy_rain")
    print(f"  Wind: is_windy_day")
    print(f"  Temporal: day_of_week, is_weekend, day_of_month")
    
    print("\n📌 Key Statistics:")
    print(f"  Cold days: {gold_df['is_cold_day'].sum()}/{len(gold_df)} ({100*gold_df['is_cold_day'].mean():.0f}%)")
    print(f"  Rainy days: {gold_df['is_rainy_day'].sum()}/{len(gold_df)} ({100*gold_df['is_rainy_day'].mean():.0f}%)")
    print(f"  Weekend days: {gold_df['is_weekend'].sum()}/{len(gold_df)} ({100*gold_df['is_weekend'].mean():.0f}%)")
    print(f"  Extreme temp ranges: {gold_df['extreme_temp_range'].sum()}/{len(gold_df)}")
    
    return gold_df, daily_agg


if __name__ == "__main__":
    gold_df, daily_agg = create_gold_layer()
    print("\n✓ Gold layer creation complete!")
