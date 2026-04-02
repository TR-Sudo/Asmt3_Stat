# Gold Layer Documentation

## Overview
Analysis-ready datasets with derived indicators for statistical analysis.

## Datasets

### weather_analysis_ready.csv
City-day observations with derived indicators.

**Columns:**
- date, city, latitude, longitude
- temp_max_celsius, temp_min_celsius, temp_range_celsius
- precipitation_mm, wind_speed_max_kmh
- is_cold_day, is_hot_day, is_freezing, extreme_temp_range
- is_rainy_day, is_wet_day, is_heavy_rain, is_windy_day
- day_of_week, is_weekend, day_of_month

### weather_daily_summary.csv
Daily aggregates across cities.

**Columns:**
- date
- avg_temp_max_celsius, avg_temp_min_celsius
- total_precipitation_mm, max_wind_speed_kmh
- any_city_rainy, any_city_cold

## Derived Indicators
- Temperature: cold/hot/freezing days, extreme ranges
- Precipitation: rainy/wet/heavy rain days
- Wind: windy days
- Temporal: weekend, day of week/month
