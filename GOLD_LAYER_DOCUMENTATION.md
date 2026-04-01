# Gold Layer Dataset Documentation

## Overview
The Gold layer contains **analysis-ready** datasets derived from Silver layer with intentional derived columns for statistical analysis and visualization.

---

## Deduplication Strategy

**Problem:** 
- Two ingestion runs on the same day (03-31) created duplicate records
- Each (date, city) pair had 2 records from different source files

**Solution Applied:**
- Grouped by `(date, city)`
- Kept **latest** `source_file` (most recent ingestion run)
- Discarded earlier duplicate

**Rationale:**
- Latest data is most accurate
- Maintains audit trail (source_file preserved)
- Reduces redundancy while preserving information

**Result:**
- Silver: 48 records → Gold: 24 records (50% duplication)

---

## Gold Layer Datasets

### 1. `weather_analysis_ready.csv`
**Purpose:** Individual city-day observations with derived indicators

**Schema (20 columns):**

| Column | Type | Derivation |
|--------|------|-----------|
| date | DATE | From silver |
| city | VARCHAR | From silver |
| latitude | FLOAT | From silver |
| longitude | FLOAT | From silver |
| temp_max_celsius | FLOAT | From silver |
| temp_min_celsius | FLOAT | From silver |
| temp_range_celsius | FLOAT | From silver |
| precipitation_mm | FLOAT | From silver |
| wind_speed_max_kmh | FLOAT | From silver |
| **is_cold_day** | INT (0/1) | `temp_min_celsius < 0` |
| **is_hot_day** | INT (0/1) | `temp_max_celsius > 20` |
| **is_freezing** | INT (0/1) | `temp_min_celsius <= -10` |
| **extreme_temp_range** | INT (0/1) | `temp_range_celsius > 15` |
| **is_rainy_day** | INT (0/1) | `precipitation_mm > 0` |
| **is_wet_day** | INT (0/1) | `precipitation_mm > 5` |
| **is_heavy_rain** | INT (0/1) | `precipitation_mm > 10` |
| **is_windy_day** | INT (0/1) | `wind_speed_max_kmh > 30` |
| **day_of_week** | VARCHAR | Extract from date |
| **is_weekend** | INT (0/1) | `dayofweek IN (6, 7)` |
| **day_of_month** | INT | Extract from date |

**Sample Row:**
```
2026-03-24, barrie, 44.39, -79.66, 3.4, -7.2, 10.6, 0.0, 22.4
1, 0, 0, 0, 0, 0, 0, 0, Tuesday, 0, 24
```

**Statistics:**
- Rows: 24 (3 cities × 8 days)
- Cold days: 16/24 (67%)
- Rainy days: 13/24 (54%)
- Weekend days: 6/24 (25%)
- Freezing days: 0/24 (0%)
- Extreme temp ranges: 1/24 (4%)

---

### 2. `weather_daily_summary.csv`
**Purpose:** Daily aggregates across all cities (reduced grain for Time Series Analysis)

**Schema (6 columns):**

| Column | Type | Aggregation |
|--------|------|-------------|
| date | DATE | Date key |
| avg_temp_max_celsius | FLOAT | AVG(temp_max) across cities |
| avg_temp_min_celsius | FLOAT | AVG(temp_min) across cities |
| total_precipitation_mm | FLOAT | SUM(precipitation) across cities |
| max_wind_speed_kmh | FLOAT | MAX(wind_speed) across cities |
| any_city_rainy | INT (0/1) | MAX(is_rainy_day) → ANY city rained |
| any_city_cold | INT (0/1) | MAX(is_cold_day) → ANY city cold |

**Sample Rows:**
```
2026-03-24, 3.93, -6.07, 0.1, 22.4, 1, 1
2026-03-26, 8.87, 0.53, 18.4, 21.3, 1, 1
2026-03-31, 14.73, 5.43, 41.1, 25.0, 1, 0
```

---

## Derived Column Definitions

### Temperature Indicators
- **is_cold_day:** `1` if minimum temperature drops below freezing (0°C)
- **is_hot_day:** `1` if maximum temperature exceeds 20°C (comfortable/warm threshold)
- **is_freezing:** `1` if minimum temperature ≤ -10°C (severe cold)
- **extreme_temp_range:** `1` if daily range exceeds 15°C (significant daily swing)

### Precipitation Indicators
- **is_rainy_day:** `1` if any precipitation falls (drizzle or rain)
- **is_wet_day:** `1` if ≥ 5mm precipitation (meaningful rainfall)
- **is_heavy_rain:** `1` if ≥ 10mm precipitation (significant rain event)

### Wind Indicators
- **is_windy_day:** `1` if max wind speed > 30 km/h

### Temporal Indicators
- **day_of_week:** Day name (Monday–Sunday) from date
- **is_weekend:** `1` if Saturday (dayofweek=6) or Sunday (dayofweek=7)
- **day_of_month:** Day number (1–31)

---

## Data Quality

| Metric | Value |
|--------|-------|
| Records | 24 (deduplicated) |
| Date range | 2026-03-24 to 2026-03-31 (8 days) |
| Cities represented | 3 (Toronto, Oshawa, Barrie) |
| Precipitation days | 13/24 (54%) |
| Cold days | 16/24 (67%) |
| Total precipitation | 124.6 mm |
| Temperature range | -12.0°C to 18.8°C |
| Missing values | 0 |

---

## Implementation Approaches

### Pandas Approach (`create_gold.py`)
```python
df_deduplicated["is_cold_day"] = (df["temp_min_celsius"] < 0).astype(int)
```
- Simple, intuitive boolean casting
- Suitable for small-medium datasets

### DuckDB SQL Approach (`create_gold_duckdb.py`)
```sql
CASE WHEN temp_min_celsius < 0 THEN 1 ELSE 0 END as is_cold_day
```
- SQL-based declarative approach
- Optimized for large volumes
- CTE-based deduplication in query

---

## Files Generated

```
data/gold/
├── weather_analysis_ready.csv          # City-level analysis (24 rows)
├── weather_analysis_ready_duckdb.csv   # SQL-generated variant
├── weather_daily_summary.csv           # Daily aggregates (8 rows)
└── weather_daily_summary_duckdb.csv    # SQL-generated variant
```

---

## Use Cases

### Time Series Analysis
- Daily summary table tracks weather trends across the 8-day period
- `any_city_rainy`, `any_city_cold` flags for event-based analysis

### Statistical Modeling
- Binary indicators (0/1) suitable for classification/regression
- Continuous values (temperature, precipitation) for regression

### Reporting & Dashboards
- City-level data for location-based comparisons
- Derived flags enable quick filtering (e.g., show only rainy days)

### Feature Engineering
- Indicator variables ready for ML pipelines
- Temporal features (day_of_week, day_of_month) for seasonality

---

## SQL Generation (DuckDB)

**Deduplication via CTE:**
```sql
WITH deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY date, city ORDER BY source_file DESC) as rn
    FROM silver_table
),
clean AS (
    SELECT * FROM deduplicated WHERE rn = 1
)
```

**Derived Columns via CASE:**
```sql
SELECT
    date,
    city,
    CASE WHEN temp_min_celsius < 0 THEN 1 ELSE 0 END as is_cold_day,
    CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END as is_rainy_day,
    ... 
FROM clean
```

---

## Next Steps

Gold datasets are **analysis-ready** for:
✓ Exploratory Data Analysis (EDA)  
✓ Time-series forecasting  
✓ Correlation analysis (weather patterns)  
✓ Dashboard visualization  
✓ Machine learning feature engineering  
