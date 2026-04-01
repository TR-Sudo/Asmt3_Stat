# Weather Data Pipeline - Statistical Analysis Assignment

A complete data pipeline (Bronze → Silver → Gold) that ingests historical weather data from the Open-Meteo API, transforms it into analysis-ready datasets, and prepares for statistical hypothesis testing.

## Project Overview

This project demonstrates a modern data engineering workflow:
1. **Bronze Layer:** Raw API data with timestamps
2. **Silver Layer:** Cleaned, deduplicated tabular data
3. **Gold Layer:** Analysis-ready datasets with derived features

**Data Source:** [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) (free, no API key required)

**Cities:** Toronto, Oshawa, Barrie (Greater Toronto Area)

**Time Period:** March 24-31, 2026 (8 days)

---

## Directory Structure

```
project-root/
├── data/
│   ├── bronze/                          # Raw API responses
│   │   └── weather/
│   │       ├── toronto_weather_201T*.json
│   │       ├── oshawa_weather_2026T*.json
│   │       └── barrie_weather_2026T*.json
│   │
│   ├── silver/                          # Cleaned, flattened tables
│   │   ├── weather_daily_clean.csv
│   │   ├── weather_daily_clean_duckdb.csv
│   │   └── weather_daily_clean_duckdb.json
│   │
│   └── gold/                            # Analysis-ready (derived columns)
│       ├── weather_analysis_ready.csv
│       ├── weather_analysis_ready_duckdb.csv
│       ├── weather_daily_summary.csv
│       └── weather_daily_summary_duckdb.csv
│
├── ingest/
│   └── ingest_weather.py               # Fetches API, saves to bronze
│
├── transform/
│   ├── transform_weather.py            # Bronze → Silver (Pandas)
│   ├── transform_weather_duckdb.py     # Bronze → Silver (DuckDB SQL)
│   ├── create_gold.py                  # Silver → Gold (Pandas)
│   ├── create_gold_duckdb.py           # Silver → Gold (DuckDB SQL)
│   └── TRANSFORM_DOCUMENTATION.md
│
├── requirements.txt
├── README.md                           # This file
└── analysis_preview.md                 # Part 2 statistical planning
```

---

## Quick Start

### 1. Setup

```bash
# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Run Pipeline

```bash
# Ingest raw data from API (run 2+ times for multiple snapshots)
python ingest/ingest_weather.py

# Transform: Bronze → Silver
python transform/transform_weather.py           # Pandas approach
python transform/transform_weather_duckdb.py    # DuckDB approach

# Create analysis-ready Gold layer
python transform/create_gold.py                 # Pandas approach
python transform/create_gold_duckdb.py          # DuckDB approach
```

---

## Data Pipeline Details

### Bronze Layer
- **Input:** Open-Meteo API (7-day historical weather)
- **Output:** 6 raw JSON files (3 cities × 2 ingestion runs)
- **Records:** 48 total (24 deduplicated)
- **Script:** `ingest/ingest_weather.py`

**Sample record:**
```json
{
  "latitude": 43.690685,
  "longitude": -79.41174,
  "timezone": "America/Toronto",
  "daily": {
    "time": ["2026-03-24", "2026-03-25", ...],
    "temperature_2m_max": [3.4, 5.4, ...],
    "temperature_2m_min": [-7.2, -2.5, ...],
    "precipitation_sum": [0.0, 0.2, ...],
    "wind_speed_10m_max": [22.4, 21.0, ...]
  }
}
```

### Silver Layer
- **Input:** Bronze JSON files
- **Output:** Flat, deduplicated CSV with clear column names
- **Records:** 24 (deduplicated)
- **Deduplication Strategy:** Group by (date, city), keep latest source_file
- **Scripts:** `transform/transform_weather.py`, `transform/transform_weather_duckdb.py`

**Columns:**
```
date, city, latitude, longitude, timezone,
temp_max_celsius, temp_min_celsius, 
precipitation_mm, wind_speed_max_kmh,
source_file, temp_range_celsius
```

**Data Quality:**
- ✓ No missing values (24/24 clean)
- ✓ All types validated
- ✓ Temperature range: -12.0°C to 18.8°C

### Gold Layer
- **Input:** Silver CSV
- **Output:** Analysis-ready tables with **11 derived binary indicators**
- **Records:** 24 city-days + 8 daily aggregates
- **Scripts:** `transform/create_gold.py`, `transform/create_gold_duckdb.py`

**Derived Columns (for Part 2 analysis):**

| Category | Indicator | Threshold | % of Data |
|----------|-----------|-----------|-----------|
| **Temperature** | is_cold_day | temp_min < 0°C | 67% |
| | is_hot_day | temp_max > 20°C | 0% |
| | is_freezing | temp_min ≤ -10°C | 0% |
| | extreme_temp_range | range > 15°C | 4% |
| **Precipitation** | is_rainy_day | precip > 0mm | **54%** |
| | is_wet_day | precip > 5mm | 29% |
| | is_heavy_rain | precip > 10mm | 13% |
| **Wind** | is_windy_day | wind > 30 km/h | 0% |
| **Temporal** | is_weekend | Sat/Sun | 25% |
| | day_of_week | Mon–Sun | – |

---

## Part 2: Statistical Analysis

### Research Question
**Do cities in the Greater Toronto Area experience statistically different minimum temperatures?**

### Hypotheses & Tests

| H₀ | H₁ | Test | Why |
|----|----|------|-----|
| City doesn't affect min temp | At least one city differs | **ANOVA** | Compares 3+ groups |
| Rain & cold are independent | They are associated | **Chi-Square** | Binary × binary |
| Weekday = Weekend temps | They differ | **t-test** | 2-group comparison |

### Data Quality for Analysis
- ✓ 24 clean observations (8 per city)
- ✓ No missing values
- ✓ Temperature precision: 0.1°C
- ✓ Multiple ingestion runs validate pipeline

See [analysis_preview.md](analysis_preview.md) for full planning.

---

## Implementation Approaches

### Pandas (Intuitive, Python-native)
```python
import pandas as pd

df = pd.read_csv("data/silver/weather_daily_clean.csv")
df["is_cold_day"] = (df["temp_min_celsius"] < 0).astype(int)
df.to_csv("data/gold/weather_analysis_ready.csv", index=False)
```

### DuckDB (SQL, optimized for analytics)
```python
import duckdb

sql = """
SELECT 
    date, city, temp_min_celsius,
    CASE WHEN temp_min_celsius < 0 THEN 1 ELSE 0 END as is_cold_day
FROM read_csv_auto('data/silver/weather_daily_clean.csv')
"""
results = duckdb.query(sql).to_df()
```

Both produce identical outputs; choose based on team preference.

---

## Files & Outputs

### Ingestion
- **Script:** `ingest/ingest_weather.py`
- **Output:** `data/bronze/weather/*.json` (6 files)
- **Rows:** 48 (3 cities × 8 days × 2 runs)

### Transformation (Bronze → Silver)
- **Scripts:** `transform/transform_weather.py` + DuckDB variant
- **Output:** `data/silver/weather_daily_clean.csv` (24 rows × 12 cols)
- **Time:** <1 second

### Analysis Ready (Silver → Gold)
- **Scripts:** `transform/create_gold.py` + DuckDB variant
- **Outputs:** 
  - `data/gold/weather_analysis_ready.csv` (24 rows × 20 cols)
  - `data/gold/weather_daily_summary.csv` (8 rows × 7 cols)

---

## Key Metrics

```
Cities:              3 (Toronto, Oshawa, Barrie)
Days:                8 (2026-03-24 to 2026-03-31)
Temperature Range:   -12.0°C to 18.8°C
Total Precipitation: 124.6mm (19 rainy days)
Cold Days:           16/24 (67%)
Weekend Days:        6/24 (25%)
```

---

## Dependencies

- `requests==2.31.0` – API calls
- `pandas==2.2.0` – Data manipulation
- `duckdb==0.10.0` – SQL analytics
- `python-dotenv==1.0.0` – Environment variables

---

## Data Quality Checks

✓ **Completeness:** No missing values (24/24 valid observations)  
✓ **Consistency:** All timestamps in ISO 8601 format  
✓ **Deduplication:** Duplicates removed (48 → 24 records)  
✓ **Type Safety:** All numeric columns properly cast  
✓ **Pipeline Validation:** Ingestion run twice; results consistent  

---

## Limitations

- ⚠️ **Small sample:** Only 8 days (not representative of seasons)
- ⚠️ **Single source:** All data from one API (no validation)
- ⚠️ **Temporal:** May exhibit autocorrelation
- ⚠️ **Geographic:** Only 3 cities (cannot generalize to region)

---

## Next Steps (Future Work)

- [ ] Add more data sources (multiple years, more cities)
- [ ] Build web dashboard (Streamlit/Plotly)
- [ ] Implement machine learning (temperature forecasting)
- [ ] Add automatic daily ingestion (cron job)
- [ ] Compare against ground-truth weather stations

---

## Documentation

- [TRANSFORM_DOCUMENTATION.md](transform/TRANSFORM_DOCUMENTATION.md) – ETL details
- [GOLD_LAYER_DOCUMENTATION.md](GOLD_LAYER_DOCUMENTATION.md) – Analysis-ready schema
- [analysis_preview.md](analysis_preview.md) – Part 2 statistical planning

---

## Author

**Assignment:** STAT Course - Part 1: Data Engineering  
**Date:** March 31, 2026  
**Language:** Python 3.12  
**Tools:** Pandas, DuckDB, Open-Meteo API  

---

## License

MIT License - Free to use and modify.

---

## Questions?

Refer to the documentation files or review the ingestion/transformation scripts for implementation details.
