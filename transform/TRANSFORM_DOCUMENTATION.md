# Weather Data Transformation Documentation

## Overview
This directory contains Python scripts that transform raw weather data from the Bronze layer into cleaned, structured data in the Silver layer.

## Scripts

### 1. `transform_weather.py` (pandas-based)
**Method:** Pandas DataFrame transformation  
**Input:** Raw JSON files from `data/bronze/weather/`  
**Output:** 
- `data/silver/weather_daily_clean.csv`

**Processing Steps:**
1. **Parse JSON**: Extract nested daily data arrays (time, temperature, precipitation, wind)
2. **Flatten structure**: Convert arrays into individual records
3. **Add metadata**: Include latitude, longitude, timezone, source filename
4. **Type conversion**: Cast all numeric fields to float, dates to datetime
5. **Derivation**: Calculate temperature range (`temp_max - temp_min`)
6. **Sorting**: Order by date and city for consistency

**Columns in Silver Layer:**
| Column | Type | Source | Notes |
|--------|------|--------|-------|
| date | date | time array | Converted from ISO 8601 string |
| city | string | filename | Extracted from JSON filename |
| latitude | float | API metadata | Location coordinate |
| longitude | float | API metadata | Location coordinate |
| timezone | string | API metadata | IANA timezone |
| temp_max_celsius | float | daily.temperature_2m_max | Rounded to 1 decimal |
| temp_min_celsius | float | daily.temperature_2m_min | Rounded to 1 decimal |
| temp_range_celsius | float | calculated | diff of max-min |
| precipitation_mm | float | daily.precipitation_sum | Rounded to 1 decimal |
| wind_speed_max_kmh | float | daily.wind_speed_10m_max | Rounded to 1 decimal |
| source_file | string | metadata | For tracking/auditing |

**Data Quality:**
- ✓ No missing values (Open-Meteo provides complete daily data)
- ✓ All numeric fields successfully cast
- ✓ Date ranges: 2026-03-24 to 2026-03-31
- ✓ 3 cities × 8 days × 2 ingestion runs = 48 records

---

### 2. `transform_weather_duckdb.py` (DuckDB SQL-based)
**Method:** DuckDB SQL transformation  
**Input:** Raw JSON files from `data/bronze/weather/`  
**Outputs:**
- `data/silver/weather_daily_clean_duckdb.csv`
- `data/silver/weather_daily_clean_duckdb.json`

**SQL Query:**
```sql
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
```

**Additional Features:**
- SQL-based data quality checks
- Rounding of decimal values to 1 place
- Processing timestamp audit column
- WHERE clause filters null values

**Data Quality Report:**
```
total_records: 48
cities: 3
dates: 8
earliest_date: 2026-03-24
latest_date: 2026-03-31
missing_temp_max: 0
missing_precip: 0
avg_max_temp: 7.4°C
min_temp: -12.0°C
max_temp: 18.8°C
max_precip_mm: 20.1mm
```

---

## Design Decisions

### Column Naming Convention
- Clear, descriptive names with units: `temp_max_celsius`, `precipitation_mm`
- Consistency across scripts
- Suffix metric units for clarity

### Data Type Handling
- **Dates**: Stored as DATE (YYYY-MM-DD) for easier filtering
- **Temperatures**: Float with 1 decimal precision
- **Precipitation/Wind**: Float with 1 decimal precision
- **Categorical**: VARCHAR for city and timezone

### Missing Value Strategy
- Open-Meteo provides complete daily aggregations; no values are missing
- WHERE clauses filter only logically invalid records (e.g., NULL temperatures)
- All rows retained unless explicitly filtered

### Deduplication
- Note: Since we ran ingestion twice on the same day with same date range,
  both runs appear in Silver layer (same date but different `source_file`)
- This is intentional for pipeline auditing

---

## Usage

### Run Pandas Transformation:
```bash
python transform/transform_weather.py
```

### Run DuckDB Transformation:
```bash
python transform/transform_weather_duckdb.py
```

### Expected Output:
```
Processing barrie...
Processing oshawa...
Processing toronto...

🔍 Missing Values Report:
✓ No missing values found

✓ Saved data\silver\weather_daily_clean.csv

📊 Summary Statistics:
  Rows: 48
  Date range: 2026-03-24 to 2026-03-31
  Cities: ['barrie', 'oshawa', 'toronto']
  Temp range: -2.5°C to 18.8°C
  Max precipitation: 20.1mm

✓ Transformation complete!
```

---

## Bronze → Silver Data Flow

```
data/bronze/weather/
├── toronto_weather_2026-03-31T21-35-47.json
│   └── Parse JSON
│       ├── Extract daily arrays
│       ├── Flatten nested structure
│       └── Add metadata fields
│
├── oshawa_weather_2026-03-31T21-35-47.json
│   └── [Same process]
│
└── barrie_weather_2026-03-31T21-35-47.json
    └── [Same process]
    
    ↓ All records combined ↓

Transformation (Pandas or DuckDB)
├── Type casting
├── Date conversion
├── Rounding precision
├── Sorting/ordering
└── Validation

    ↓ Output ↓

data/silver/
├── weather_daily_clean.csv
├── weather_daily_clean_duckdb.csv
└── weather_daily_clean_duckdb.json
```

---

## Next Steps (Gold Layer)
The Silver layer can be further aggregated in the Gold layer for:
- Monthly/quarterly climate summaries
- City comparisons and rankings
- Time-series analysis
- Dashboard/visualization data
