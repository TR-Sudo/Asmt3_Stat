# Weather Data Pipeline for Statistical Analysis

## Overview

This project implements a comprehensive data engineering pipeline for weather data analysis, following the Bronze-Silver-Gold data lakehouse architecture. It ingests historical weather data from the Open-Meteo API, processes it through transformation layers, and prepares datasets for statistical hypothesis testing.

### Key Features
- **Multi-layer Data Architecture**: Bronze (raw), Silver (cleaned), Gold (analysis-ready)
- **Statistical Readiness**: Derived indicators for hypothesis testing
- **Geographic Focus**: Weather data for Toronto, Oshawa, and Barrie (Greater Toronto Area)
- **Time Period**: March 24-31, 2026

## Data Source
- **API**: [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
- **No API Key Required**: Free tier usage
- **Data Points**: Daily temperature (max/min), precipitation, wind speed

## Project Structure

```
.
├── data/
│   ├── bronze/weather/          # Raw JSON API responses
│   ├── silver/                  # Cleaned CSV data
│   └── gold/                    # Analysis-ready datasets with derived features
├── ingest/
│   └── ingest_weather.py        # Data ingestion script
├── transform/
│   ├── transform_weather.py     # Bronze to Silver transformation
│   ├── create_gold.py           # Silver to Gold transformation
│   └── TRANSFORM_DOCUMENTATION.md
├── notebooks/                   # Jupyter notebooks for analysis
├── requirements.txt             # Python dependencies
├── analysis_preview.md          # Statistical analysis planning
├── GOLD_LAYER_DOCUMENTATION.md  # Gold layer details
└── README.md
```

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd Asmt3_Stat
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Data Pipeline Execution

1. **Ingest Raw Data**:
   ```bash
   python ingest/ingest_weather.py
   ```
   Run multiple times to create snapshots for deduplication testing.

2. **Transform to Silver Layer**:
   ```bash
   python transform/transform_weather.py
   ```

3. **Create Gold Layer**:
   ```bash
   python transform/create_gold.py
   ```

### Data Layers

#### Bronze Layer
- Raw API responses stored as JSON
- Contains nested data structures
- Preserves original timestamps and metadata

#### Silver Layer
- Flattened, deduplicated tabular data
- Clean column names and data types
- Removes duplicates based on date and city

#### Gold Layer
- Analysis-ready datasets with derived features
- Binary indicators for statistical testing
- Aggregated summaries

## Statistical Analysis

### Research Question
Do cities in the Greater Toronto Area experience statistically different minimum temperatures?

### Planned Tests
- **ANOVA**: Compare minimum temperatures across cities
- **Chi-Square**: Test association between rain and cold days
- **t-test**: Compare weekday vs. weekend temperatures

### Derived Indicators
- Temperature: cold days, hot days, freezing conditions, extreme ranges
- Precipitation: rainy days, wet days, heavy rain
- Wind: windy days
- Temporal: weekends, day of week

## Technologies

- **Python**: Core programming language
- **Pandas**: Data manipulation and analysis
- **Requests**: HTTP API calls
- **Open-Meteo API**: Weather data source

## Data Quality

- **Completeness**: No missing values in processed datasets
- **Accuracy**: Validated against API specifications
- **Consistency**: Standardized units and formats
- **Timeliness**: Current data for analysis period

## Documentation

- [Transform Documentation](transform/TRANSFORM_DOCUMENTATION.md)
- [Gold Layer Documentation](GOLD_LAYER_DOCUMENTATION.md)
- [Analysis Preview](analysis_preview.md)

## License

This project is for educational purposes as part of a statistical analysis assignment.  

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

**Assignment:** STATS Course
**Date:** March 31, 2026  
**Language:** Python 3.12  
**Tools:** Pandas, Open-Meteo API  

---

## License

MIT License - Free to use and modify.

---

## Questions?

Refer to the documentation files or review the ingestion/transformation scripts for implementation details.
