# Weather Transformation Notes

- `transform_weather.py` (Pandas) ingest JSON from `data/bronze/weather/` and output cleaned CSV in `data/silver/`.
- Silver schema includes `date, city, latitude, longitude, timezone, temp_max_celsius, temp_min_celsius, temp_range_celsius, precipitation_mm, wind_speed_max_kmh, source_file`.

## Key points
- Output records: 48 rows (3 cities × 8 days × 2 ingests).
- Data already normalized, typed, and sorted.
- Decimal values rounded to 1 place; date uses YYYY-MM-DD format.
- Duplicate rows are expected for audit (`source_file` differences).

## Next step
- Build Gold layer from Silver with per-city daily summaries, join with analytic flags, and run planned hypothesis tests.
