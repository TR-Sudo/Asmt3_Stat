#!/usr/bin/env python3
import json
import pandas as pd
from pathlib import Path

bronze_dir = Path("data/bronze/weather")
silver_dir = Path("data/silver")
silver_dir.mkdir(parents=True, exist_ok=True)

rows = []
for p in sorted(bronze_dir.glob("*.json")):
    city = p.stem.split("_weather_")[0]
    with open(p, "r") as f:
        raw = json.load(f)

    d = raw.get("daily", {})
    for i, date in enumerate(d.get("time", [])):
        rows.append({
            "date": date,
            "city": city,
            "latitude": raw.get("latitude"),
            "longitude": raw.get("longitude"),
            "timezone": raw.get("timezone"),
            "temp_max_celsius": d.get("temperature_2m_max", [None])[i],
            "temp_min_celsius": d.get("temperature_2m_min", [None])[i],
            "precipitation_mm": d.get("precipitation_sum", [None])[i],
            "wind_speed_max_kmh": d.get("wind_speed_10m_max", [None])[i],
            "source_file": p.name,
        })

df = pd.DataFrame(rows)

df["date"] = pd.to_datetime(df["date"]).dt.date
for col in ["temp_max_celsius", "temp_min_celsius", "precipitation_mm", "wind_speed_max_kmh"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df["temp_range_celsius"] = df["temp_max_celsius"] - df["temp_min_celsius"]

df = df.sort_values(["date", "city"]).reset_index(drop=True)

df.to_csv(silver_dir / "weather_daily_clean.csv", index=False)
print("✓ Saved data/silver/weather_daily_clean.csv")

