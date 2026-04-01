# Analysis Preview Memo

## Research Question
**Do cities in the Greater Toronto Area (Toronto, Oshawa, Barrie) experience statistically different minimum temperatures?**

---

## Key Variables

| Variable | Type | Use |
|----------|------|-----|
| `temp_min_celsius` | Continuous | Outcome (what we're measuring) |
| `city` | Categorical (3 levels) | Grouping (explains geographic/lake effects) |
| `is_cold_day` (temp_min < 0°C) | Binary | Tests frost/freeze risk; 67% of data |
| `is_rainy_day` (precip > 0mm) | Binary | Explores weather associations; 54% of data |
| `is_weekend` | Binary | Control for day-of-week effects |

---

## Hypotheses & Statistical Tests

### H₁: City Effect on Temperature
- **H₀:** Mean minimum temp is same across 3 cities
- **H₁:** At least one city differs
- **Test:** **One-Way ANOVA** (compares 3+ groups, continuous outcome)
- **If significant:** Tukey HSD for pairwise comparisons

### H₂: Rain–Temperature Association
- **H₀:** Cold days and rainy days are independent
- **H₁:** They are associated
- **Test:** **Chi-Square Test** (binary × binary variables)

### H₃: Weekend Effect
- **H₀:** Temps don't differ weekday vs. weekend
- **H₁:** They do differ
- **Test:** **Independent t-test** (2 groups, continuous outcome)

---

## Why This Pipeline Supports Analysis

| Gold Column | Purpose |
|---|---|
| `city` | Enables ANOVA grouping |
| `temp_min_celsius` | Provides continuous outcome |
| `is_cold_day`, `is_rainy_day` | Binary indicators for association tests |
| `is_weekend`, `day_of_week` | Temporal stratification to control confounds |
| No missing values (24/24 clean) | Ready for analysis without cleaning |

---

## Data Quality Check
- ✓ No missing values
- ✓ 8 observations per city (balanced)
- ✓ Temperature range: -12°C to 18.8°C
- ⚠ Only 8 days (short window; assumes no autocorrelation)

---

**Status:** Ready for Part 2 Statistical Analysis
