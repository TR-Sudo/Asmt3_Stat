# Analysis Preview Memo

## Part 2 Planning Questions

1. What is one statistical question you plan to answer in Part 2?
- Do minimum temperatures differ between Toronto, Oshawa, and Barrie? This asks if location (in the GTA) affects temperature.

2. What is your outcome variable?
- `temp_min_celsius` (continuous): daily minimum temperature in Celsius.

3. What is your grouping variable, if any?
- `city` (categorical with 3 levels).

4. What is one binary variable you created, and why?
- `is_cold_day` where `temp_min_celsius < 0`. It allows testing frost risk and comparing cold-day frequency across cities.

5. What null and alternative hypotheses might you test?
- H₀: mean `temp_min_celsius` is equal across all three cities.
- H₁: at least one city has a different mean `temp_min_celsius`.

6. Which test do you think fits best, and why?
- One-Way ANOVA fits best because we compare one continuous outcome across three independent groups.

---

## Pipeline support for this plan
- Gold table includes `city`, `temp_min_celsius`, and `is_cold_day` so analysis variables are already prepared.
- Derived binary flag `is_cold_day` supports secondary analyses (proportion comparisons, chi-square tests).
- Data cleaning and alignment in silver/gold stages set up fast hypothesis testing in Part 2.

---

**Status:** Planning memo complete; ready to finalize Gold table and run Part 2 tests.
