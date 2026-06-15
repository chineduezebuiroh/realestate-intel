# Data Inventory v1

## Purpose

This document defines the Phase A data inventory audit for the Real Estate Intelligence Platform.

The purpose of the audit is to document what data exists in the platform, how much history is available, how fresh each source is, and whether each source is suitable for indicator engineering, normalization, and regime scoring.

---

# Audit Outputs

The audit script produces two files:

```text
artifacts/audit/data_inventory.csv
artifacts/audit/indicator_history_inventory.csv
```

---

# Source Inventory

The source inventory summarizes each data source currently loaded into the DuckDB database.

Fields include:

* source_id
* metric_count
* geo_count
* row_count
* first_date
* last_date
* estimated_frequency
* latest_refresh_timestamp

---

# Indicator History Inventory

The indicator history inventory summarizes available history by:

* source_id
* metric_id
* geo_level
* property_type_id
* first_date
* last_date
* observation_count
* approximate_years_available

This file is used to determine whether each indicator can support:

* historical percentiles
* short-term change features
* long-term change features
* multi-year comparisons
* regime backtesting

---

# Why This Matters

Indicator engineering decisions should be based on actual data availability.

Examples:

* Annual indicators may not support 10-year changes if only 15 years of history exist.
* Monthly indicators may support YoY features only when at least 12 months of history exist.
* Historical percentile scoring requires enough observations to be meaningful.
* Some sources update monthly while others update quarterly or annually.

---

# Current Source Families

The platform currently includes:

| Source            | Purpose                                      |
| ----------------- | -------------------------------------------- |
| Redfin            | Housing market metrics                       |
| BLS CES           | Payroll / employment data                    |
| BLS LAUS          | Labor force, employment, unemployment        |
| Census ACS        | Population and income                        |
| Census BPS        | Building permits and construction indicators |
| Census NRC / FRED | Housing starts and completions               |
| BEA QGDP          | Quarterly GDP                                |
| BEA AGDP          | Annual GDP                                   |
| FRED Macro        | Rates, spreads, CPI                          |
| FRED Unemployment | National and state unemployment              |

---

# Revision Awareness

Some sources are revised after initial publication.

Examples:

* BLS CES may revise historical employment series.
* BLS LAUS may revise labor market series.
* BEA GDP may be revised.
* FRED series may reflect revised source data.
* Redfin and ACS are generally treated as published snapshots for platform purposes.

Point-in-time revision handling is outside the scope of v1 but should be considered for future backtesting rigor.

---

# Refresh Integration

The audit should run after successful refresh jobs.

Recommended sequence:

```text
refresh source
validate source
run data inventory audit
upload audit artifacts
```

The audit should not replace source validation. It is a platform-level visibility layer.
