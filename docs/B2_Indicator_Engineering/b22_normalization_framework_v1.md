# B2.2 Normalization Framework v1

## Objective

Define how raw indicators are transformed into standardized signals that can be compared across time and ultimately consumed by the Regime Engine.

The purpose of normalization is not to compare geographies against one another.

The purpose is to determine whether a geography is currently strong, weak, accelerating, or deteriorating relative to its own historical behavior.

---

# Core Philosophy

Normalization shall be based on historical context.

Each geography should be evaluated against its own history rather than against other geographies.

Example:

* 6% unemployment may be normal in one market.
* 6% unemployment may be extremely elevated in another.

Historical normalization preserves local context and avoids distortions caused by structural differences between markets.

---

# Primary Normalization Method

For each feature:

```text
Raw Feature
    →
Historical Distribution
    →
Historical Percentile
    →
Direction-Adjusted Signal
```

Example:

```text
County:
Montgomery County, MD

Feature:
Inventory Growth YoY

Current:
+18%

Historical Percentile:
85th percentile

Signal:
Strongly Positive
```

---

# Historical Percentile Framework

Percentiles shall be calculated using the available historical series for the specific:

```text
Metric
+
Geography
+
Property Type
```

Examples:

```text
Inventory
Washington DC
ALL Properties

Median Sale Price
Arlington County
SFH

Unemployment Rate
Virginia
ALL Properties
```

Each series maintains its own historical distribution.

---

# Feature Construction Framework

The default framework shall use three feature families.

## 1. Level

Current value relative to history.

Examples:

```text
Current Unemployment Rate
Current Inventory
Current GDP
Current Population
Current Income
```

Output:

```text
Historical Percentile(Level)
```

---

## 2. Short-Term Change

Measures recent acceleration or deceleration.

Examples:

```text
MoM Change
QoQ Change
YoY Change
```

Depends on source frequency.

Output:

```text
Historical Percentile(Short-Term Change)
```

---

## 3. Long-Term Change

Measures structural trend.

Examples:

```text
YoY Change
3-Year Change
5-Year Change
10-Year Change
```

Selection depends on data frequency and available history.

Output:

```text
Historical Percentile(Long-Term Change)
```

---

# Frequency-Specific Rules

## Monthly Indicators

Examples:

```text
Redfin
CES
LAUS
FRED
Building Permits
Housing Starts
```

Features:

```text
Level
MoM Change
YoY Change
```

---

## Quarterly Indicators

Examples:

```text
BEA Quarterly GDP
```

Features:

```text
Level
QoQ Change
YoY Change
```

No monthly interpolation shall be used for regime scoring.

---

## Annual Indicators

Examples:

```text
Population
Income
ACS Variables
Annual GDP
```

Features:

```text
Level
YoY Change
5-Year Change
```

Longer-term horizons may be added once source histories are fully audited.

---

# Minimum History Requirements

## Monthly

Minimum:

```text
36 observations
```

Preferred:

```text
120+ observations
```

---

## Quarterly

Minimum:

```text
20 observations
```

Preferred:

```text
100+ observations
```

---

## Annual

Minimum:

```text
10 observations
```

Preferred:

```text
50+ observations
```

---

# Directionality Rules

Indicators shall be tagged as either:

## Positive Direction

Higher values are favorable.

Examples:

```text
Employment Growth
GDP Growth
Population Growth
Income Growth
Housing Starts
Building Permits
```

---

## Negative Direction

Higher values are unfavorable.

Examples:

```text
Unemployment Rate
Inventory Growth
Months of Supply
Days on Market
```

Percentiles shall be inverted where necessary so that:

```text
Higher normalized score
=
More favorable condition
```

across all indicators.

---

# Property Type Handling

Macro regime features shall use:

```text
Property Type = ALL
```

Property-type-specific indicators shall be reserved for:

```text
Local Scoring
Asset Scoring
Property-Type Overlays
```

Property type shall not influence the core Macro Regime Engine.

---

# Deferred Items

The following concepts are intentionally deferred from v1:

```text
Peer Percentile Ranking
Cross-Sectional Ranking
Z-Scores
Composite Scores
Regime Classification
```

These topics belong to later phases of the architecture.

---

# v1 Output

Each engineered feature should ultimately produce:

```text
Metric
Geography
Property Type
Feature Family
Raw Value
Historical Percentile
Direction-Adjusted Percentile
```

These outputs become inputs into the Regime Engine.
