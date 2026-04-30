# Sports Streaming Data Warehouse & Analytics Platform

A dimensional data warehouse and end-to-end ETL pipeline built for **SportsTV Germany**, an OTT platform that distributes amateur sporting event recordings. The project ingests streaming transactions from heterogeneous sources (an operational SQLite OLTP database and a flat CSV export), transforms them into a Kimball-style **star schema** on a cloud-hosted MySQL instance, and produces an automated business-intelligence report in R Markdown.

> **Course:** CS5200 — Database Management Systems (Northeastern University, Fall 2025)
> **Author:** Karthik Bharadwaj _(see [Credits](#credits) for project partner)_

---

## Highlights

- **End-to-end OLTP → OLAP pipeline** that consolidates ~2M+ streaming transactions from two disparate sources into a single analytical warehouse.
- **Kimball dimensional model** (star schema) with five conformed dimensions and a partitioned fact table, optimized for slice-and-dice analytics.
- **MySQL `RANGE` partitioning by year** on the fact table to keep historical queries fast as the dataset grows.
- **Cloud-hosted warehouse** on Aiven MySQL — no local DB setup required to run analytics.
- **Reproducible BI report** generated from R Markdown (`knitr` + `kableExtra`) with charts, KPI tables, and natural-language insights computed live from SQL.
- **Bulk-batched, idempotent ETL** with surrogate-key resolution, data validation, and partition-distribution checks.

---

## Architecture

```text
┌──────────────────────┐     ┌──────────────────────┐
│  SQLite OLTP source  │     │  CSV transactions    │
│  subscribersDB       │     │  (~98K rows / year)  │
│  - streaming_txns    │     │                      │
│  - assets            │     │                      │
│  - countries         │     │                      │
└──────────┬───────────┘     └──────────┬───────────┘
           │                            │
           └─────────────┬──────────────┘
                         ▼
               ┌──────────────────┐
               │   ETL  (R)       │
               │   - Extract      │
               │   - Aggregate    │
               │   - Surrogate    │
               │     key lookup   │
               │   - Bulk insert  │
               └────────┬─────────┘
                        ▼
        ┌──────────────────────────────────┐
        │  MySQL Star Schema (Aiven Cloud) │
        │                                  │
        │   dim_date ─┐                    │
        │   dim_country ┐                  │
        │   dim_sport   ├─►  fact_streaming_analytics
        │   dim_device  ┘    (RANGE partitioned by year)
        │   dim_quality ┘                  │
        └──────────────┬───────────────────┘
                       ▼
               ┌────────────────┐
               │  R Markdown BI │
               │  Report (HTML) │
               └────────────────┘
```

---

## The Star Schema

Modeled following **Kimball's dimensional design** principles. The fact table stores pre-aggregated daily metrics; dimensions are keyed by surrogate `*_key` columns to decouple the warehouse from source-system identifiers.

### Dimensions

| Table | Grain | Notable attributes |
| --- | --- | --- |
| `dim_date` | One row per calendar date | `year`, `quarter`, `month`, `week`, `day_of_week`, `day_name` |
| `dim_country` | One row per country | `country_id`, `country` |
| `dim_sport` | One row per sport category | `sport` |
| `dim_device` | One row per device type | `device_type` (Smart TV, Mobile, Laptop, …) |
| `dim_quality` | One row per stream quality | `quality_streamed` (4K, HD, Auto, …) |

### Fact table — `fact_streaming_analytics`

Grain: **one row per (date × country × sport × device × quality)**.

Stored measures:

- `transaction_count`
- `total_minutes_streamed`
- `avg_minutes_per_transaction`
- `completed_count`
- `completion_rate` (%)

**Partitioning:** `PARTITION BY RANGE(partition_year)` with one partition per year (`p2020`…`p2025`) plus a `p_future` catch-all. This enables MySQL **partition pruning**, so queries filtered by year touch only the relevant partition. Foreign keys to dimensions are intentionally omitted because MySQL does not allow FK constraints on partitioned tables — referential integrity is instead enforced by the ETL layer at load time.

**Indexes:** single-column indexes on every dimension key, plus composite indexes on `(date_key, country_key)`, `(date_key, sport_key)`, and `(date_key, country_key, sport_key)` for the heaviest query patterns in the BI report.

---

## ETL Pipeline

Implemented in R using `DBI`, `RMySQL`, and `RSQLite`.

### 1. Schema creation — [createStarSchema.R](createStarSchema.R)

- Connects to the cloud MySQL instance.
- Drops any prior copies of the schema for idempotent rebuilds.
- Creates dimension tables with `AUTO_INCREMENT` surrogate keys, `UNIQUE` natural-key constraints, and per-attribute indexes.
- Creates the partitioned fact table.
- Verifies partition layout via `INFORMATION_SCHEMA.PARTITIONS`.

### 2. Data load — [loadAnalyticsDB.R](loadAnalyticsDB.R)

A six-stage ETL job:

1. **Populate dimensions** — Union the distinct values from both sources (SQLite + CSV) for each dimension. Date attributes (year/quarter/week/day-of-week) are derived from raw dates in R. Inserts are batched with `INSERT IGNORE` for idempotency.
2. **Extract from SQLite** — A single SQL query joins `streaming_txns ⋈ assets ⋈ countries` so the join is pushed down to the database engine instead of being done row-wise in R.
3. **Extract from CSV** — The CSV is staged into a temp SQLite table, then joined against the same `assets`/`countries` tables to enrich it with sport and country attributes (since the CSV only carries `asset_id`).
4. **Aggregate** — Combined transactions are rolled up to the fact-table grain using `aggregate()` with sum measures, and `avg_minutes_per_transaction` and `completion_rate` are derived.
5. **Surrogate-key lookup** — Each dimension's natural keys are joined back to fetch the warehouse-assigned `*_key`, replacing source IDs with warehouse keys.
6. **Bulk load** — Fact rows are inserted in batches of 15,000 with multi-row `INSERT … VALUES ()` statements for throughput.

### 3. Validation

After load, the script reports:

- Total fact rows and total transactions in the warehouse
- Source vs. warehouse match rate (target: > 99%)
- Per-partition row counts (verifies partition pruning is wired up correctly)
- Dimension cardinalities

---

## Business Analysis Report

[BusinessAnalysis.Rmd](BusinessAnalysis.Rmd) — knit to HTML.

The report is fully driven by live SQL against the warehouse — no hard-coded numbers. Every KPI, table, and chart is computed from the fact/dimension tables at render time.

Sections:

1. **Executive summary** — total transactions, total hours streamed, average session duration, completion rate, geographic and content coverage, unique users.
2. **Growth by sport** — multi-year trends in streaming events and streaming time across Ice Hockey, Inline Hockey, and Ski Jumping; top sport and peak year highlighted.
3. **Weekly volume in the most recent year** — per-week transaction counts with peak/low-week annotations and week-to-week variance.
4. **Sport × country breakdown** — average session duration and total volume across countries.
5. **Day-of-week analysis** — peak streaming days over the trailing three years, drilled down by sport and country.
6. **Device & quality** — share of streams by device type and stream quality, with average session duration per device.

A pre-rendered copy is checked in at [BusinessAnalysis.html](BusinessAnalysis.html).

---

## Tech Stack

- **R** — ETL and reporting (`DBI`, `RMySQL`, `RSQLite`, `knitr`, `kableExtra`)
- **MySQL 8** (Aiven managed cloud) — analytical warehouse with range partitioning
- **SQLite** — operational source database
- **R Markdown** — reproducible BI report
- **Star schema / Kimball methodology** — dimensional modeling

---

## Repository Layout

```text
.
├── createStarSchema.R          # DDL: dimension tables + partitioned fact table
├── loadAnalyticsDB.R           # ETL pipeline (extract, conform, aggregate, load)
├── BusinessAnalysis.Rmd        # BI report (source)
├── BusinessAnalysis.html       # BI report (pre-rendered)
├── sandbox.R                   # Connection smoke test
├── streaming-warehouse.Rproj   # RStudio project file
├── .Renviron.example           # Template for DB credentials (copy to .Renviron)
├── .gitignore
└── data/                       # Source data — git-ignored, not committed
    ├── subscribersDB.sqlitedb              # OLTP source (SQLite)
    └── new-streaming-transactions-98732.csv# CSV source
```

---

## Running the Project

### 1. Configure database credentials

The scripts read MySQL credentials from environment variables — nothing is hardcoded. Copy the template and fill in your own values:

```bash
cp .Renviron.example .Renviron
# then edit .Renviron with your MYSQL_HOST / MYSQL_PORT / MYSQL_DB / MYSQL_USER / MYSQL_PWD
```

`.Renviron` is auto-loaded by R at session start and is git-ignored.

### 2. Provide the source data

The ETL expects two files inside `data/` (not committed to the repo):

- `data/subscribersDB.sqlitedb` — operational SQLite database
- `data/new-streaming-transactions-98732.csv` — streaming transactions export

### 3. Run

```r
# Build the warehouse schema
source("createStarSchema.R")

# Run the ETL load
source("loadAnalyticsDB.R")

# Render the BI report to HTML
rmarkdown::render("BusinessAnalysis.Rmd")
```

---

## Key Design Decisions

- **Why a star schema over 3NF?** Optimized for ad-hoc analytical queries (slice/dice/roll-up) rather than transactional writes; minimizes joins for BI tooling.
- **Why partition by year?** Time-bounded queries dominate the workload (yearly growth, weekly trends in the most recent year, trailing-3-year day-of-week analysis). `RANGE(year)` enables partition pruning so older data doesn't slow current analytics.
- **Why aggregate at ETL time?** The fact grain (date × country × sport × device × quality) collapses millions of raw transactions into a much smaller fact table while preserving all dimensions needed for the report — trading some flexibility for major query-time speedups.
- **Why surrogate keys?** Insulates the warehouse from source-system key changes, supports slowly changing dimensions in the future, and keeps fact-table rows narrow.
- **Why join inside the source DB during extraction?** Pushing the `streaming_txns ⋈ assets ⋈ countries` join down to SQLite is dramatically faster than pulling raw tables into R and joining in memory.

---

## Authors

- [Karthik Bharadwaj](https://github.com/karthik-b-2001)
- [Akshay Mambakkam Sridharan](https://github.com/Akshay-Mambakkam-Sridharan)