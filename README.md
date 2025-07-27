# ✈️ Flight Bookings Data Engineering Project

## 📑 Contents:
- Code
- Data Files

---

## 📌 Project Overview

The **Flight Bookings Data Engineering Project** is built to ingest and process incremental daily flight booking data from CSV files using **Databricks Delta Live Tables (DLT)**. The pipeline follows the **Lakehouse Medallion Architecture (Bronze → Silver → Gold)** to model a **normalized star schema** with one fact table (**Bookings**) and three dimension tables (**Flights, Airports, Passengers**).

### Highlights
- ✅ **Real-time incremental ingestion** using **Autoloader (cloudFiles)**
- ✅ **Bronze → Silver → Gold medallion pipeline** using **Delta Live Tables**
- ✅ **SCD Type-1** implementation on dimension tables via `create_auto_cdc_flow`
- ✅ **Data validation rules** on fact table using `dlt.expect_all_or_drop`
- ✅ **Structured streaming pipeline** for near real-time updates
- ✅ **Fully normalized schema** with **referential integrity** between fact and dimension tables

---

## 🚀 Key Features

- **Tech Stack**: Databricks, Delta Lake, PySpark, Autoloader, Delta Live Tables (DLT), Structured Streaming

### Data Storage & Layers

- **Bronze Layer**: Raw ingestion of daily CSV drops from S3 (`cloudFiles` format)
- **Silver Layer**: Cleaned and type-enforced tables with transformations applied
- **Gold Layer**: Modeled tables with quality rules and SCD-1 logic

---

## 🧱 Star Schema Design

### Fact Table:
**`gold.gold_bookings`**
- Fields: `booking_id`, `passenger_id`, `flight_id`, `amount`, `booking_date`, etc.
- **Validation**: Null checks via `dlt.expect_all_or_drop`

### Dimension Tables (SCD-1 Applied):
- **`gold.gold_passengers`**: Unique by `passenger_id`
- **`gold.gold_flights`**: Unique by `flight_id`
- **`gold.gold_airports`**: Unique by `airport_id`

> 🔹 The fact table references dimension keys; the schema is **fully normalized** to maintain **integrity** and support **analytical queries**.

---

## 🔁 Incremental Ingestion & SCD Logic

### Ingestion
- Files are dropped daily into S3 and **automatically picked up via Autoloader**
- Each table has its own ingestion path in the `rawvolume` folder

### CDC & SCD Type-1
- Implemented using `dlt.create_auto_cdc_flow` for **Flights, Passengers, Airports**
- Uses **primary key-based deduplication** and **overwrites previous records (Type-1)**

### Validation Rules on Bookings
**Rules**:
- `booking_id IS NOT NULL`
- `passenger_id IS NOT NULL`

> Applied in the `gold.gold_bookings` table using `@dlt.expect_all_or_drop`
