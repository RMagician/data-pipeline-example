# Choose Hybrid ELT over Pure ETL

- **Status:** Accepted  
- **Date:** 2025-09-06  

## Context
We need to establish the high-level design pattern for our data pipeline.  

Two industry-standard approaches exist:  

- **ETL (Extract → Transform → Load):** Transform data outside the warehouse (e.g., in Python/Spark) and load only curated results.  
- **ELT (Extract → Load → Transform):** Load raw data into the warehouse first, then transform it inside the warehouse.  

Our stack consists of Python + Airflow for ingestion/orchestration and Postgres as the warehouse. Some transformations are significantly easier outside SQL (Excel quirks, locale parsing, complex text cleanup), while others are best left to SQL in the database (foreign key resolution, SCD handling, relational integrity, set-based deduplication).  

## Decision
We will adopt a **Hybrid ELT approach**:  

- **Bronze and Raw Parquet (outside warehouse)**  
  - Ingestion stores both raw (as-is) and bronze (parsed/normalized) data as Parquet in object storage for auditability and replay.  

- **Bronze→Silver in Python**  
  - Parsing, type coercion, locale normalization, light validations, and schema enforcement.  
  - Load cleaned rows (Silver) into Postgres staging tables.  
  - Invalid rows are captured in `silver_rejects`.  

- **Silver→Gold in Postgres**  
  - Use SQL to resolve foreign keys, apply relational rules, deduplicate, and perform Slowly Changing Dimension (SCD) merges.  
  - Facts: row-level validation, good rows inserted, bad rows rejected.  
  - Dimensions: **SCD2 is the default**, with batch-level all-or-nothing merges to preserve historical correctness. Explicit Type 1 dimensions may use row-level validation and upserts.  

## Consequences
**Positive**  
- Plays to the strengths of each tool: Python for parsing/cleanup, Postgres for set-based relational work.  
- Full audit trail: raw → bronze → silver → gold, all preserved and replayable.  
- Dimensions have a consistent, history-preserving default (SCD2) unless marked otherwise.  
- Facts always load deterministically: either a full row goes in or it is rejected.  
- Easier reprocessing and debugging: Parquet storage + rejects + run IDs.  

**Negative**  
- Slightly more complex pipeline (Python + SQL steps instead of one or the other).  
- Higher storage footprint (retaining raw, bronze, silver, gold).  
- Requires clear documentation of which dims are Type 1 exceptions.  

**Neutral**  
- Warehouse compute costs may rise slightly compared to pure ETL, but offset by reduced Python complexity.  
- Hybrid ELT aligns with industry best practices (dbt-style modeling, lakehouse Bronze/Silver/Gold), but we are not yet adopting dbt itself.  

## Alternatives Considered
- **Pure ETL:** Transform all data in Python and only load curated data into Postgres. Simpler but harder to maintain lineage, replay, and relational integrity.  
- **Pure ELT:** Land raw data directly into Postgres and transform entirely with SQL/dbt. Avoids Python complexity but makes Excel/locale/text parsing awkward.  
- **Hybrid (chosen):** Python handles Bronze→Silver, Postgres handles Silver→Gold, with a default of SCD2 dimensions. This provides the best balance between developer productivity, auditability, and warehouse efficiency.  
