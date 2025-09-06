[CmdletBinding()]
param(
  [string]$RootPath = "."
)

$ErrorActionPreference = "Stop"

function New-Dir($Path) {
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

function New-FileUtf8($Path, [string]$Content = "") {
  $dir = Split-Path -Parent $Path
  if ($dir) { New-Dir $dir }
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType File -Path $Path -Force | Out-Null
    if ($Content -ne "") {
      $Content | Out-File -FilePath $Path -Encoding utf8
    }
  }
}

# Root
New-Dir $RootPath

# --- airflow ---
$airflow = Join-Path $RootPath "airflow"
New-Dir $airflow
New-Dir (Join-Path $airflow "dags")
New-Dir (Join-Path $airflow "plugins")

New-FileUtf8 (Join-Path $airflow "dags\orders_dag.py") @'
# Airflow DAG: orders – ingest -> bronze_to_silver -> dims -> facts
# (Fill with your real tasks/operators)
'@

New-FileUtf8 (Join-Path $airflow "dags\customers_dag.py") @'
# Airflow DAG: customers
'@

New-FileUtf8 (Join-Path $airflow "dags\products_dag.py") @'
# Airflow DAG: products
'@

New-FileUtf8 (Join-Path $airflow "plugins\run_sql_operator.py") @'
# Optional custom operator to run SQL files with params
'@

New-FileUtf8 (Join-Path $airflow "plugins\staging_helpers.py") @'
# Optional helpers for staging loads
'@

# --- alembic ---
$alembic = Join-Path $RootPath "alembic"
New-Dir $alembic
New-Dir (Join-Path $alembic "versions")
New-FileUtf8 (Join-Path $alembic "env.py") @'
# Alembic env.py – configure SQLAlchemy connection here
'@

# --- src ---
$src = Join-Path $RootPath "src"
New-Dir $src

# config
New-Dir (Join-Path $src "config")
New-FileUtf8 (Join-Path $src "config\settings.py") @'
# Centralized settings (env vars, batch sizes, paths)
'@
New-FileUtf8 (Join-Path $src "config\logging.py") @'
# Logging config
'@

# lake
New-Dir (Join-Path $src "lake")
New-FileUtf8 (Join-Path $src "lake\paths.py") @'
# Parquet naming/versioning helpers
'@
New-FileUtf8 (Join-Path $src "lake\io.py") @'
# write_parquet/read_parquet, checksum utils
'@

# sources
$sources = Join-Path $src "sources"
New-Dir $sources

# excel_orders
New-Dir (Join-Path $sources "excel_orders")
New-FileUtf8 (Join-Path $sources "excel_orders\ingest.py") @'
# Read Excel -> raw DF + metadata
'@
New-FileUtf8 (Join-Path $sources "excel_orders\bronze_to_silver.py") @'
# Parse/normalize/type/enum/null policy
'@

# csv_customers
New-Dir (Join-Path $sources "csv_customers")
New-FileUtf8 (Join-Path $sources "csv_customers\ingest.py") @'
# Read CSV -> raw DF
'@
New-FileUtf8 (Join-Path $sources "csv_customers\bronze_to_silver.py") @'
# Clean to Silver
'@

# api_products
New-Dir (Join-Path $sources "api_products")
New-FileUtf8 (Join-Path $sources "api_products\ingest.py") @'
# Call API -> raw DF
'@
New-FileUtf8 (Join-Path $sources "api_products\bronze_to_silver.py") @'
# Clean to Silver
'@

# contracts
$contracts = Join-Path $src "contracts"
New-Dir $contracts
New-FileUtf8 (Join-Path $contracts "orders_schema.py") @'
# Pandera schema for Silver: orders
'@
New-FileUtf8 (Join-Path $contracts "customers_schema.py") @'
# Pandera schema for Silver: customers
'@
New-FileUtf8 (Join-Path $contracts "products_schema.py") @'
# Pandera schema for Silver: products
'@

# db
$db = Join-Path $src "db"
New-Dir $db
New-FileUtf8 (Join-Path $db "engine.py") @'
# SQLAlchemy engine/session factory
'@
New-FileUtf8 (Join-Path $db "uow.py") @'
# Unit of Work (commit/rollback/retry)
'@

# db/models
New-Dir (Join-Path $db "models")
New-FileUtf8 (Join-Path $db "models\order.py") @'
# ORM model (optional)
'@
New-FileUtf8 (Join-Path $db "models\customer.py") @'
# ORM model (optional)
'@
New-FileUtf8 (Join-Path $db "models\product.py") @'
# ORM model (optional)
'@

# db/sql (Silver->Gold SQL executed from Python/Airflow)
$dbsql = Join-Path $db "sql"
New-Dir $dbsql

# facts/sales SQL chain
$facts = Join-Path $dbsql "facts"
New-Dir $facts
$sales = Join-Path $facts "sales"
New-Dir $sales
New-FileUtf8 (Join-Path $sales "01_resolve_tmp.sql") @'
-- Create/populate temp/UNLOGGED table with FK joins + flags
'@
New-FileUtf8 (Join-Path $sales "02_insert_good.sql") @'
-- UPSERT good rows into fact table
'@
New-FileUtf8 (Join-Path $sales "03_insert_rejects.sql") @'
-- Insert rejects with reasons
'@
New-FileUtf8 (Join-Path $sales "99_cleanup_tmp.sql") @'
-- Drop/truncate temp tables
'@

# dims/customer SQL chain
$dims = Join-Path $dbsql "dims"
New-Dir $dims
$customer = Join-Path $dims "customer"
New-Dir $customer
New-FileUtf8 (Join-Path $customer "01_candidates_tmp.sql") @'
-- Stage candidates for this chunk
'@
New-FileUtf8 (Join-Path $customer "02_validate.sql") @'
-- Uniqueness/parent checks
'@
New-FileUtf8 (Join-Path $customer "03_merge_scd.sql") @'
-- Type-1/SCD2 merge (transactional)
'@

# db/sql/utils
$utils = Join-Path $dbsql "utils"
New-Dir $utils
New-FileUtf8 (Join-Path $utils "analyze_tmp.sql") @'
-- ANALYZE temp tables if needed
'@
New-FileUtf8 (Join-Path $utils "helpers.sql") @'
-- Shared SQL helpers (e.g., reason builders)
'@

# repositories
$repos = Join-Path $src "repositories"
New-Dir $repos
New-FileUtf8 (Join-Path $repos "order_repository.py") @'
# Persistence helper (if needed)
'@
New-FileUtf8 (Join-Path $repos "customer_repository.py") @'
# Persistence helper
'@
New-FileUtf8 (Join-Path $repos "product_repository.py") @'
# Persistence helper
'@

# services
$services = Join-Path $src "services"
New-Dir $services
New-FileUtf8 (Join-Path $services "stage.py") @'
# COPY/bulk load Silver into Postgres staging
'@
New-FileUtf8 (Join-Path $services "run_sql.py") @'
# Execute SQL files with params (run_id, chunk_id)
'@
New-FileUtf8 (Join-Path $services "load_dims.py") @'
# Orchestrate dims SQL chain
'@
New-FileUtf8 (Join-Path $services "load_facts.py") @'
# Orchestrate facts SQL chain
'@
New-FileUtf8 (Join-Path $services "audit.py") @'
# Batch audit: counts, timings, checksums
'@

# quality
$quality = Join-Path $src "quality"
New-Dir $quality
New-FileUtf8 (Join-Path $quality "dq_rules.py") @'
# Runtime DQ checks/thresholds
'@
New-FileUtf8 (Join-Path $quality "expectations.md") @'
# Document DQ expectations
'@

# --- docs (ADRs, architecture, runbooks, DQ policy) ---
$docs = Join-Path $RootPath "docs"
New-Dir $docs

# ADRs
$adr = Join-Path $docs "adr"
New-Dir $adr

function New-Adr {
  param([int]$Num, [string]$Slug, [string]$Title)
  $file = Join-Path $adr ("{0:D4}-{1}.md" -f $Num, $Slug)
  if (-not (Test-Path -LiteralPath $file)) {
    $date = Get-Date -Format "yyyy-MM-dd"
    $content = @"
# $Title

- **Status:** Accepted
- **Date:** $date

## Context
(why this decision is needed)

## Decision
(the decision made)

## Consequences
(positive, negative, neutral)

## Alternatives Considered
(short bullets)
"@
    $content | Out-File -FilePath $file -Encoding utf8
  }
}

New-Adr 1 "choose-elt-over-etl" "Choose ELT over ETL"
New-Adr 2 "store-raw-as-parquet" "Store Raw Data as Parquet in Object Storage"
New-Adr 3 "silver-in-python-gold-in-postgres" "Do Bronze→Silver in Python, Silver→Gold in Postgres"
New-Adr 4 "staging-and-rejects-strategy" "Use Staging + Rejects with Reasons and Run IDs"
New-Adr 5 "uow-and-repository-pattern" "Adopt Unit of Work + Repository Pattern"
New-Adr 6 "fk-resolution-via-temp-tables" "Resolve FKs via Temp Tables During Silver→Gold"

# architecture docs
$arch = Join-Path $docs "architecture"
New-Dir $arch
New-FileUtf8 (Join-Path $arch "system-context.md") @'
# C4 L1: context diagram notes
'@
New-FileUtf8 (Join-Path $arch "container.md") @'
# C4 L2: containers (Airflow, Postgres, Object Storage)
'@
New-FileUtf8 (Join-Path $arch "component.md") @'
# C4 L3: components (sources, services, sql)
'@
New-FileUtf8 (Join-Path $arch "data-model.md") @'
# Facts/Dimensions dictionary, grains, keys
'@
New-Dir (Join-Path $arch "diagrams")
New-FileUtf8 (Join-Path $arch "diagrams\README.md") @'
# Add Mermaid/PlantUML diagrams here
'@

# runbooks
$runbooks = Join-Path $docs "runbooks"
New-Dir $runbooks
New-FileUtf8 (Join-Path $runbooks "replay-a-run.md") @'
# How to reprocess a run_id/checksum
'@
New-FileUtf8 (Join-Path $runbooks "handling-rejects.md") @'
# Triage flow for rejects
'@
New-FileUtf8 (Join-Path $runbooks "oncall-checklist.md") @'
# On-call checklist
'@

# dq policy
$dq = Join-Path $docs "dq"
New-Dir $dq
New-FileUtf8 (Join-Path $dq "rules.md") @'
# Hard vs soft rules; thresholds
'@
New-FileUtf8 (Join-Path $dq "monitoring.md") @'
# Alerts, SLOs
'@

New-FileUtf8 (Join-Path $docs "glossary.md") @'
# Project glossary
'@

# --- tests ---
$tests = Join-Path $RootPath "tests"
New-Dir $tests
New-Dir (Join-Path $tests "unit")
New-FileUtf8 (Join-Path $tests "unit\test_bronze_to_silver.py") @'
# Unit tests for parsing/normalization
'@
New-FileUtf8 (Join-Path $tests "unit\test_contracts.py") @'
# Unit tests for Pandera/Pydantic
'@
New-FileUtf8 (Join-Path $tests "unit\test_lake_io.py") @'
# Unit tests for parquet IO
'@

New-Dir (Join-Path $tests "integration")
New-FileUtf8 (Join-Path $tests "integration\test_stage_to_staging_clean.py") @'
# Integration: COPY to staging
'@
New-FileUtf8 (Join-Path $tests "integration\test_dims_merge.py") @'
# Integration: dim merge
'@
New-FileUtf8 (Join-Path $tests "integration\test_facts_resolve_split_load.py") @'
# Integration: fact load
'@

New-Dir (Join-Path $tests "fixtures")
New-FileUtf8 (Join-Path $tests "fixtures\example_orders.xlsx") ""
New-Dir (Join-Path $tests "fixtures\small_batches")

# --- scripts ---
$scripts = Join-Path $RootPath "scripts"
New-Dir $scripts
New-FileUtf8 (Join-Path $scripts "backfill_run.py") @'
# CLI to backfill a specific run_id
'@

# --- root files ---
New-FileUtf8 (Join-Path $RootPath "pyproject.toml") @'
[project]
name = "data_pipeline"
version = "0.1.0"
description = "Python-first ELT pipeline: Bronze→Silver in Python, Silver→Gold in Postgres"
requires-python = ">=3.10"

[tool]
# add black/ruff/pytest configs as needed
'@

New-FileUtf8 (Join-Path $RootPath ".env.example") @'
# Example env vars
DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/db
OBJECT_STORAGE_ROOT=/data/lake
AIRFLOW__CORE__DAGS_FOLDER=./airflow/dags
'@

New-FileUtf8 (Join-Path $RootPath "README.md") @'
# Data Pipeline

- Python for ingestion + Bronze→Silver
- Parquet for raw/bronze artifact storage
- Postgres SQL for Silver→Gold
- Airflow orchestrates (each transformation is a task)
- ADRs in docs/adr record architectural decisions
'@

Write-Host "✅ Project scaffold created at: $RootPath"
