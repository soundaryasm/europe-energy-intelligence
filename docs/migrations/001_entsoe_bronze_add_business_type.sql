-- Migration 001: add `business_type` to the ENTSO-E Bronze table.
--
-- One-time, explicit migration — NOT applied automatically by any
-- ingestion run. `business_type` was added to the application-owned
-- schema (`src/ingestion/delta_schema.entsoe_bronze_schema`) after some
-- environments' `bronze_entsoe_energy` table was first created, to keep a
-- pumped-storage Production (A01) observation distinct from a Consumption
-- (A04) observation at the same country/dataset/timestamp/production_type
-- (see `src/ingestion/entsoe_bronze.py`). Run this once, by hand, in a
-- scratch notebook against the real table before the next ingestion run
-- against it — a run against a table that still lacks this column will
-- fail fast with a clear `SchemaMismatchError` rather than silently
-- evolving the table.
--
-- Replace `bronze_entsoe_energy` below with the fully qualified
-- `catalog.schema.table` name if the table is not resolved from the
-- notebook's current default catalog/schema.

ALTER TABLE bronze_entsoe_energy ADD COLUMN business_type STRING;

-- Verify the table now matches src/ingestion/delta_schema.entsoe_bronze_schema():
-- DESCRIBE TABLE bronze_entsoe_energy;
