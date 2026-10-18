"""Historical ingestion into S3-compatible blob storage (Spec 010).

The historical months of every source are fetched outside Databricks and
landed here as one Parquet file per month, which Databricks then loads
into Bronze in a single pass. Nothing in this package imports PySpark:
it runs wherever Python and network access are available.
"""
