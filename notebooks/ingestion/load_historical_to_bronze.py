# Databricks notebook source
# MAGIC %md
# MAGIC # Merge Staged Historical Parquet Into Bronze
# MAGIC
# MAGIC Workflow entry point for the `load_historical` task. All logic lives in
# MAGIC `src/blob/loader.py`; this notebook only wires up the active `spark`
# MAGIC session and reports what the pass merged.
# MAGIC
# MAGIC Reads the Parquet files `stage_historical` put on the volume and merges
# MAGIC them into Bronze, one source-year at a time, oldest first. Spark reads
# MAGIC the files itself — the driver never holds a row.
# MAGIC
# MAGIC The merge is keyed on the same Bronze business key the daily pipeline
# MAGIC uses, so this is safe to re-run and safe to run against Bronze tables
# MAGIC that already hold overlapping history: matching rows are updated in
# MAGIC place, rows Bronze alone holds are left untouched, and nothing is
# MAGIC duplicated or deleted.
# MAGIC
# MAGIC Run once, by hand, after the historical ingestion reports complete. The
# MAGIC months it loads are whole calendar months ending at the last complete
# MAGIC month; the current month-to-date is covered by running the daily
# MAGIC pipeline in `reprocess` mode for that range.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `spark`,
# MAGIC which only exists in a Databricks notebook runtime.

# COMMAND ----------

# See the staging notebook's bootstrap comment for why this is needed.
dbutils.widgets.text("bundle_root", "", "Workspace root to add to sys.path (bundle deployments)")

import sys

_bundle_root = dbutils.widgets.get("bundle_root")
if _bundle_root and _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------

from src.blob.loader import load_all
from src.blob.stager import DEFAULT_VOLUME_ROOT

# COMMAND ----------

dbutils.widgets.text("volume_root", DEFAULT_VOLUME_ROOT, "Volume path holding the staged files")
dbutils.widgets.text("sources", "", "Comma-separated sources to load (blank = all)")
dbutils.widgets.dropdown("resume", "true", ["true", "false"], "Skip batches already merged")

volume_root = dbutils.widgets.get("volume_root") or DEFAULT_VOLUME_ROOT
sources = [s.strip() for s in dbutils.widgets.get("sources").split(",") if s.strip()] or None
# Set to false to force every batch to be merged again. The merge is
# idempotent, so this is a way to spend compute deliberately, not a
# repair for a corrupted load.
resume = dbutils.widgets.get("resume") == "true"

# COMMAND ----------

result = load_all(spark, volume_root, sources=sources, resume=resume)

print(f"Batches merged:  {result.batches_loaded}")
print(f"Batches skipped: {result.batches_skipped}")
for source, rows in sorted(result.rows_by_source.items()):
    print(f"  {source}: {rows:,} rows")

if result.errors:
    for batch, message in result.errors.items():
        print(f"FAILED {batch}: {message}")
    # A batch that could not be merged leaves Bronze incomplete, so the
    # task fails rather than reporting success with a gap. Re-running is
    # safe, and skips the batches that already landed.
    raise RuntimeError(
        f"{len(result.errors)} batch(es) failed to merge; Bronze is incomplete"
    )
