# Databricks notebook source
# MAGIC %md
# MAGIC # Historical Bronze Load from Blob Storage
# MAGIC
# MAGIC Workflow entry point for the `load_historical` task. All business logic
# MAGIC lives in `src/blob/loader.py`; this notebook only reads the blob
# MAGIC credentials and wires up the active `spark` session.
# MAGIC
# MAGIC Reads every Parquet file the historical ingestion landed in blob storage
# MAGIC and merges it into Bronze, oldest period first. The merge is keyed on
# MAGIC the same Bronze business key the daily pipeline uses, so this is safe to
# MAGIC re-run and safe to run against Bronze tables that already hold
# MAGIC overlapping history — matching rows are updated in place, not
# MAGIC duplicated.
# MAGIC
# MAGIC Run once, by hand, after the historical ingestion reports complete. The
# MAGIC months it loads are whole calendar months ending at the last complete
# MAGIC month; the current month-to-date is covered by running the daily
# MAGIC pipeline in `reprocess` mode for that range.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `spark`,
# MAGIC which only exists in a Databricks notebook runtime.

# COMMAND ----------

# Bundle deployments run this notebook from a plain Workspace Files
# location, not a Databricks Repo — which does not add the repo root to
# sys.path automatically the way a Repo clone does. Confirmed via a
# real deployed run failing with `ModuleNotFoundError: No module named
# 'src'` (2026-09-09) despite the identical code working fine from a
# Repos-synced folder. Must run before any `from src...` import below.
dbutils.widgets.text("bundle_root", "", "Workspace root to add to sys.path (bundle deployments)")

import sys

_bundle_root = dbutils.widgets.get("bundle_root")
if _bundle_root and _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------

import os

from src.blob.loader import load_all
from src.blob.store import TOKEN_ENV_VAR, BlobStore

# COMMAND ----------

dbutils.widgets.text("secret_scope", "europe-energy-intelligence", "Databricks secret scope")
dbutils.widgets.text("secret_key", "UPSTASH_BLOB_TOKEN", "Secret key holding the bucket token")
dbutils.widgets.text("sources", "", "Comma-separated sources to load (blank = all)")

secret_scope = dbutils.widgets.get("secret_scope")
secret_key = dbutils.widgets.get("secret_key")
sources = [s.strip() for s in dbutils.widgets.get("sources").split(",") if s.strip()] or None

# COMMAND ----------

# The store reads the bucket token from the environment, which is how
# the same code runs unchanged here and in CI. It comes from the secret
# scope rather than a notebook parameter so it never appears in a job
# definition or a run's parameter list. The token is exchanged for
# short-lived S3 credentials inside the store, which re-mints them as
# the load runs — a decade of history takes longer than one credential
# lifetime.
os.environ[TOKEN_ENV_VAR] = dbutils.secrets.get(scope=secret_scope, key=secret_key)

store = BlobStore.from_env()

# COMMAND ----------

result = load_all(spark, store, sources=sources)

print(f"Files loaded:  {result.files_loaded}")
print(f"Files skipped: {result.files_skipped}")
for source, rows in sorted(result.rows_by_source.items()):
    print(f"  {source}: {rows} rows")

if result.errors:
    for key, message in result.errors.items():
        print(f"FAILED {key}: {message}")
    # A file that could not be loaded leaves Bronze incomplete, so the
    # task fails rather than reporting success with a gap. Re-running is
    # safe: already-loaded months merge to no change.
    raise RuntimeError(
        f"{len(result.errors)} file(s) failed to load; Bronze is incomplete"
    )
