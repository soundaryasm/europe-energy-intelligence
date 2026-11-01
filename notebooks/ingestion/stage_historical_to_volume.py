# Databricks notebook source
# MAGIC %md
# MAGIC # Stage Historical Parquet Onto A Volume
# MAGIC
# MAGIC Workflow entry point for the `stage_historical` task. All logic lives
# MAGIC in `src/blob/stager.py`; this notebook only reads the blob credentials
# MAGIC and reports what the pass did.
# MAGIC
# MAGIC Copies every Parquet file the historical ingestion landed in blob
# MAGIC storage onto a Unity Catalog volume, byte for byte. Nothing is parsed
# MAGIC and only one file is held in memory at a time, so this task's cost is
# MAGIC bounded by network time rather than data volume.
# MAGIC
# MAGIC Already-staged files are skipped, so re-running after a failure resumes
# MAGIC rather than re-downloading.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `spark` and
# MAGIC `dbutils`, which only exist in a Databricks notebook runtime.

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

from src.blob.stager import DEFAULT_VOLUME_ROOT, stage_all, volume_identifier
from src.blob.store import TOKEN_ENV_VAR, BlobStore

# COMMAND ----------

dbutils.widgets.text("secret_scope", "europe-energy-intelligence", "Databricks secret scope")
dbutils.widgets.text("secret_key", "UPSTASH_BLOB_TOKEN", "Secret key holding the bucket token")
dbutils.widgets.text("volume_root", DEFAULT_VOLUME_ROOT, "Volume path to stage into")
dbutils.widgets.text("sources", "", "Comma-separated sources to stage (blank = all)")

secret_scope = dbutils.widgets.get("secret_scope")
secret_key = dbutils.widgets.get("secret_key")
volume_root = dbutils.widgets.get("volume_root") or DEFAULT_VOLUME_ROOT
sources = [s.strip() for s in dbutils.widgets.get("sources").split(",") if s.strip()] or None

# COMMAND ----------

# Created here rather than by hand so the job is self-sufficient on a
# fresh workspace. The identifier is derived from the path the staging
# writes to, so the two cannot name different volumes.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_identifier(volume_root)}")

# COMMAND ----------

# The store reads the bucket token from the environment, which is how
# the same code runs unchanged here and in CI. It comes from the secret
# scope rather than a notebook parameter so it never appears in a job
# definition or a run's parameter list. The token is exchanged for
# short-lived S3 credentials inside the store, which re-mints them as
# the pass runs — staging a decade of files takes longer than one
# credential lifetime.
os.environ[TOKEN_ENV_VAR] = dbutils.secrets.get(scope=secret_scope, key=secret_key)

store = BlobStore.from_env()

# COMMAND ----------

result = stage_all(store, volume_root, sources=sources)

print(f"Files staged:  {result.files_staged}")
print(f"Files skipped: {result.files_skipped}")
print(f"Bytes staged:  {result.bytes_staged / 1e6:.1f} MB")
for source, count in sorted(result.by_source.items()):
    print(f"  {source}: {count} file(s)")

if result.errors:
    for key, message in result.errors.items():
        print(f"FAILED {key}: {message}")
    # A file that could not be staged would leave Bronze short of a
    # month, so the task fails rather than letting the merge run on an
    # incomplete volume. Re-running is safe: staged files are skipped.
    raise RuntimeError(
        f"{len(result.errors)} file(s) failed to stage; the volume is incomplete"
    )
