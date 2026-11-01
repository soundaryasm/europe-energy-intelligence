# Databricks notebook source
# MAGIC %md
# MAGIC # Verify The Historical Bronze Load
# MAGIC
# MAGIC Workflow entry point for the `verify_historical` task. All logic lives
# MAGIC in `src/blob/verify.py`.
# MAGIC
# MAGIC Checks the load's result independently of what the load reported: for
# MAGIC every source, is there a row on the volume whose merge key is absent
# MAGIC from Bronze? That is a left-anti join, not a row comparison — Bronze
# MAGIC legitimately holds more rows than the staged set, because the daily
# MAGIC pipeline wrote some of them, so matching counts would prove nothing.
# MAGIC
# MAGIC This is the gate before Silver and Gold are rebuilt. A decade of Silver
# MAGIC plus a full dbt build is the most expensive run in the project and
# MAGIC should not start on a Bronze that is quietly short of a year.
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

from src.blob.stager import DEFAULT_VOLUME_ROOT
from src.blob.verify import verify_all

# COMMAND ----------

dbutils.widgets.text("volume_root", DEFAULT_VOLUME_ROOT, "Volume path holding the staged files")
dbutils.widgets.text("sources", "", "Comma-separated sources to verify (blank = all)")

volume_root = dbutils.widgets.get("volume_root") or DEFAULT_VOLUME_ROOT
sources = [s.strip() for s in dbutils.widgets.get("sources").split(",") if s.strip()] or None

# COMMAND ----------

verdicts = verify_all(spark, volume_root, sources=sources)

for verdict in verdicts:
    print(verdict.describe())
    for sample in verdict.samples:
        print(f"    missing key: {sample}")

if not verdicts:
    raise RuntimeError(f"Nothing staged under {volume_root}; there is nothing to verify")

failed = [v for v in verdicts if not v.ok]
if failed:
    raise RuntimeError(
        f"{len(failed)} source(s) did not verify: {[v.source for v in failed]}. "
        "Bronze is short of rows the volume holds — re-run the load before "
        "rebuilding Silver and Gold."
    )

print("\nAll sources verified: every staged row has a counterpart in Bronze.")
