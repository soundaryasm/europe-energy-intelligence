# Databricks notebook source
# MAGIC %md
# MAGIC # Historical Backfill — One Month per Run
# MAGIC
# MAGIC Workflow entry point for the `europe_energy_intelligence_backfill` job
# MAGIC (runs every 2 hours, UTC). All business logic lives in
# MAGIC `src/orchestration/backfill_runner.py`; this notebook only retrieves the
# MAGIC ENTSO-E token, wires up `spark`, and handles self-pausing the job's own
# MAGIC schedule once nothing is left to backfill.
# MAGIC
# MAGIC Each invocation processes exactly one historical calendar month, walking
# MAGIC backward from the previous complete month to `BACKFILL_HISTORICAL_START`
# MAGIC (2015-01-01). A month only counts as done once every
# MAGIC `(source, country, dataset)` combination in it has *whole-month* data
# MAGIC coverage — a partial month is left `failed`/retryable, never silently
# MAGIC advanced past (see `src/orchestration/backfill_completeness.py`).
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `dbutils`
# MAGIC and `spark`, which only exist in a Databricks notebook runtime.

# COMMAND ----------

from src.orchestration.backfill_runner import run_backfill_month

# COMMAND ----------

dbutils.widgets.text("secret_scope", "europe-energy-intelligence", "Databricks secret scope")
dbutils.widgets.text("secret_key", "ENTSOE_API_TOKEN", "Databricks secret key")
# `{{job.id}}` is a Databricks dynamic value reference, resolved to this
# job's own numeric ID at run time — needed to pause its own schedule.
dbutils.widgets.text("job_id", "", "This job's own job_id (for self-pause)")

secret_scope = dbutils.widgets.get("secret_scope")
secret_key = dbutils.widgets.get("secret_key")
job_id = dbutils.widgets.get("job_id")

# COMMAND ----------

# Token retrieved via Databricks-managed secrets only — never hard-coded,
# never logged, never committed.
security_token = dbutils.secrets.get(scope=secret_scope, key=secret_key)

result = run_backfill_month(spark, token=security_token)

# COMMAND ----------

if result.self_pause:
    print("Backfill complete: every month back to 2015-01-01 is accounted for. Pausing this job's schedule.")

    if not job_id:
        raise RuntimeError(
            "Backfill is complete but no job_id was provided, so the schedule could not be "
            "self-paused. Pass job_id={{job.id}} as a base_parameter, or pause the "
            "'europe_energy_intelligence_backfill' job manually."
        )

    # Read-then-write the full schedule object rather than trying a
    # partial update of just `pause_status`: the Jobs API does not
    # merge nested fields on `update`, so submitting `schedule` with
    # only `pause_status` set would silently drop the cron expression
    # and timezone instead of merely pausing the job.
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs as sdk_jobs

    client = WorkspaceClient()
    current = client.jobs.get(job_id=int(job_id))
    current_schedule = current.settings.schedule

    client.jobs.update(
        job_id=int(job_id),
        new_settings=sdk_jobs.JobSettings(
            schedule=sdk_jobs.CronSchedule(
                quartz_cron_expression=current_schedule.quartz_cron_expression,
                timezone_id=current_schedule.timezone_id,
                pause_status=sdk_jobs.PauseStatus.PAUSED,
            )
        ),
    )
    print(f"Job {job_id} schedule paused.")
else:
    print(f"Target month:               {result.target_month}")
    print(f"ENTSO-E records written:    {result.entsoe_records_written}")
    print(f"Open-Meteo records written: {result.open_meteo_records_written}")
    print(f"World Bank records written: {result.worldbank_records_written}"
          + (f" (error: {result.worldbank_error})" if result.worldbank_error else ""))
    print(f"Checkpoint rows written:    {result.checkpoint_rows_written}")
    for combo, status in sorted(result.combo_statuses.items()):
        print(f"  {combo}: {status}")

    from src.orchestration.backfill_checkpoint import STATUS_FAILED

    failed_combos = [combo for combo, status in result.combo_statuses.items() if status == STATUS_FAILED]
    if failed_combos:
        # A failed/partial combo is expected to be retried on the next
        # invocation (the checkpoint keeps it out of `_DONE_STATUSES`,
        # so the walker will pick this same month again) — surfacing it
        # as a task failure here just makes that visible, it does not
        # change what happens next.
        raise RuntimeError(
            f"Backfill for {result.target_month} left {len(failed_combos)} combination(s) "
            f"incomplete, will retry this month next run: {failed_combos}"
        )
