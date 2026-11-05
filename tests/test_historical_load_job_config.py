"""Static validation of the historical Bronze load job config (Spec 010).

This only parses and checks the YAML structure — it does not call the
Databricks CLI or a real workspace, so it cannot confirm the bundle
actually deploys. It does confirm the task DAG and the absence of a
schedule match what Spec 010 requires on paper.

There was a second job here, `historical_rebuild`, holding
silver -> gold -> publish. It was the daily job's last three tasks with
different parameter defaults, and
`bundle run ... --only transform_silver+` runs that chain from the one
job, so it was deleted. Its assertions lived on as duplicates of
`test_daily_pipeline_job_config` and went with it.
"""
from pathlib import Path

import yaml

RESOURCES = Path(__file__).resolve().parents[1] / "resources"
BRONZE_LOAD_PATH = RESOURCES / "historical_bronze_load.yml"


def _job(path, job_key):
    with open(path, "r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
    return config["resources"]["jobs"][job_key]


def _bronze_load():
    return _job(BRONZE_LOAD_PATH, "europe_energy_intelligence_historical_bronze_load")


def _tasks_by_key(job):
    return {task["task_key"]: task for task in job["tasks"]}


def test_bronze_load_defines_stage_load_and_verify():
    assert {t["task_key"] for t in _bronze_load()["tasks"]} == {
        "stage_historical", "load_historical", "verify_historical",
    }


def test_the_bronze_load_has_no_schedule():
    # It runs by hand. There is nothing recurring to do, and since the
    # blob bucket it stages from was emptied on 2026-11-04 it is kept as
    # the record of how the history was loaded rather than as a live path.
    assert "schedule" not in _bronze_load()


def test_the_merge_waits_for_staging():
    tasks = _tasks_by_key(_bronze_load())

    assert tasks["load_historical"]["depends_on"] == [{"task_key": "stage_historical"}]


def test_verify_waits_for_the_merge():
    tasks = _tasks_by_key(_bronze_load())

    assert tasks["verify_historical"]["depends_on"] == [{"task_key": "load_historical"}]


def test_the_merge_has_no_task_level_retry():
    # A failure here needs looking at before being repeated: the load
    # reports which batches failed, and a blind retry just re-merges
    # everything that already succeeded.
    tasks = _tasks_by_key(_bronze_load())

    assert "max_retries" not in tasks["load_historical"]


def test_the_bucket_token_comes_from_a_secret_scope_not_a_parameter():
    # Spec 010: the bucket token is never passed as a job parameter or
    # written into a run's parameter list.
    params = _tasks_by_key(_bronze_load())["stage_historical"]["notebook_task"]["base_parameters"]

    assert "secret_scope" in params
    assert params["secret_key"] == "UPSTASH_BLOB_TOKEN"


def test_only_staging_needs_the_bucket_token():
    # The merge and the verify read the volume, not the bucket, so
    # neither is given credentials it has no use for.
    tasks = _tasks_by_key(_bronze_load())

    for task_key in ("load_historical", "verify_historical"):
        params = tasks[task_key]["notebook_task"]["base_parameters"]
        assert "secret_key" not in params


def test_every_task_stages_and_reads_the_same_volume():
    # One variable behind all three, so staging cannot write where the
    # merge does not look.
    tasks = _tasks_by_key(_bronze_load())
    roots = {
        t["notebook_task"]["base_parameters"]["volume_root"] for t in tasks.values()
    }

    assert roots == {"${var.volume_root}"}
