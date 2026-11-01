"""Static validation of the historical load job configs (Spec 010).

This only parses and checks the YAML structure — it does not call the
Databricks CLI or a real workspace, so it cannot confirm the bundles
actually deploy. It does confirm the task DAGs, the split between the
two jobs, and the absence of a schedule match what Spec 010 requires on
paper.
"""
from pathlib import Path

import yaml

RESOURCES = Path(__file__).resolve().parents[1] / "resources"
BRONZE_LOAD_PATH = RESOURCES / "historical_bronze_load.yml"
REBUILD_PATH = RESOURCES / "historical_rebuild.yml"


def _job(path, job_key):
    with open(path, "r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
    return config["resources"]["jobs"][job_key]


def _bronze_load():
    return _job(BRONZE_LOAD_PATH, "europe_energy_intelligence_historical_bronze_load")


def _rebuild():
    return _job(REBUILD_PATH, "europe_energy_intelligence_historical_rebuild")


def _tasks_by_key(job):
    return {task["task_key"]: task for task in job["tasks"]}


def test_bronze_load_defines_stage_load_and_verify():
    assert {t["task_key"] for t in _bronze_load()["tasks"]} == {
        "stage_historical", "load_historical", "verify_historical",
    }


def test_rebuild_defines_silver_gold_and_publish():
    assert {t["task_key"] for t in _rebuild()["tasks"]} == {
        "transform_silver", "build_dbt_gold", "publish_postgres",
    }


def test_the_rebuild_is_a_separate_job_from_the_bronze_load():
    # Spec 010: the verify output is meant to be read by a person before
    # the rebuild starts. A dependency between the two would remove that
    # gate, and a failure in the rebuild would cost the Bronze load too.
    assert "transform_silver" not in _tasks_by_key(_bronze_load())
    assert "load_historical" not in _tasks_by_key(_rebuild())


def test_neither_job_has_a_schedule():
    # Both run once, by hand. There is nothing recurring to do.
    assert "schedule" not in _bronze_load()
    assert "schedule" not in _rebuild()


def test_the_merge_waits_for_staging():
    tasks = _tasks_by_key(_bronze_load())

    assert tasks["load_historical"]["depends_on"] == [{"task_key": "stage_historical"}]


def test_verify_waits_for_the_merge():
    tasks = _tasks_by_key(_bronze_load())

    assert tasks["verify_historical"]["depends_on"] == [{"task_key": "load_historical"}]


def test_gold_waits_for_silver():
    tasks = _tasks_by_key(_rebuild())

    assert tasks["build_dbt_gold"]["depends_on"] == [{"task_key": "transform_silver"}]


def test_publish_waits_for_gold_so_a_failed_build_never_reaches_serving():
    # Spec 007 data-quality gate.
    tasks = _tasks_by_key(_rebuild())

    assert tasks["publish_postgres"]["depends_on"] == [{"task_key": "build_dbt_gold"}]


def test_the_merge_has_no_task_level_retry():
    # A failure here needs looking at before being repeated: the load
    # reports which batches failed, and a blind retry just re-merges
    # everything that already succeeded.
    tasks = _tasks_by_key(_bronze_load())

    assert "max_retries" not in tasks["load_historical"]


def test_gold_targets_the_validated_catalog_and_schema():
    # Left to default, dbt_task would use the workspace default catalog
    # and the `default` schema; publish_postgres reads workspace.gold.
    dbt_task = _tasks_by_key(_rebuild())["build_dbt_gold"]["dbt_task"]

    assert dbt_task["catalog"] == "workspace"
    assert dbt_task["schema"] == "gold"


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
