"""Static validation of the historical load job config (Spec 010).

This only parses and checks the YAML structure — it does not call the
Databricks CLI or a real workspace, so it cannot confirm the bundle
actually deploys. It does confirm the task DAG and the absence of a
schedule match what Spec 010 requires on paper.
"""
from pathlib import Path

import yaml

JOB_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "resources" / "historical_load_pipeline.yml"
)

EXPECTED_TASK_KEYS = {
    "load_historical",
    "transform_silver",
    "build_dbt_gold",
    "publish_postgres",
}


def _load_job():
    with open(JOB_CONFIG_PATH, "r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
    return config["resources"]["jobs"]["europe_energy_intelligence_historical_load"]


def _tasks_by_key(job):
    return {task["task_key"]: task for task in job["tasks"]}


def test_job_config_defines_all_expected_tasks():
    assert {task["task_key"] for task in _load_job()["tasks"]} == EXPECTED_TASK_KEYS


def test_job_has_no_schedule():
    # Spec 010: this runs once, by hand, after the ingestion reports
    # complete. There is nothing recurring to do, and the Silver/Gold
    # rebuild over a decade is the most expensive run in the project.
    assert "schedule" not in _load_job()


def test_silver_waits_for_the_bronze_load():
    tasks = _tasks_by_key(_load_job())

    assert tasks["transform_silver"]["depends_on"] == [{"task_key": "load_historical"}]


def test_gold_waits_for_silver():
    tasks = _tasks_by_key(_load_job())

    assert tasks["build_dbt_gold"]["depends_on"] == [{"task_key": "transform_silver"}]


def test_publish_waits_for_gold_so_a_failed_build_never_reaches_serving():
    # Spec 007 data-quality gate.
    tasks = _tasks_by_key(_load_job())

    assert tasks["publish_postgres"]["depends_on"] == [{"task_key": "build_dbt_gold"}]


def test_the_bronze_load_has_no_task_level_retry():
    # A failure here needs looking at before being repeated: the load
    # reports which files failed, and a blind retry just re-merges
    # everything that already succeeded.
    tasks = _tasks_by_key(_load_job())

    assert "max_retries" not in tasks["load_historical"]


def test_gold_targets_the_validated_catalog_and_schema():
    # Left to default, dbt_task would use the workspace default catalog
    # and the `default` schema; publish_postgres reads workspace.gold.
    dbt_task = _tasks_by_key(_load_job())["build_dbt_gold"]["dbt_task"]

    assert dbt_task["catalog"] == "workspace"
    assert dbt_task["schema"] == "gold"


def test_the_bronze_load_token_comes_from_a_secret_scope_not_a_parameter():
    # Spec 010: the bucket token is never passed as a job parameter or
    # written into a run's parameter list.
    params = _tasks_by_key(_load_job())["load_historical"]["notebook_task"]["base_parameters"]

    assert "secret_scope" in params
    assert params["secret_key"] == "UPSTASH_BLOB_TOKEN"
