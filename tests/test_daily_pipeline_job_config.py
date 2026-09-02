"""Static validation of the Databricks job DAG config (Spec 006).

This only parses and checks the YAML structure — it does not call the
Databricks CLI or a real workspace (neither is available in this
environment), so it cannot confirm the bundle actually deploys. It does
confirm the task DAG, schedule, and retry policy match what Spec 006
requires on paper.
"""
from pathlib import Path

import yaml

JOB_CONFIG_PATH = Path(__file__).resolve().parents[1] / "resources" / "daily_pipeline.yml"

EXPECTED_TASK_KEYS = {
    "ingest_open_meteo",
    "ingest_entsoe",
    "transform_silver",
    "build_dbt_gold",
    "publish_postgres",
}


def _load_job():
    with open(JOB_CONFIG_PATH, "r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
    return config["resources"]["jobs"]["europe_energy_intelligence_daily"]


def _tasks_by_key(job):
    return {task["task_key"]: task for task in job["tasks"]}


def test_job_config_defines_all_five_expected_tasks():
    job = _load_job()
    assert {task["task_key"] for task in job["tasks"]} == EXPECTED_TASK_KEYS


def test_job_schedule_is_02_00_europe_dublin():
    job = _load_job()
    assert job["schedule"]["timezone_id"] == "Europe/Dublin"
    assert job["schedule"]["quartz_cron_expression"] == "0 0 2 * * ?"


def test_job_supports_daily_backfill_reprocess_parameters():
    job = _load_job()
    param_names = {p["name"] for p in job["parameters"]}
    assert {"mode", "start_date", "end_date"} <= param_names
    mode_param = next(p for p in job["parameters"] if p["name"] == "mode")
    assert mode_param["default"] == "daily"  # scheduled runs never default to backfill


def test_ingestion_tasks_have_no_dependencies():
    tasks = _tasks_by_key(_load_job())
    assert "depends_on" not in tasks["ingest_open_meteo"]
    assert "depends_on" not in tasks["ingest_entsoe"]


def test_silver_depends_on_both_ingestion_tasks():
    tasks = _tasks_by_key(_load_job())
    depends_on = {d["task_key"] for d in tasks["transform_silver"]["depends_on"]}
    assert depends_on == {"ingest_open_meteo", "ingest_entsoe"}


def test_dbt_depends_on_silver():
    tasks = _tasks_by_key(_load_job())
    depends_on = {d["task_key"] for d in tasks["build_dbt_gold"]["depends_on"]}
    assert depends_on == {"transform_silver"}


def test_publish_depends_on_dbt_only():
    tasks = _tasks_by_key(_load_job())
    depends_on = {d["task_key"] for d in tasks["publish_postgres"]["depends_on"]}
    assert depends_on == {"build_dbt_gold"}


def test_every_dependency_reference_points_to_a_real_task():
    job = _load_job()
    valid_keys = {task["task_key"] for task in job["tasks"]}
    for task in job["tasks"]:
        for dep in task.get("depends_on", []):
            assert dep["task_key"] in valid_keys


def test_retries_are_bounded_on_every_task_that_declares_them():
    job = _load_job()
    for task in job["tasks"]:
        if "max_retries" in task:
            assert isinstance(task["max_retries"], int)
            assert 0 <= task["max_retries"] <= 5  # bounded, not "infinite retries"


def test_dbt_task_pins_dbt_databricks_not_dbt_spark():
    tasks = _tasks_by_key(_load_job())
    libraries = tasks["build_dbt_gold"].get("libraries", [])
    packages = [lib["pypi"]["package"] for lib in libraries if "pypi" in lib]
    assert any(pkg.startswith("dbt-databricks==") for pkg in packages)
    assert not any("dbt-spark" in pkg for pkg in packages)
