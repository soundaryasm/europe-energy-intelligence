"""Every windowed task is actually handed the window (Spec 006).

Spec 006 requires one canonical processing window passed to downstream
tasks rather than each task deriving its own. Silver and the serving
publish read that window from job parameters, so a task left without
them would silently fall back to the notebook widget defaults — a
3-day `daily` window — and quietly publish almost nothing during a
historical rebuild. Static YAML checks only; no workspace is contacted.
"""
from pathlib import Path

import pytest
import yaml

RESOURCES = Path(__file__).resolve().parents[1] / "resources"

WINDOW_PARAMS = ("execution_mode", "start_date", "end_date", "lookback_days")

# Task key -> the file and job it belongs to. Every notebook task that
# resolves a processing window must appear here.
WINDOWED_TASKS = (
    ("daily_pipeline.yml", "europe_energy_intelligence_daily", "ingest_entsoe"),
    ("daily_pipeline.yml", "europe_energy_intelligence_daily", "ingest_open_meteo"),
    ("daily_pipeline.yml", "europe_energy_intelligence_daily", "transform_silver"),
    ("daily_pipeline.yml", "europe_energy_intelligence_daily", "publish_postgres"),
    ("historical_rebuild.yml", "europe_energy_intelligence_historical_rebuild", "transform_silver"),
    ("historical_rebuild.yml", "europe_energy_intelligence_historical_rebuild", "publish_postgres"),
)


def _job(filename, job_key):
    with open(RESOURCES / filename, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)["resources"]["jobs"][job_key]


def _task(filename, job_key, task_key):
    for task in _job(filename, job_key)["tasks"]:
        if task["task_key"] == task_key:
            return task
    raise AssertionError(f"{task_key} not found in {filename}")


@pytest.mark.parametrize("filename,job_key,task_key", WINDOWED_TASKS)
def test_a_windowed_task_receives_every_window_parameter(filename, job_key, task_key):
    params = _task(filename, job_key, task_key)["notebook_task"]["base_parameters"]
    for name in WINDOW_PARAMS:
        assert params.get(name) == "{{job.parameters.%s}}" % name, (
            f"{task_key} in {filename} does not pass {name} through from job parameters"
        )


@pytest.mark.parametrize("filename,job_key", [
    ("daily_pipeline.yml", "europe_energy_intelligence_daily"),
    ("historical_rebuild.yml", "europe_energy_intelligence_historical_rebuild"),
])
def test_the_job_declares_every_window_parameter(filename, job_key):
    declared = {p["name"] for p in _job(filename, job_key)["parameters"]}
    assert set(WINDOW_PARAMS) <= declared


def test_the_daily_job_defaults_to_a_narrow_daily_window():
    # A scheduled run must never span the history (Spec 006).
    params = {p["name"]: p["default"] for p in
              _job("daily_pipeline.yml", "europe_energy_intelligence_daily")["parameters"]}
    assert params["execution_mode"] == "daily"
    assert params["start_date"] == ""
    assert params["end_date"] == ""
    assert int(params["lookback_days"]) <= 7


def test_the_rebuild_job_defaults_to_reprocess_from_the_start_of_history():
    params = {p["name"]: p["default"] for p in
              _job("historical_rebuild.yml",
                   "europe_energy_intelligence_historical_rebuild")["parameters"]}
    assert params["execution_mode"] == "reprocess"
    assert params["start_date"] == "2015-01-01"
    # Deliberately empty: reprocess requires an explicit end_date, so a
    # run that forgets to pass one fails loudly instead of silently
    # rebuilding a stale range.
    assert params["end_date"] == ""


# --- Unattended-run resilience -----------------------------------------
#
# The daily job runs at 02:00 with nobody watching. A task that fails
# without retrying takes every downstream task with it, so the night is
# lost and the gap has to be noticed and repaired by hand.

# Exceeding ENTSO-E's per-token rate limit earns a 10-minute ban (see
# .github/workflows/historical-backfill.yml), so a retry scheduled
# inside that window arrives banned and burns an attempt for nothing.
ENTSOE_BAN_SECONDS = 10 * 60


def _daily_tasks():
    return {t["task_key"]: t for t in
            _job("daily_pipeline.yml", "europe_energy_intelligence_daily")["tasks"]}


@pytest.mark.parametrize("task_key", [
    "ingest_entsoe", "ingest_open_meteo", "ingest_worldbank",
    "transform_silver", "build_dbt_gold", "publish_postgres",
])
def test_every_daily_task_retries(task_key):
    task = _daily_tasks()[task_key]
    assert task.get("max_retries", 0) >= 1, (
        f"{task_key} would fail the whole night on one transient fault"
    )


def test_the_ingest_tasks_all_retry_the_same_number_of_times():
    # No source gets a weaker retry budget than another. All three
    # re-fetch their whole window every night, so a failed night
    # self-heals identically for each.
    attempts = {
        k: _daily_tasks()[k]["max_retries"]
        for k in ("ingest_entsoe", "ingest_open_meteo", "ingest_worldbank")
    }
    assert len(set(attempts.values())) == 1, attempts


def test_entsoe_retries_wait_out_the_rate_limit_ban():
    task = _daily_tasks()["ingest_entsoe"]
    gap_seconds = task["min_retry_interval_millis"] / 1000
    assert gap_seconds > ENTSOE_BAN_SECONDS


def test_the_api_backed_tasks_retry_on_timeout():
    # Read timeouts were the observed ENTSO-E failure mode on
    # 2026-11-04, and a timeout is not retried unless this is set.
    tasks = _daily_tasks()
    for task_key in ("ingest_entsoe", "ingest_open_meteo"):
        assert tasks[task_key].get("retry_on_timeout") is True, task_key
